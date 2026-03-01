#![forbid(unsafe_code)]

#[cfg(feature = "blockifier-adapter")]
use std::sync::Arc;
use std::sync::Mutex;

use node_spec_core::protocol_version::{
    VersionResolutionError, VersionedConstants, VersionedConstantsResolver,
};
use semver::Version;
#[cfg(feature = "blockifier-adapter")]
use starknet_node_types::BlockGasPrices;
#[cfg(feature = "blockifier-adapter")]
use starknet_node_types::GasPricePerToken;
#[cfg(feature = "blockifier-adapter")]
use starknet_node_types::StarknetFelt;
use starknet_node_types::{
    BlockContext, ExecutionOutput, MutableState, SimulationResult, StarknetBlock,
    StarknetTransaction, StateReader,
};
#[cfg(feature = "blockifier-adapter")]
use std::collections::BTreeMap;
#[cfg(feature = "blockifier-adapter")]
use std::str::FromStr;
#[cfg(feature = "blockifier-adapter")]
use std::time::{Duration as StdDuration, Instant};

#[cfg(feature = "blockifier-adapter")]
use blockifier::blockifier::config::TransactionExecutorConfig;
#[cfg(feature = "blockifier-adapter")]
use blockifier::blockifier::transaction_executor::{TransactionExecutor, TransactionExecutorError};
#[cfg(feature = "blockifier-adapter")]
use blockifier::blockifier_versioned_constants::VersionedConstants as BlockifierVersionedConstants;
#[cfg(feature = "blockifier-adapter")]
use blockifier::bouncer::BouncerConfig;
#[cfg(feature = "blockifier-adapter")]
use blockifier::context::{
    BlockContext as BlockifierBlockContext, ChainInfo as BlockifierChainInfo,
};
#[cfg(feature = "blockifier-adapter")]
use blockifier::execution::contract_class::RunnableCompiledClass;
#[cfg(feature = "blockifier-adapter")]
use blockifier::state::errors::StateError;
#[cfg(feature = "blockifier-adapter")]
use blockifier::state::state_api::StateReader as BlockifierStateReader;
#[cfg(feature = "blockifier-adapter")]
use blockifier::transaction::transaction_execution::Transaction as BlockifierTransaction;
#[cfg(feature = "blockifier-adapter")]
use starknet_api::block::{
    BlockHash as BlockifierBlockHash, BlockHashAndNumber as BlockifierBlockHashAndNumber,
    BlockInfo as BlockifierBlockInfo, BlockNumber as BlockifierBlockNumber,
    BlockTimestamp as BlockifierBlockTimestamp, GasPrice as BlockifierGasPrice,
    GasPriceVector as BlockifierGasPriceVector, GasPrices as BlockifierGasPrices,
    NonzeroGasPrice as BlockifierNonzeroGasPrice, StarknetVersion as BlockifierProtocolVersion,
};
#[cfg(feature = "blockifier-adapter")]
use starknet_api::core::{
    ClassHash as BlockifierClassHash, CompiledClassHash as BlockifierCompiledClassHash,
    ContractAddress as BlockifierContractAddress, Nonce as BlockifierNonce,
};
#[cfg(feature = "blockifier-adapter")]
use starknet_api::hash::StarkHash as BlockifierFelt;
#[cfg(feature = "blockifier-adapter")]
use starknet_api::state::StorageKey as BlockifierStorageKey;
#[cfg(feature = "blockifier-adapter")]
use starknet_api::versioned_constants_logic::VersionedConstantsTrait;
#[cfg(feature = "blockifier-adapter")]
use starknet_node_types::ExecutableStarknetTransaction;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ExecutionError {
    #[error("backend error: {0}")]
    Backend(String),
    #[error("execution mismatch detected at block {block_number}")]
    Mismatch { block_number: u64 },
    #[error("execution halted due to mismatch policy")]
    Halted,
    #[error("missing constants for protocol version {0}")]
    MissingConstants(Version),
}

pub trait ExecutionBackend: Send + Sync {
    fn execute_block(
        &self,
        block: &StarknetBlock,
        state: &mut dyn MutableState,
    ) -> Result<ExecutionOutput, ExecutionError>;

    fn simulate_tx(
        &self,
        tx: &StarknetTransaction,
        state: &dyn StateReader,
        block_context: &BlockContext,
    ) -> Result<SimulationResult, ExecutionError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CasmTrace {
    pub steps: usize,
}

pub trait TracingExecutionBackend: ExecutionBackend {
    fn execute_block_with_traces(
        &self,
        block: &StarknetBlock,
        state: &mut dyn MutableState,
    ) -> Result<(ExecutionOutput, Vec<CasmTrace>), ExecutionError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionMode {
    CanonicalOnly,
    FastOnly,
    DualWithVerification { verification_depth: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MismatchPolicy {
    WarnAndFallback,
    Halt,
    CooldownThenRetry { cooldown_blocks: u64 },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DualExecutionMetrics {
    pub mismatches: u64,
    pub fast_executions: u64,
    pub canonical_executions: u64,
}

pub struct DualExecutionBackend {
    fast: Option<Box<dyn ExecutionBackend>>,
    canonical: Box<dyn ExecutionBackend>,
    mode: ExecutionMode,
    mismatch_policy: MismatchPolicy,
    metrics: Mutex<DualExecutionMetrics>,
    cooldown_remaining: Mutex<u64>,
    verification_tip: Mutex<Option<u64>>,
    verification_shadow_state: Mutex<Option<Box<dyn MutableState>>>,
}

struct VerificationTipScope<'a> {
    backend: &'a DualExecutionBackend,
    active: bool,
}

impl<'a> VerificationTipScope<'a> {
    fn enter(backend: &'a DualExecutionBackend, tip: u64) -> Result<Self, ExecutionError> {
        backend.set_verification_tip(tip)?;
        Ok(Self {
            backend,
            active: true,
        })
    }

    fn close(mut self) -> Result<(), ExecutionError> {
        self.backend.clear_verification_tip()?;
        self.active = false;
        Ok(())
    }
}

impl Drop for VerificationTipScope<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Err(error) = self.backend.clear_verification_tip() {
            log::error!("failed to clear verification tip during scope teardown: {error}");
        }
        self.active = false;
    }
}

impl DualExecutionBackend {
    fn poisoned_lock(name: &'static str) -> ExecutionError {
        log::error!(
            "invariant violation: dual execution {} mutex poisoned; aborting affected operation",
            name
        );
        ExecutionError::Backend(format!("invariant violation: {name} mutex poisoned"))
    }

    pub fn new(
        fast: Option<Box<dyn ExecutionBackend>>,
        canonical: Box<dyn ExecutionBackend>,
        mode: ExecutionMode,
        mismatch_policy: MismatchPolicy,
    ) -> Self {
        Self {
            fast,
            canonical,
            mode,
            mismatch_policy,
            metrics: Mutex::new(DualExecutionMetrics::default()),
            cooldown_remaining: Mutex::new(0),
            verification_tip: Mutex::new(None),
            verification_shadow_state: Mutex::new(None),
        }
    }

    pub fn set_verification_tip(&self, tip: u64) -> Result<(), ExecutionError> {
        let mut guard = self
            .verification_tip
            .lock()
            .map_err(|_| Self::poisoned_lock("verification tip"))?;
        *guard = Some(tip);
        Ok(())
    }

    pub fn clear_verification_tip(&self) -> Result<(), ExecutionError> {
        let mut guard = self
            .verification_tip
            .lock()
            .map_err(|_| Self::poisoned_lock("verification tip"))?;
        *guard = None;
        Ok(())
    }

    pub fn with_verification_tip<T>(
        &self,
        tip: u64,
        f: impl FnOnce(&Self) -> Result<T, ExecutionError>,
    ) -> Result<T, ExecutionError> {
        let scope = VerificationTipScope::enter(self, tip)?;
        let result = f(self);
        let clear_result = scope.close();
        match (result, clear_result) {
            (Ok(value), Ok(())) => Ok(value),
            (Ok(_), Err(clear_error)) => Err(clear_error),
            (Err(run_error), Ok(())) => Err(run_error),
            (Err(run_error), Err(clear_error)) => Err(ExecutionError::Backend(format!(
                "verification tip scope failed: run_error={run_error}; clear_error={clear_error}"
            ))),
        }
    }

    pub fn metrics(&self) -> Result<DualExecutionMetrics, ExecutionError> {
        let guard = self
            .metrics
            .lock()
            .map_err(|_| Self::poisoned_lock("metrics"))?;
        Ok(guard.clone())
    }

    fn with_metrics(
        &self,
        f: impl FnOnce(&mut DualExecutionMetrics),
    ) -> Result<(), ExecutionError> {
        let mut guard = self
            .metrics
            .lock()
            .map_err(|_| Self::poisoned_lock("metrics"))?;
        f(&mut guard);
        Ok(())
    }

    fn should_verify_block(
        &self,
        block_number: u64,
        verification_depth: u64,
    ) -> Result<bool, ExecutionError> {
        if verification_depth == 0 {
            return Ok(false);
        }

        let tip = *self
            .verification_tip
            .lock()
            .map_err(|_| Self::poisoned_lock("verification tip"))?;
        let Some(tip) = tip else {
            return Ok(true);
        };

        if block_number > tip {
            return Ok(true);
        }

        // `verification_depth` counts blocks including the tip.
        // depth=1 verifies only tip; depth=2 verifies tip and tip-1.
        let window = verification_depth.saturating_sub(1);
        let window_start = tip.saturating_sub(window);
        Ok(block_number >= window_start)
    }

    fn apply_state_diff(
        state: &mut dyn MutableState,
        diff: &starknet_node_types::StarknetStateDiff,
    ) {
        for (contract, writes) in &diff.storage_diffs {
            for (key, value) in writes {
                state.set_storage(contract.clone(), key.clone(), *value);
            }
        }
        for (contract, nonce) in &diff.nonces {
            state.set_nonce(contract.clone(), *nonce);
        }
    }

    fn update_shadow_with_diff(
        &self,
        diff: &starknet_node_types::StarknetStateDiff,
    ) -> Result<(), ExecutionError> {
        let mut guard = self
            .verification_shadow_state
            .lock()
            .map_err(|_| Self::poisoned_lock("verification shadow state"))?;
        if let Some(shadow) = guard.as_mut() {
            Self::apply_state_diff(shadow.as_mut(), diff);
        }
        Ok(())
    }

    fn take_or_seed_shadow(
        &self,
        state: &dyn MutableState,
    ) -> Result<Box<dyn MutableState>, ExecutionError> {
        let mut guard = self
            .verification_shadow_state
            .lock()
            .map_err(|_| Self::poisoned_lock("verification shadow state"))?;
        Ok(guard.take().unwrap_or_else(|| state.boxed_clone()))
    }

    fn store_shadow(&self, shadow: Box<dyn MutableState>) -> Result<(), ExecutionError> {
        let mut guard = self
            .verification_shadow_state
            .lock()
            .map_err(|_| Self::poisoned_lock("verification shadow state"))?;
        *guard = Some(shadow);
        Ok(())
    }

    fn reset_shadow_from_state(&self, state: &dyn MutableState) -> Result<(), ExecutionError> {
        self.store_shadow(state.boxed_clone())
    }

    pub fn execute_verified(
        &self,
        block: &StarknetBlock,
        state: &mut dyn MutableState,
    ) -> Result<ExecutionOutput, ExecutionError> {
        let mut cooldown = self
            .cooldown_remaining
            .lock()
            .map_err(|_| Self::poisoned_lock("cooldown"))?;
        let forced_canonical = if *cooldown > 0 {
            *cooldown -= 1;
            true
        } else {
            false
        };
        drop(cooldown);

        let effective_mode = if forced_canonical {
            ExecutionMode::CanonicalOnly
        } else {
            self.mode.clone()
        };

        match effective_mode {
            ExecutionMode::CanonicalOnly => {
                let out = self.canonical.execute_block(block, state)?;
                self.update_shadow_with_diff(&out.state_diff)?;
                self.with_metrics(|metrics| metrics.canonical_executions += 1)?;
                Ok(out)
            }
            ExecutionMode::FastOnly => {
                if let Some(fast) = &self.fast {
                    let out = fast.execute_block(block, state)?;
                    self.update_shadow_with_diff(&out.state_diff)?;
                    self.with_metrics(|metrics| metrics.fast_executions += 1)?;
                    Ok(out)
                } else {
                    let out = self.canonical.execute_block(block, state)?;
                    self.update_shadow_with_diff(&out.state_diff)?;
                    self.with_metrics(|metrics| metrics.canonical_executions += 1)?;
                    Ok(out)
                }
            }
            ExecutionMode::DualWithVerification { verification_depth } => {
                let Some(fast) = &self.fast else {
                    let out = self.canonical.execute_block(block, state)?;
                    self.update_shadow_with_diff(&out.state_diff)?;
                    self.with_metrics(|metrics| metrics.canonical_executions += 1)?;
                    return Ok(out);
                };

                if !self.should_verify_block(block.number, verification_depth)? {
                    let out = fast.execute_block(block, state)?;
                    self.update_shadow_with_diff(&out.state_diff)?;
                    self.with_metrics(|metrics| metrics.fast_executions += 1)?;
                    return Ok(out);
                }

                // Reuse a long-lived verification shadow to avoid cloning full state per block.
                let mut fast_state = self.take_or_seed_shadow(state)?;
                let fast_output = fast.execute_block(block, fast_state.as_mut())?;
                let mut canonical_state = state.boxed_clone();
                let canonical_output = self
                    .canonical
                    .execute_block(block, canonical_state.as_mut())?;

                self.with_metrics(|metrics| {
                    metrics.fast_executions += 1;
                    metrics.canonical_executions += 1;
                })?;

                let outputs_match = fast_output.receipts == canonical_output.receipts
                    && fast_output.state_diff == canonical_output.state_diff;
                if fast_output.builtin_stats != canonical_output.builtin_stats {
                    log::warn!(
                        "warning: dual verification observed differing builtin stats at block {}",
                        block.number
                    );
                }
                if outputs_match {
                    Self::apply_state_diff(state, &canonical_output.state_diff);
                    self.store_shadow(fast_state)?;
                    return Ok(canonical_output);
                }

                self.with_metrics(|metrics| metrics.mismatches += 1)?;
                match self.mismatch_policy {
                    MismatchPolicy::WarnAndFallback => {
                        Self::apply_state_diff(state, &canonical_output.state_diff);
                        self.reset_shadow_from_state(state)?;
                        Ok(canonical_output)
                    }
                    MismatchPolicy::Halt => {
                        self.reset_shadow_from_state(state)?;
                        Err(ExecutionError::Halted)
                    }
                    MismatchPolicy::CooldownThenRetry { cooldown_blocks } => {
                        Self::apply_state_diff(state, &canonical_output.state_diff);
                        let mut cooldown = self
                            .cooldown_remaining
                            .lock()
                            .map_err(|_| Self::poisoned_lock("cooldown"))?;
                        *cooldown = cooldown_blocks;
                        self.reset_shadow_from_state(state)?;
                        Ok(canonical_output)
                    }
                }
            }
        }
    }
}

impl ExecutionBackend for DualExecutionBackend {
    fn execute_block(
        &self,
        block: &StarknetBlock,
        state: &mut dyn MutableState,
    ) -> Result<ExecutionOutput, ExecutionError> {
        self.execute_verified(block, state)
    }

    fn simulate_tx(
        &self,
        tx: &StarknetTransaction,
        state: &dyn StateReader,
        block_context: &BlockContext,
    ) -> Result<SimulationResult, ExecutionError> {
        match self.mode {
            ExecutionMode::FastOnly => {
                if let Some(fast) = &self.fast {
                    fast.simulate_tx(tx, state, block_context)
                } else {
                    self.canonical.simulate_tx(tx, state, block_context)
                }
            }
            ExecutionMode::CanonicalOnly | ExecutionMode::DualWithVerification { .. } => {
                self.canonical.simulate_tx(tx, state, block_context)
            }
        }
    }
}

pub struct ProtocolVersionSelector {
    resolver: VersionedConstantsResolver,
}

impl ProtocolVersionSelector {
    pub fn new(entries: impl IntoIterator<Item = (Version, VersionedConstants)>) -> Self {
        Self {
            resolver: VersionedConstantsResolver::new(entries),
        }
    }

    pub fn constants_for_block(
        &self,
        block: &StarknetBlock,
    ) -> Result<&VersionedConstants, ExecutionError> {
        self.resolver
            .resolve_for_protocol(&block.protocol_version)
            .map_err(|VersionResolutionError::Missing { requested }| {
                ExecutionError::MissingConstants(requested)
            })
    }
}

#[cfg(feature = "blockifier-adapter")]
#[derive(Clone, Debug)]
pub struct BlockifierProtocolVersionResolver {
    versions: BTreeMap<Version, BlockifierProtocolVersion>,
}

#[cfg(feature = "blockifier-adapter")]
impl BlockifierProtocolVersionResolver {
    pub fn new(entries: impl IntoIterator<Item = (Version, BlockifierProtocolVersion)>) -> Self {
        Self {
            versions: entries.into_iter().collect(),
        }
    }

    pub fn starknet_mainnet_defaults() -> Self {
        let versions = [
            ("0.13.0", "0.13.0"),
            ("0.13.1", "0.13.1"),
            ("0.13.1-1", "0.13.1.1"),
            ("0.13.2", "0.13.2"),
            ("0.13.2-1", "0.13.2.1"),
            ("0.13.3", "0.13.3"),
            ("0.13.4", "0.13.4"),
            ("0.13.5", "0.13.5"),
            ("0.13.6", "0.13.6"),
            ("0.14.0", "0.14.0"),
            ("0.14.1", "0.14.1"),
            ("0.14.2", "0.14.2"),
        ];
        let entries = versions.into_iter().map(|(semver, raw)| {
            let semver = Version::parse(semver).expect("known valid protocol version");
            let protocol = BlockifierProtocolVersion::try_from(raw)
                .expect("blockifier supports bundled protocol version");
            (semver, protocol)
        });
        Self::new(entries)
    }

    pub fn resolve_for_block(
        &self,
        requested: &Version,
    ) -> Result<BlockifierProtocolVersion, ExecutionError> {
        self.versions
            .get(requested)
            .copied()
            .ok_or_else(|| ExecutionError::MissingConstants(requested.clone()))
    }
}

#[cfg(feature = "blockifier-adapter")]
fn blockifier_felt_to_node_felt(
    value: BlockifierFelt,
    field: &'static str,
) -> Result<StarknetFelt, ExecutionError> {
    let encoded = format!("{:#x}", value);
    StarknetFelt::from_hex(&encoded).map_err(|error| {
        ExecutionError::Backend(format!(
            "failed to parse blockifier felt for {field}: {encoded} ({error})"
        ))
    })
}

#[cfg(feature = "blockifier-adapter")]
fn node_felt_to_blockifier(
    value: StarknetFelt,
    field: &'static str,
) -> Result<BlockifierFelt, StateError> {
    let encoded = format!("{:#x}", value);
    BlockifierFelt::from_str(&encoded).map_err(|error| {
        StateError::StateReadError(format!(
            "failed to encode felt for blockifier {field}: {encoded} ({error})"
        ))
    })
}

#[cfg(feature = "blockifier-adapter")]
fn parse_blockifier_sequencer_address(
    value: &str,
) -> Result<BlockifierContractAddress, ExecutionError> {
    let felt = BlockifierFelt::from_str(value).map_err(|error| {
        ExecutionError::Backend(format!("invalid sequencer_address '{value}': {error}"))
    })?;
    BlockifierContractAddress::try_from(felt).map_err(|error| {
        ExecutionError::Backend(format!("sequencer_address out of range '{value}': {error}"))
    })
}

#[cfg(feature = "blockifier-adapter")]
fn nonzero_gas_price(
    value: u128,
    field: &'static str,
) -> Result<BlockifierNonzeroGasPrice, ExecutionError> {
    BlockifierNonzeroGasPrice::try_from(BlockifierGasPrice(value)).map_err(|error| {
        ExecutionError::Backend(format!("invalid {field} gas price {value}: {error}"))
    })
}

#[cfg(feature = "blockifier-adapter")]
fn map_block_gas_prices(prices: &BlockGasPrices) -> Result<BlockifierGasPrices, ExecutionError> {
    Ok(BlockifierGasPrices {
        eth_gas_prices: BlockifierGasPriceVector {
            l1_gas_price: nonzero_gas_price(prices.l1_gas.price_in_wei, "l1_gas.price_in_wei")?,
            l1_data_gas_price: nonzero_gas_price(
                prices.l1_data_gas.price_in_wei,
                "l1_data_gas.price_in_wei",
            )?,
            l2_gas_price: nonzero_gas_price(prices.l2_gas.price_in_wei, "l2_gas.price_in_wei")?,
        },
        strk_gas_prices: BlockifierGasPriceVector {
            l1_gas_price: nonzero_gas_price(prices.l1_gas.price_in_fri, "l1_gas.price_in_fri")?,
            l1_data_gas_price: nonzero_gas_price(
                prices.l1_data_gas.price_in_fri,
                "l1_data_gas.price_in_fri",
            )?,
            l2_gas_price: nonzero_gas_price(prices.l2_gas.price_in_fri, "l2_gas.price_in_fri")?,
        },
    })
}

#[cfg(feature = "blockifier-adapter")]
type SharedStateSnapshot = Arc<Mutex<Box<dyn MutableState>>>;

#[cfg(feature = "blockifier-adapter")]
pub trait BlockifierClassProvider: Send + Sync {
    fn supports_account_execution(&self) -> bool {
        false
    }

    /// Called before block execution to let providers bind their lookups to the
    /// pre-state of `block_number`.
    fn prepare_for_block_execution(&self, _block_number: u64) -> Result<(), StateError> {
        Ok(())
    }

    /// Called before simulation to let providers bind their lookups to the
    /// requested simulation state at `block_number`.
    fn prepare_for_simulation(&self, _block_number: u64) -> Result<(), StateError> {
        Ok(())
    }

    fn get_class_hash_at(
        &self,
        contract_address: BlockifierContractAddress,
    ) -> Result<BlockifierClassHash, StateError>;

    fn get_compiled_class(
        &self,
        class_hash: BlockifierClassHash,
    ) -> Result<RunnableCompiledClass, StateError>;

    fn get_compiled_class_hash(
        &self,
        class_hash: BlockifierClassHash,
    ) -> Result<BlockifierCompiledClassHash, StateError>;
}

#[cfg(feature = "blockifier-adapter")]
#[derive(Default)]
struct UnsupportedBlockifierClassProvider;

#[cfg(feature = "blockifier-adapter")]
impl BlockifierClassProvider for UnsupportedBlockifierClassProvider {
    fn get_class_hash_at(
        &self,
        _contract_address: BlockifierContractAddress,
    ) -> Result<BlockifierClassHash, StateError> {
        Err(StateError::StateReadError(
            "class hash lookup requires a configured BlockifierClassProvider".to_string(),
        ))
    }

    fn get_compiled_class(
        &self,
        _class_hash: BlockifierClassHash,
    ) -> Result<RunnableCompiledClass, StateError> {
        Err(StateError::StateReadError(
            "compiled class lookup requires a configured BlockifierClassProvider".to_string(),
        ))
    }

    fn get_compiled_class_hash(
        &self,
        _class_hash: BlockifierClassHash,
    ) -> Result<BlockifierCompiledClassHash, StateError> {
        Err(StateError::StateReadError(
            "compiled class hash lookup requires a configured BlockifierClassProvider".to_string(),
        ))
    }
}

#[cfg(feature = "blockifier-adapter")]
struct BlockifierStateReaderAdapter {
    state: SharedStateSnapshot,
    class_provider: Arc<dyn BlockifierClassProvider>,
}

#[cfg(feature = "blockifier-adapter")]
impl BlockifierStateReaderAdapter {
    fn new(state: SharedStateSnapshot, class_provider: Arc<dyn BlockifierClassProvider>) -> Self {
        Self {
            state,
            class_provider,
        }
    }
}

#[cfg(feature = "blockifier-adapter")]
impl BlockifierStateReader for BlockifierStateReaderAdapter {
    fn get_storage_at(
        &self,
        contract_address: BlockifierContractAddress,
        key: BlockifierStorageKey,
    ) -> Result<BlockifierFelt, StateError> {
        let state = self
            .state
            .lock()
            .map_err(|_| StateError::StateReadError("state snapshot mutex poisoned".to_string()))?;
        let contract_felt: BlockifierFelt = contract_address.into();
        let key_felt: BlockifierFelt = key.into();
        let contract = starknet_node_types::ContractAddress::parse(format!("{:#x}", contract_felt))
            .expect("valid contract address");
        let value = state
            .get_storage(&contract, &format!("{:#x}", key_felt))
            .map_err(|error| StateError::StateReadError(error.to_string()))?
            .unwrap_or_default();
        node_felt_to_blockifier(value, "state.storage")
    }

    fn get_nonce_at(
        &self,
        contract_address: BlockifierContractAddress,
    ) -> Result<BlockifierNonce, StateError> {
        let state = self
            .state
            .lock()
            .map_err(|_| StateError::StateReadError("state snapshot mutex poisoned".to_string()))?;
        let contract_felt: BlockifierFelt = contract_address.into();
        let contract = starknet_node_types::ContractAddress::parse(format!("{:#x}", contract_felt))
            .expect("valid contract address");
        let nonce = state
            .nonce_of(&contract)
            .map_err(|error| StateError::StateReadError(error.to_string()))?
            .unwrap_or_default();
        Ok(BlockifierNonce(node_felt_to_blockifier(
            nonce,
            "state.nonce",
        )?))
    }

    fn get_class_hash_at(
        &self,
        contract_address: BlockifierContractAddress,
    ) -> Result<BlockifierClassHash, StateError> {
        self.class_provider.get_class_hash_at(contract_address)
    }

    fn get_compiled_class(
        &self,
        class_hash: BlockifierClassHash,
    ) -> Result<RunnableCompiledClass, StateError> {
        self.class_provider.get_compiled_class(class_hash)
    }

    fn get_compiled_class_hash(
        &self,
        class_hash: BlockifierClassHash,
    ) -> Result<BlockifierCompiledClassHash, StateError> {
        self.class_provider.get_compiled_class_hash(class_hash)
    }
}

#[cfg(feature = "blockifier-adapter")]
struct BlockifierReadOnlyStateAdapter<'a> {
    state: &'a dyn StateReader,
    class_provider: Arc<dyn BlockifierClassProvider>,
}

#[cfg(feature = "blockifier-adapter")]
impl<'a> BlockifierReadOnlyStateAdapter<'a> {
    fn new(state: &'a dyn StateReader, class_provider: Arc<dyn BlockifierClassProvider>) -> Self {
        Self {
            state,
            class_provider,
        }
    }
}

#[cfg(feature = "blockifier-adapter")]
impl BlockifierStateReader for BlockifierReadOnlyStateAdapter<'_> {
    fn get_storage_at(
        &self,
        contract_address: BlockifierContractAddress,
        key: BlockifierStorageKey,
    ) -> Result<BlockifierFelt, StateError> {
        let contract_felt: BlockifierFelt = contract_address.into();
        let key_felt: BlockifierFelt = key.into();
        let contract = starknet_node_types::ContractAddress::parse(format!("{:#x}", contract_felt))
            .expect("valid contract address");
        let value = self
            .state
            .get_storage(&contract, &format!("{:#x}", key_felt))
            .map_err(|error| StateError::StateReadError(error.to_string()))?
            .unwrap_or_default();
        node_felt_to_blockifier(value, "state.storage")
    }

    fn get_nonce_at(
        &self,
        contract_address: BlockifierContractAddress,
    ) -> Result<BlockifierNonce, StateError> {
        let contract_felt: BlockifierFelt = contract_address.into();
        let contract = starknet_node_types::ContractAddress::parse(format!("{:#x}", contract_felt))
            .expect("valid contract address");
        let nonce = self
            .state
            .nonce_of(&contract)
            .map_err(|error| StateError::StateReadError(error.to_string()))?
            .unwrap_or_default();
        Ok(BlockifierNonce(node_felt_to_blockifier(
            nonce,
            "state.nonce",
        )?))
    }

    fn get_class_hash_at(
        &self,
        contract_address: BlockifierContractAddress,
    ) -> Result<BlockifierClassHash, StateError> {
        self.class_provider.get_class_hash_at(contract_address)
    }

    fn get_compiled_class(
        &self,
        class_hash: BlockifierClassHash,
    ) -> Result<RunnableCompiledClass, StateError> {
        self.class_provider.get_compiled_class(class_hash)
    }

    fn get_compiled_class_hash(
        &self,
        class_hash: BlockifierClassHash,
    ) -> Result<BlockifierCompiledClassHash, StateError> {
        self.class_provider.get_compiled_class_hash(class_hash)
    }
}

#[cfg(feature = "blockifier-adapter")]
pub trait ExecutableTransactionResolver: Send + Sync {
    fn resolve(
        &self,
        block_number: u64,
        tx: &StarknetTransaction,
    ) -> Result<ExecutableStarknetTransaction, ExecutionError>;
}

#[cfg(feature = "blockifier-adapter")]
#[derive(Default)]
pub struct EmbeddedExecutablePayloadResolver;

#[cfg(feature = "blockifier-adapter")]
impl ExecutableTransactionResolver for EmbeddedExecutablePayloadResolver {
    fn resolve(
        &self,
        block_number: u64,
        tx: &StarknetTransaction,
    ) -> Result<ExecutableStarknetTransaction, ExecutionError> {
        tx.executable.clone().ok_or_else(|| {
            ExecutionError::Backend(format!(
                "missing executable payload for tx {} in block {}",
                tx.hash, block_number
            ))
        })
    }
}

#[cfg(feature = "blockifier-adapter")]
pub struct BlockifierVmBackend {
    version_resolver: BlockifierProtocolVersionResolver,
    chain_info: BlockifierChainInfo,
    executor_config: TransactionExecutorConfig,
    tx_resolver: Arc<dyn ExecutableTransactionResolver>,
    class_provider: Arc<dyn BlockifierClassProvider>,
    execution_timeout: StdDuration,
    last_executed_block: Mutex<Option<u64>>,
    state_snapshot: Mutex<Option<SharedStateSnapshot>>,
}

#[cfg(feature = "blockifier-adapter")]
impl BlockifierVmBackend {
    pub fn new(
        version_resolver: BlockifierProtocolVersionResolver,
        chain_info: BlockifierChainInfo,
        executor_config: TransactionExecutorConfig,
    ) -> Self {
        Self {
            version_resolver,
            chain_info,
            executor_config,
            tx_resolver: Arc::new(EmbeddedExecutablePayloadResolver),
            class_provider: Arc::new(UnsupportedBlockifierClassProvider),
            execution_timeout: StdDuration::from_secs(30),
            last_executed_block: Mutex::new(None),
            state_snapshot: Mutex::new(None),
        }
    }

    pub fn with_tx_resolver(mut self, tx_resolver: Arc<dyn ExecutableTransactionResolver>) -> Self {
        self.tx_resolver = tx_resolver;
        self
    }

    pub fn with_class_provider(mut self, class_provider: Arc<dyn BlockifierClassProvider>) -> Self {
        self.class_provider = class_provider;
        self
    }

    pub fn with_execution_timeout(mut self, timeout: StdDuration) -> Self {
        self.execution_timeout = timeout;
        self
    }

    pub fn starknet_mainnet() -> Self {
        Self::new(
            BlockifierProtocolVersionResolver::starknet_mainnet_defaults(),
            BlockifierChainInfo::default(),
            TransactionExecutorConfig::default(),
        )
    }

    fn simulation_gas_prices() -> BlockGasPrices {
        BlockGasPrices {
            l1_gas: GasPricePerToken {
                price_in_fri: 1,
                price_in_wei: 1,
            },
            l1_data_gas: GasPricePerToken {
                price_in_fri: 1,
                price_in_wei: 1,
            },
            l2_gas: GasPricePerToken {
                price_in_fri: 1,
                price_in_wei: 1,
            },
        }
    }

    fn build_block_context_from_parts(
        &self,
        block_number: u64,
        timestamp: u64,
        protocol_version: &Version,
        sequencer_address: &str,
        gas_prices: &BlockGasPrices,
    ) -> Result<BlockifierBlockContext, ExecutionError> {
        let protocol = self.version_resolver.resolve_for_block(protocol_version)?;
        let versioned_constants = BlockifierVersionedConstants::get(&protocol)
            .map_err(|error| ExecutionError::Backend(error.to_string()))?;
        let sequencer_address = parse_blockifier_sequencer_address(sequencer_address)?;
        let gas_prices = map_block_gas_prices(gas_prices)?;

        let block_info = BlockifierBlockInfo {
            block_number: BlockifierBlockNumber(block_number),
            block_timestamp: BlockifierBlockTimestamp(timestamp),
            starknet_version: protocol,
            sequencer_address,
            gas_prices,
            use_kzg_da: false,
        };

        Ok(BlockifierBlockContext::new(
            block_info,
            self.chain_info.clone(),
            versioned_constants.clone(),
            BouncerConfig::default(),
        ))
    }

    fn build_block_context(
        &self,
        block: &StarknetBlock,
    ) -> Result<BlockifierBlockContext, ExecutionError> {
        self.build_block_context_from_parts(
            block.number,
            block.timestamp,
            &block.protocol_version,
            &block.sequencer_address,
            &block.gas_prices,
        )
    }

    fn build_simulation_context(
        &self,
        block_context: &BlockContext,
    ) -> Result<BlockifierBlockContext, ExecutionError> {
        self.build_block_context_from_parts(
            block_context.block_number,
            0,
            &block_context.protocol_version,
            "0x1",
            &Self::simulation_gas_prices(),
        )
    }

    fn map_transaction_for_block(
        &self,
        block_number: u64,
        tx: &StarknetTransaction,
    ) -> Result<BlockifierTransaction, ExecutionError> {
        let executable = self.tx_resolver.resolve(block_number, tx)?;
        let expected_hash = BlockifierFelt::from_str(&tx.hash).map_err(|error| {
            ExecutionError::Backend(format!(
                "invalid tx hash '{}' in block {}: {}",
                tx.hash, block_number, error
            ))
        })?;
        if executable.tx_hash().0 != expected_hash {
            return Err(ExecutionError::Backend(format!(
                "resolver tx hash mismatch for block {}: expected {:#x}, got {:#x}",
                block_number,
                expected_hash,
                executable.tx_hash().0
            )));
        }
        match &executable {
            ExecutableStarknetTransaction::L1Handler(l1_handler)
                if l1_handler.tx.calldata.0.is_empty() =>
            {
                return Err(ExecutionError::Backend(format!(
                    "invalid L1 handler tx {} in block {}: calldata must include the \
                     from-address slot",
                    tx.hash, block_number
                )));
            }
            ExecutableStarknetTransaction::L1Handler(_) => {}
            ExecutableStarknetTransaction::Account(account_tx) => {
                if self.class_provider.supports_account_execution() {
                    return Ok(BlockifierTransaction::new_for_sequencing(executable));
                }
                return Err(ExecutionError::Backend(format!(
                    "unsupported executable transaction type {:?} for tx {} in block {}: \
                     class-provider integration is required and currently unavailable",
                    account_tx.tx_type(),
                    tx.hash,
                    block_number
                )));
            }
        }
        Ok(BlockifierTransaction::new_for_sequencing(executable))
    }

    fn map_block_transactions(
        &self,
        block: &StarknetBlock,
    ) -> Result<Vec<BlockifierTransaction>, ExecutionError> {
        block
            .transactions
            .iter()
            .map(|tx| self.map_transaction_for_block(block.number, tx))
            .collect()
    }

    fn gas_consumed_from_receipt(receipt: &blockifier::fee::receipt::TransactionReceipt) -> u64 {
        receipt
            .gas
            .l1_gas
            .0
            .saturating_add(receipt.gas.l1_data_gas.0)
            .saturating_add(receipt.gas.l2_gas.0)
    }

    fn validate_sequential_block(&self, block_number: u64) -> Result<(), ExecutionError> {
        let guard = self
            .last_executed_block
            .lock()
            .map_err(|_| ExecutionError::Backend("block sequence mutex poisoned".to_string()))?;

        if let Some(previous) = *guard {
            let expected = previous.checked_add(1).ok_or_else(|| {
                ExecutionError::Backend(format!("block sequence overflow after block {previous}"))
            })?;
            if block_number != expected {
                drop(guard);
                self.clear_state_snapshot()?;
                return Err(ExecutionError::Backend(format!(
                    "non-sequential block execution: expected {expected}, got {block_number}"
                )));
            }
        }

        Ok(())
    }

    fn record_executed_block(&self, block_number: u64) -> Result<(), ExecutionError> {
        let mut guard = self
            .last_executed_block
            .lock()
            .map_err(|_| ExecutionError::Backend("block sequence mutex poisoned".to_string()))?;
        *guard = Some(block_number);
        Ok(())
    }

    fn clear_state_snapshot(&self) -> Result<(), ExecutionError> {
        let mut guard = self
            .state_snapshot
            .lock()
            .map_err(|_| ExecutionError::Backend("state snapshot mutex poisoned".to_string()))?;
        *guard = None;
        Ok(())
    }

    fn snapshot_for_execution(
        &self,
        state: &dyn MutableState,
    ) -> Result<SharedStateSnapshot, ExecutionError> {
        let mut guard = self
            .state_snapshot
            .lock()
            .map_err(|_| ExecutionError::Backend("state snapshot mutex poisoned".to_string()))?;
        if let Some(snapshot) = guard.as_ref() {
            return Ok(Arc::clone(snapshot));
        }

        let snapshot: SharedStateSnapshot = Arc::new(Mutex::new(state.boxed_clone()));
        *guard = Some(Arc::clone(&snapshot));
        Ok(snapshot)
    }

    fn apply_state_diff_to_snapshot(
        snapshot: &SharedStateSnapshot,
        diff: &starknet_node_types::StarknetStateDiff,
    ) -> Result<(), ExecutionError> {
        let mut guard = snapshot
            .lock()
            .map_err(|_| ExecutionError::Backend("state snapshot mutex poisoned".to_string()))?;
        for (contract, writes) in &diff.storage_diffs {
            for (key, value) in writes {
                guard.set_storage(contract.clone(), key.clone(), *value);
            }
        }
        for (contract, nonce) in &diff.nonces {
            guard.set_nonce(contract.clone(), *nonce);
        }
        Ok(())
    }
}

#[cfg(feature = "blockifier-adapter")]
impl ExecutionBackend for BlockifierVmBackend {
    fn execute_block(
        &self,
        block: &StarknetBlock,
        state: &mut dyn MutableState,
    ) -> Result<ExecutionOutput, ExecutionError> {
        block
            .validate()
            .map_err(|error| ExecutionError::Backend(format!("invalid block input: {error}")))?;
        self.validate_sequential_block(block.number)?;
        let started_at = Instant::now();
        let block_context = self.build_block_context(block)?;
        self.class_provider
            .prepare_for_block_execution(block.number)
            .map_err(|error| {
                ExecutionError::Backend(format!(
                    "class provider preparation failed for block execution {}: {}",
                    block.number, error
                ))
            })?;
        let txs = self.map_block_transactions(block)?;
        let snapshot = self.snapshot_for_execution(state)?;
        let result: Result<ExecutionOutput, ExecutionError> = (|| {
            let mut executor = TransactionExecutor::pre_process_and_create(
                BlockifierStateReaderAdapter::new(
                    Arc::clone(&snapshot),
                    Arc::clone(&self.class_provider),
                ),
                block_context,
                (block.number >= 10).then_some(BlockifierBlockHashAndNumber {
                    hash: BlockifierBlockHash::default(),
                    number: BlockifierBlockNumber(block.number.saturating_sub(10)),
                }),
                self.executor_config.clone(),
            )
            .map_err(|error| ExecutionError::Backend(error.to_string()))?;

            let execution_results =
                executor.execute_txs(&txs, Some(Instant::now() + self.execution_timeout));
            if execution_results.len() != txs.len() {
                return Err(ExecutionError::Backend(format!(
                    "blockifier returned {} execution results for {} transactions in block {}",
                    execution_results.len(),
                    txs.len(),
                    block.number
                )));
            }

            let receipts = execution_results
                .into_iter()
                .zip(block.transactions.iter())
                .map(|(result, tx)| match result {
                    Ok((execution_info, _)) => Ok(starknet_node_types::StarknetReceipt {
                        tx_hash: tx.hash.clone(),
                        execution_status: !execution_info.is_reverted(),
                        events: execution_info
                            .non_optional_call_infos()
                            .map(|call_info| call_info.execution.events.len() as u64)
                            .sum(),
                        gas_consumed: Self::gas_consumed_from_receipt(&execution_info.receipt),
                    }),
                    Err(TransactionExecutorError::TransactionExecutionError(_)) => {
                        Ok(starknet_node_types::StarknetReceipt {
                            tx_hash: tx.hash.clone(),
                            execution_status: false,
                            events: 0,
                            gas_consumed: 0,
                        })
                    }
                    Err(other) => Err(ExecutionError::Backend(format!(
                        "fatal blockifier transaction executor error at block {}: {}",
                        block.number, other
                    ))),
                })
                .collect::<Result<Vec<_>, _>>()?;

            let summary = executor
                .finalize()
                .map_err(|error| ExecutionError::Backend(error.to_string()))?;

            let mut state_diff = starknet_node_types::StarknetStateDiff::default();
            for (address, writes) in summary.state_diff.storage_updates {
                let contract_felt: BlockifierFelt = address.into();
                let contract =
                    starknet_node_types::ContractAddress::parse(format!("{:#x}", contract_felt))
                        .expect("valid contract address");
                let contract_writes = state_diff.storage_diffs.entry(contract).or_default();
                for (key, value) in writes {
                    let key_felt: BlockifierFelt = key.into();
                    contract_writes.insert(
                        format!("{:#x}", key_felt),
                        blockifier_felt_to_node_felt(value, "state_diff.storage_diffs")?,
                    );
                }
            }
            for (address, nonce) in summary.state_diff.address_to_nonce {
                let contract_felt: BlockifierFelt = address.into();
                state_diff.nonces.insert(
                    starknet_node_types::ContractAddress::parse(format!("{:#x}", contract_felt))
                        .expect("valid contract address"),
                    blockifier_felt_to_node_felt(nonce.0, "state_diff.nonces")?,
                );
            }
            for class_hash in summary.state_diff.class_hash_to_compiled_class_hash.keys() {
                state_diff.declared_classes.push(
                    starknet_node_types::ClassHash::parse(format!("{:#x}", class_hash.0))
                        .expect("valid class hash"),
                );
            }
            state_diff.validate().map_err(|error| {
                ExecutionError::Backend(format!("invalid blockifier state diff output: {error}"))
            })?;

            for (contract, writes) in &state_diff.storage_diffs {
                for (key, value) in writes {
                    state.set_storage(contract.clone(), key.clone(), *value);
                }
            }
            for (contract, nonce) in &state_diff.nonces {
                state.set_nonce(contract.clone(), *nonce);
            }
            Self::apply_state_diff_to_snapshot(&snapshot, &state_diff)?;

            let output = ExecutionOutput {
                receipts,
                state_diff,
                builtin_stats: starknet_node_types::BuiltinStats::default(),
                execution_time: started_at.elapsed(),
            };
            self.record_executed_block(block.number)?;
            Ok(output)
        })();

        if result.is_err() {
            // Never reuse a potentially partially-mutated cached snapshot after failure.
            let _ = self.clear_state_snapshot();
        }
        result
    }

    fn simulate_tx(
        &self,
        tx: &StarknetTransaction,
        state: &dyn StateReader,
        block_context: &BlockContext,
    ) -> Result<SimulationResult, ExecutionError> {
        tx.validate_hash()
            .map_err(|error| ExecutionError::Backend(format!("invalid tx hash: {error}")))?;
        let blockifier_context = self.build_simulation_context(block_context)?;
        self.class_provider
            .prepare_for_simulation(block_context.block_number)
            .map_err(|error| {
                ExecutionError::Backend(format!(
                    "class provider preparation failed for simulation at block {}: {}",
                    block_context.block_number, error
                ))
            })?;
        let mapped_tx = self.map_transaction_for_block(block_context.block_number, tx)?;
        let mut executor = TransactionExecutor::pre_process_and_create(
            BlockifierReadOnlyStateAdapter::new(state, Arc::clone(&self.class_provider)),
            blockifier_context,
            (block_context.block_number >= 10).then_some(BlockifierBlockHashAndNumber {
                hash: BlockifierBlockHash::default(),
                number: BlockifierBlockNumber(block_context.block_number.saturating_sub(10)),
            }),
            self.executor_config.clone(),
        )
        .map_err(|error| ExecutionError::Backend(error.to_string()))?;

        match executor.execute(&mapped_tx) {
            Ok((execution_info, _)) => {
                let gas_consumed = Self::gas_consumed_from_receipt(&execution_info.receipt);
                Ok(SimulationResult {
                    receipt: starknet_node_types::StarknetReceipt {
                        tx_hash: tx.hash.clone(),
                        execution_status: !execution_info.is_reverted(),
                        events: execution_info
                            .non_optional_call_infos()
                            .map(|call_info| call_info.execution.events.len() as u64)
                            .sum(),
                        gas_consumed,
                    },
                    estimated_fee: u128::from(gas_consumed),
                })
            }
            Err(TransactionExecutorError::TransactionExecutionError(_)) => Ok(SimulationResult {
                receipt: starknet_node_types::StarknetReceipt {
                    tx_hash: tx.hash.clone(),
                    execution_status: false,
                    events: 0,
                    gas_consumed: 0,
                },
                estimated_fee: 0,
            }),
            Err(other) => Err(ExecutionError::Backend(format!(
                "fatal blockifier simulation error at block {} for tx {}: {}",
                block_context.block_number, tx.hash, other
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use semver::Version;
    use starknet_node_types::{
        BlockGasPrices, BuiltinStats, ContractAddress, ExecutionOutput, GasPricePerToken,
        InMemoryState, MutableState, SimulationResult, StarknetBlock, StarknetFelt,
        StarknetReceipt, StarknetStateDiff, StarknetTransaction, StateReader,
    };

    use super::*;

    struct ScriptedBackend {
        outputs: BTreeMap<u64, ExecutionOutput>,
        default: ExecutionOutput,
        _name: &'static str,
    }

    impl ScriptedBackend {
        fn new(
            name: &'static str,
            outputs: BTreeMap<u64, ExecutionOutput>,
            default: ExecutionOutput,
        ) -> Self {
            Self {
                outputs,
                default,
                _name: name,
            }
        }
    }

    impl ExecutionBackend for ScriptedBackend {
        fn execute_block(
            &self,
            block: &StarknetBlock,
            _state: &mut dyn MutableState,
        ) -> Result<ExecutionOutput, ExecutionError> {
            Ok(self
                .outputs
                .get(&block.number)
                .cloned()
                .unwrap_or_else(|| self.default.clone()))
        }

        fn simulate_tx(
            &self,
            tx: &StarknetTransaction,
            _state: &dyn StateReader,
            _block_context: &BlockContext,
        ) -> Result<SimulationResult, ExecutionError> {
            Ok(SimulationResult {
                receipt: StarknetReceipt {
                    tx_hash: tx.hash.clone(),
                    execution_status: true,
                    events: 0,
                    gas_consumed: 1,
                },
                estimated_fee: 1,
            })
        }
    }

    struct StateWritingBackend {
        gas: u64,
        value: StarknetFelt,
    }

    impl ExecutionBackend for StateWritingBackend {
        fn execute_block(
            &self,
            _block: &StarknetBlock,
            state: &mut dyn MutableState,
        ) -> Result<ExecutionOutput, ExecutionError> {
            let contract = ContractAddress::parse("0x1").expect("valid contract address");
            let key = "0x2".to_string();
            state.set_storage(contract.clone(), key.clone(), self.value);
            let mut out = output(self.gas);
            out.state_diff
                .storage_diffs
                .entry(contract)
                .or_default()
                .insert(key, self.value);
            Ok(out)
        }

        fn simulate_tx(
            &self,
            tx: &StarknetTransaction,
            _state: &dyn StateReader,
            _block_context: &BlockContext,
        ) -> Result<SimulationResult, ExecutionError> {
            Ok(SimulationResult {
                receipt: StarknetReceipt {
                    tx_hash: tx.hash.clone(),
                    execution_status: true,
                    events: 0,
                    gas_consumed: self.gas,
                },
                estimated_fee: self.gas as u128,
            })
        }
    }

    #[derive(Clone, Default)]
    struct CountingCloneState {
        inner: InMemoryState,
        clone_count: Arc<AtomicUsize>,
    }

    impl CountingCloneState {
        fn new(clone_count: Arc<AtomicUsize>) -> Self {
            Self {
                inner: InMemoryState::default(),
                clone_count,
            }
        }
    }

    impl StateReader for CountingCloneState {
        fn get_storage(
            &self,
            contract: &ContractAddress,
            key: &str,
        ) -> Result<Option<StarknetFelt>, starknet_node_types::StateReadError> {
            self.inner.get_storage(contract, key)
        }

        fn nonce_of(
            &self,
            contract: &ContractAddress,
        ) -> Result<Option<StarknetFelt>, starknet_node_types::StateReadError> {
            self.inner.nonce_of(contract)
        }
    }

    impl MutableState for CountingCloneState {
        fn set_storage(&mut self, contract: ContractAddress, key: String, value: StarknetFelt) {
            self.inner.set_storage(contract, key, value);
        }

        fn set_nonce(&mut self, contract: ContractAddress, nonce: StarknetFelt) {
            self.inner.set_nonce(contract, nonce);
        }

        fn boxed_clone(&self) -> Box<dyn MutableState> {
            self.clone_count.fetch_add(1, Ordering::SeqCst);
            Box::new(self.clone())
        }
    }

    fn sample_gas_prices() -> BlockGasPrices {
        BlockGasPrices {
            l1_gas: GasPricePerToken {
                price_in_fri: 2,
                price_in_wei: 3,
            },
            l1_data_gas: GasPricePerToken {
                price_in_fri: 4,
                price_in_wei: 5,
            },
            l2_gas: GasPricePerToken {
                price_in_fri: 6,
                price_in_wei: 7,
            },
        }
    }

    fn block(number: u64, version: &str) -> StarknetBlock {
        StarknetBlock {
            number,
            parent_hash: format!("0x{:x}", number.saturating_sub(1)),
            state_root: format!("0x{number:x}"),
            timestamp: 1_700_000_000 + number,
            sequencer_address: ContractAddress::parse("0x1").expect("valid contract address"),
            gas_prices: sample_gas_prices(),
            protocol_version: Version::parse(version).expect("valid version"),
            transactions: vec![StarknetTransaction::new(
                starknet_node_types::TxHash::parse(format!("0x{number:x}")).expect("valid tx hash"),
            )],
        }
    }

    #[cfg(feature = "blockifier-adapter")]
    fn context(block_number: u64, version: &str) -> BlockContext {
        BlockContext {
            block_number,
            protocol_version: Version::parse(version).expect("valid version"),
        }
    }

    #[cfg(feature = "blockifier-adapter")]
    fn empty_block(number: u64, version: &str) -> StarknetBlock {
        StarknetBlock {
            number,
            parent_hash: format!("0x{:x}", number.saturating_sub(1)),
            state_root: format!("0x{number:x}"),
            timestamp: 1_700_000_000 + number,
            sequencer_address: ContractAddress::parse("0x1").expect("valid contract address"),
            gas_prices: sample_gas_prices(),
            protocol_version: Version::parse(version).expect("valid version"),
            transactions: Vec::new(),
        }
    }

    fn output(gas: u64) -> ExecutionOutput {
        ExecutionOutput {
            receipts: vec![StarknetReceipt {
                tx_hash: starknet_node_types::TxHash::parse(format!("0x{gas:x}"))
                    .expect("valid tx hash"),
                execution_status: true,
                events: gas,
                gas_consumed: gas,
            }],
            state_diff: StarknetStateDiff::default(),
            builtin_stats: BuiltinStats::default(),
            execution_time: Duration::from_millis(1),
        }
    }

    #[cfg(feature = "blockifier-adapter")]
    fn executable_l1_handler_tx(hash: &str) -> StarknetTransaction {
        use starknet_api::executable_transaction::{
            L1HandlerTransaction as ExecutableL1Handler, Transaction as ExecutableTx,
        };
        use starknet_api::transaction::TransactionHash;
        let mut executable = ExecutableL1Handler::default();
        // L1Handler payload size is calldata_len - 1; keep one slot to avoid underflow.
        executable.tx.calldata = vec![Default::default()].into();
        executable.tx_hash =
            TransactionHash(BlockifierFelt::from_str(hash).expect("valid tx hash"));

        StarknetTransaction::with_executable(hash.to_string(), ExecutableTx::L1Handler(executable))
            .expect("matching executable hash")
    }

    #[cfg(feature = "blockifier-adapter")]
    fn executable_account_invoke_tx(hash: &str) -> StarknetTransaction {
        use starknet_api::executable_transaction::{
            AccountTransaction as ExecutableAccountTransaction,
            InvokeTransaction as ExecutableInvokeTransaction, Transaction as ExecutableTx,
        };
        use starknet_api::transaction::{
            InvokeTransaction as ApiInvokeTransaction, InvokeTransactionV0, TransactionHash,
        };

        let executable = ExecutableInvokeTransaction {
            tx: ApiInvokeTransaction::V0(InvokeTransactionV0::default()),
            tx_hash: TransactionHash(BlockifierFelt::from_str(hash).expect("valid tx hash")),
        };

        StarknetTransaction::with_executable(
            hash.to_string(),
            ExecutableTx::Account(ExecutableAccountTransaction::Invoke(executable)),
        )
        .expect("matching executable hash")
    }

    #[cfg(feature = "blockifier-adapter")]
    struct StaticExecutableResolver;

    #[cfg(feature = "blockifier-adapter")]
    impl ExecutableTransactionResolver for StaticExecutableResolver {
        fn resolve(
            &self,
            _block_number: u64,
            tx: &StarknetTransaction,
        ) -> Result<ExecutableStarknetTransaction, ExecutionError> {
            use starknet_api::executable_transaction::{
                L1HandlerTransaction as ExecutableL1Handler, Transaction as ExecutableTx,
            };
            use starknet_api::transaction::TransactionHash;

            let mut executable = ExecutableL1Handler::default();
            executable.tx.calldata = vec![Default::default()].into();
            executable.tx_hash =
                TransactionHash(BlockifierFelt::from_str(&tx.hash).map_err(|error| {
                    ExecutionError::Backend(format!(
                        "invalid tx hash provided to StaticExecutableResolver: {error}"
                    ))
                })?);
            Ok(ExecutableTx::L1Handler(executable))
        }
    }

    #[cfg(feature = "blockifier-adapter")]
    struct MismatchedHashResolver;

    #[cfg(feature = "blockifier-adapter")]
    impl ExecutableTransactionResolver for MismatchedHashResolver {
        fn resolve(
            &self,
            _block_number: u64,
            _tx: &StarknetTransaction,
        ) -> Result<ExecutableStarknetTransaction, ExecutionError> {
            use starknet_api::executable_transaction::{
                L1HandlerTransaction as ExecutableL1Handler, Transaction as ExecutableTx,
            };

            let mut executable = ExecutableL1Handler::default();
            executable.tx.calldata = vec![Default::default()].into();
            // Keep default tx hash (0x0) to force mismatch against requested tx hash.
            Ok(ExecutableTx::L1Handler(executable))
        }
    }

    #[cfg(feature = "blockifier-adapter")]
    struct MalformedL1Resolver;

    #[cfg(feature = "blockifier-adapter")]
    impl ExecutableTransactionResolver for MalformedL1Resolver {
        fn resolve(
            &self,
            _block_number: u64,
            tx: &StarknetTransaction,
        ) -> Result<ExecutableStarknetTransaction, ExecutionError> {
            use starknet_api::executable_transaction::{
                L1HandlerTransaction as ExecutableL1Handler, Transaction as ExecutableTx,
            };
            use starknet_api::transaction::TransactionHash;

            let executable = ExecutableL1Handler {
                tx_hash: TransactionHash(BlockifierFelt::from_str(&tx.hash).map_err(|error| {
                    ExecutionError::Backend(format!(
                        "invalid tx hash provided to MalformedL1Resolver: {error}"
                    ))
                })?),
                ..Default::default()
            };
            // Keep calldata empty to ensure adapter guards against upstream underflow panic.
            Ok(ExecutableTx::L1Handler(executable))
        }
    }

    #[cfg(feature = "blockifier-adapter")]
    #[derive(Default)]
    struct RecordingClassProvider {
        execution_blocks: Mutex<Vec<u64>>,
        simulation_blocks: Mutex<Vec<u64>>,
    }

    #[cfg(feature = "blockifier-adapter")]
    impl BlockifierClassProvider for RecordingClassProvider {
        fn supports_account_execution(&self) -> bool {
            true
        }

        fn prepare_for_block_execution(&self, block_number: u64) -> Result<(), StateError> {
            self.execution_blocks
                .lock()
                .map_err(|_| StateError::StateReadError("recording mutex poisoned".to_string()))?
                .push(block_number);
            Ok(())
        }

        fn prepare_for_simulation(&self, block_number: u64) -> Result<(), StateError> {
            self.simulation_blocks
                .lock()
                .map_err(|_| StateError::StateReadError("recording mutex poisoned".to_string()))?
                .push(block_number);
            Ok(())
        }

        fn get_class_hash_at(
            &self,
            _contract_address: BlockifierContractAddress,
        ) -> Result<BlockifierClassHash, StateError> {
            Ok(BlockifierClassHash::default())
        }

        fn get_compiled_class(
            &self,
            class_hash: BlockifierClassHash,
        ) -> Result<RunnableCompiledClass, StateError> {
            Err(StateError::UndeclaredClassHash(class_hash))
        }

        fn get_compiled_class_hash(
            &self,
            _class_hash: BlockifierClassHash,
        ) -> Result<BlockifierCompiledClassHash, StateError> {
            Ok(BlockifierCompiledClassHash::default())
        }
    }

    #[cfg(feature = "blockifier-adapter")]
    struct FailingPrepareClassProvider;

    #[cfg(feature = "blockifier-adapter")]
    impl BlockifierClassProvider for FailingPrepareClassProvider {
        fn supports_account_execution(&self) -> bool {
            true
        }

        fn prepare_for_block_execution(&self, _block_number: u64) -> Result<(), StateError> {
            Err(StateError::StateReadError(
                "prepare execution failure".to_string(),
            ))
        }

        fn prepare_for_simulation(&self, _block_number: u64) -> Result<(), StateError> {
            Err(StateError::StateReadError(
                "prepare simulation failure".to_string(),
            ))
        }

        fn get_class_hash_at(
            &self,
            _contract_address: BlockifierContractAddress,
        ) -> Result<BlockifierClassHash, StateError> {
            Ok(BlockifierClassHash::default())
        }

        fn get_compiled_class(
            &self,
            class_hash: BlockifierClassHash,
        ) -> Result<RunnableCompiledClass, StateError> {
            Err(StateError::UndeclaredClassHash(class_hash))
        }

        fn get_compiled_class_hash(
            &self,
            _class_hash: BlockifierClassHash,
        ) -> Result<BlockifierCompiledClassHash, StateError> {
            Ok(BlockifierCompiledClassHash::default())
        }
    }

    #[test]
    fn falls_back_to_canonical_when_fast_backend_missing() {
        let canonical =
            ScriptedBackend::new("canonical", BTreeMap::from([(1, output(5))]), output(9));
        let backend = DualExecutionBackend::new(
            None,
            Box::new(canonical),
            ExecutionMode::DualWithVerification {
                verification_depth: 10,
            },
            MismatchPolicy::WarnAndFallback,
        );

        let mut state = InMemoryState::default();
        let result = backend.execute_verified(&block(1, "0.14.2"), &mut state);
        assert_eq!(result.expect("must fallback").receipts[0].gas_consumed, 5);
    }

    #[test]
    fn mismatch_with_warn_policy_returns_canonical_output() {
        let fast = ScriptedBackend::new("fast", BTreeMap::from([(1, output(1))]), output(1));
        let canonical =
            ScriptedBackend::new("canonical", BTreeMap::from([(1, output(7))]), output(7));
        let backend = DualExecutionBackend::new(
            Some(Box::new(fast)),
            Box::new(canonical),
            ExecutionMode::DualWithVerification {
                verification_depth: 10,
            },
            MismatchPolicy::WarnAndFallback,
        );

        let mut state = InMemoryState::default();
        let result = backend.execute_verified(&block(1, "0.14.2"), &mut state);
        assert_eq!(result.expect("fallback").receipts[0].gas_consumed, 7);
    }

    #[test]
    fn semantic_match_ignores_execution_time_differences() {
        let mut fast_output = output(7);
        let mut canonical_output = output(7);
        fast_output.execution_time = Duration::from_millis(1);
        canonical_output.execution_time = Duration::from_millis(25);
        fast_output.builtin_stats.pedersen = 1;
        canonical_output.builtin_stats.pedersen = 9;

        let fast = ScriptedBackend::new("fast", BTreeMap::from([(1, fast_output)]), output(1));
        let canonical = ScriptedBackend::new(
            "canonical",
            BTreeMap::from([(1, canonical_output)]),
            output(7),
        );
        let backend = DualExecutionBackend::new(
            Some(Box::new(fast)),
            Box::new(canonical),
            ExecutionMode::DualWithVerification {
                verification_depth: 10,
            },
            MismatchPolicy::Halt,
        );

        let mut state = InMemoryState::default();
        let result = backend
            .execute_verified(&block(1, "0.14.2"), &mut state)
            .expect("semantic equality should pass");
        assert_eq!(result.receipts[0].gas_consumed, 7);
    }

    #[test]
    fn verification_depth_one_only_verifies_tip_block() {
        let fast = ScriptedBackend::new(
            "fast",
            BTreeMap::from([(99, output(3)), (100, output(4))]),
            output(1),
        );
        let canonical = ScriptedBackend::new(
            "canonical",
            BTreeMap::from([(99, output(30)), (100, output(40))]),
            output(10),
        );
        let backend = DualExecutionBackend::new(
            Some(Box::new(fast)),
            Box::new(canonical),
            ExecutionMode::DualWithVerification {
                verification_depth: 1,
            },
            MismatchPolicy::WarnAndFallback,
        );
        backend.set_verification_tip(100).expect("set tip");

        let mut state = InMemoryState::default();
        let older = backend
            .execute_verified(&block(99, "0.14.2"), &mut state)
            .expect("older than tip should skip canonical verification");
        let tip = backend
            .execute_verified(&block(100, "0.14.2"), &mut state)
            .expect("tip should be canonically verified");

        assert_eq!(older.receipts[0].gas_consumed, 3);
        assert_eq!(tip.receipts[0].gas_consumed, 40);
    }

    #[test]
    fn verification_depth_zero_skips_canonical_reverification() {
        let fast = ScriptedBackend::new("fast", BTreeMap::from([(1, output(1))]), output(1));
        let canonical =
            ScriptedBackend::new("canonical", BTreeMap::from([(1, output(7))]), output(7));
        let backend = DualExecutionBackend::new(
            Some(Box::new(fast)),
            Box::new(canonical),
            ExecutionMode::DualWithVerification {
                verification_depth: 0,
            },
            MismatchPolicy::WarnAndFallback,
        );

        let mut state = InMemoryState::default();
        let result = backend.execute_verified(&block(1, "0.14.2"), &mut state);
        assert_eq!(result.expect("fast path only").receipts[0].gas_consumed, 1);
    }

    #[test]
    fn verification_depth_applies_to_latest_tip_window() {
        let fast = ScriptedBackend::new(
            "fast",
            BTreeMap::from([(98, output(2)), (99, output(3))]),
            output(1),
        );
        let canonical = ScriptedBackend::new(
            "canonical",
            BTreeMap::from([(98, output(20)), (99, output(30))]),
            output(10),
        );
        let backend = DualExecutionBackend::new(
            Some(Box::new(fast)),
            Box::new(canonical),
            ExecutionMode::DualWithVerification {
                verification_depth: 2,
            },
            MismatchPolicy::WarnAndFallback,
        );
        backend.set_verification_tip(100).expect("set tip");

        let mut state = InMemoryState::default();
        let old_block = backend
            .execute_verified(&block(98, "0.14.2"), &mut state)
            .expect("outside verification window should use fast");
        let recent_block = backend
            .execute_verified(&block(99, "0.14.2"), &mut state)
            .expect("inside verification window should use canonical on mismatch");
        assert_eq!(old_block.receipts[0].gas_consumed, 2);
        assert_eq!(recent_block.receipts[0].gas_consumed, 30);
    }

    #[test]
    fn verification_tip_scope_clears_tip_when_inner_operation_fails() {
        let backend = DualExecutionBackend::new(
            None,
            Box::new(ScriptedBackend::new(
                "canonical",
                BTreeMap::new(),
                output(1),
            )),
            ExecutionMode::CanonicalOnly,
            MismatchPolicy::WarnAndFallback,
        );

        let err: Result<(), ExecutionError> = backend
            .with_verification_tip(123, |_| Err(ExecutionError::Backend("boom".to_string())));
        assert!(matches!(err, Err(ExecutionError::Backend(message)) if message == "boom"));
        assert!(
            backend
                .should_verify_block(1, 1)
                .expect("scope teardown should clear tip"),
            "verification tip must be cleared after scoped failure"
        );
    }

    #[test]
    fn mismatch_with_halt_policy_fails_closed() {
        let fast = ScriptedBackend::new("fast", BTreeMap::from([(1, output(1))]), output(1));
        let canonical =
            ScriptedBackend::new("canonical", BTreeMap::from([(1, output(7))]), output(7));
        let backend = DualExecutionBackend::new(
            Some(Box::new(fast)),
            Box::new(canonical),
            ExecutionMode::DualWithVerification {
                verification_depth: 10,
            },
            MismatchPolicy::Halt,
        );

        let mut state = InMemoryState::default();
        let err = backend
            .execute_verified(&block(1, "0.14.2"), &mut state)
            .expect_err("must halt");
        assert_eq!(err, ExecutionError::Halted);
    }

    #[test]
    fn canonical_state_wins_when_dual_execution_mismatches() {
        let fast = StateWritingBackend {
            gas: 1,
            value: StarknetFelt::from(111_u64),
        };
        let canonical = StateWritingBackend {
            gas: 7,
            value: StarknetFelt::from(999_u64),
        };
        let backend = DualExecutionBackend::new(
            Some(Box::new(fast)),
            Box::new(canonical),
            ExecutionMode::DualWithVerification {
                verification_depth: 10,
            },
            MismatchPolicy::WarnAndFallback,
        );

        let mut state = InMemoryState::default();
        let result = backend
            .execute_verified(&block(1, "0.14.2"), &mut state)
            .expect("fallback");
        assert_eq!(result.receipts[0].gas_consumed, 7);
        assert_eq!(
            state.get_storage(
                &ContractAddress::parse("0x1").expect("valid contract address"),
                "0x2"
            ),
            Ok(Some(StarknetFelt::from(999_u64)))
        );
    }

    #[test]
    fn mismatch_with_cooldown_uses_canonical_for_following_blocks() {
        let fast = ScriptedBackend::new(
            "fast",
            BTreeMap::from([(1, output(1)), (2, output(2)), (3, output(3))]),
            output(3),
        );
        let canonical = ScriptedBackend::new(
            "canonical",
            BTreeMap::from([(1, output(10)), (2, output(20)), (3, output(30))]),
            output(30),
        );
        let backend = DualExecutionBackend::new(
            Some(Box::new(fast)),
            Box::new(canonical),
            ExecutionMode::DualWithVerification {
                verification_depth: 10,
            },
            MismatchPolicy::CooldownThenRetry { cooldown_blocks: 2 },
        );

        let mut state = InMemoryState::default();
        let first = backend
            .execute_verified(&block(1, "0.14.2"), &mut state)
            .expect("first");
        let second = backend
            .execute_verified(&block(2, "0.14.2"), &mut state)
            .expect("second");
        let third = backend
            .execute_verified(&block(3, "0.14.2"), &mut state)
            .expect("third");

        assert_eq!(first.receipts[0].gas_consumed, 10);
        assert_eq!(second.receipts[0].gas_consumed, 20);
        assert_eq!(third.receipts[0].gas_consumed, 30);
    }

    #[test]
    fn dual_verification_clones_state_for_halt_safety() {
        let fast = ScriptedBackend::new(
            "fast",
            BTreeMap::from([(1, output(7)), (2, output(8))]),
            output(1),
        );
        let canonical = ScriptedBackend::new(
            "canonical",
            BTreeMap::from([(1, output(7)), (2, output(8))]),
            output(1),
        );
        let backend = DualExecutionBackend::new(
            Some(Box::new(fast)),
            Box::new(canonical),
            ExecutionMode::DualWithVerification {
                verification_depth: 10,
            },
            MismatchPolicy::WarnAndFallback,
        );
        let clone_count = Arc::new(AtomicUsize::new(0));
        let mut state = CountingCloneState::new(Arc::clone(&clone_count));

        backend
            .execute_verified(&block(1, "0.14.2"), &mut state)
            .expect("first block");
        backend
            .execute_verified(&block(2, "0.14.2"), &mut state)
            .expect("second block");

        // One clone seeds fast shadow; canonical verification clones per block so Halt can fail
        // without mutating live state.
        assert_eq!(clone_count.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn dual_backend_simulate_uses_fast_backend_in_fast_only_mode() {
        let backend = DualExecutionBackend::new(
            Some(Box::new(StateWritingBackend {
                gas: 9,
                value: StarknetFelt::from(1_u64),
            })),
            Box::new(StateWritingBackend {
                gas: 3,
                value: StarknetFelt::from(2_u64),
            }),
            ExecutionMode::FastOnly,
            MismatchPolicy::WarnAndFallback,
        );
        let state = InMemoryState::default();
        let tx = StarknetTransaction::new(
            starknet_node_types::TxHash::parse("0xabc").expect("valid tx hash"),
        );
        let context = BlockContext {
            block_number: 1,
            protocol_version: Version::parse("0.14.2").expect("valid version"),
        };
        let simulation = backend
            .simulate_tx(&tx, &state, &context)
            .expect("fast-only simulation should succeed");
        assert_eq!(simulation.estimated_fee, 9);
    }

    #[test]
    fn dual_backend_simulate_prefers_canonical_in_dual_mode() {
        let backend = DualExecutionBackend::new(
            Some(Box::new(StateWritingBackend {
                gas: 9,
                value: StarknetFelt::from(1_u64),
            })),
            Box::new(StateWritingBackend {
                gas: 3,
                value: StarknetFelt::from(2_u64),
            }),
            ExecutionMode::DualWithVerification {
                verification_depth: 3,
            },
            MismatchPolicy::WarnAndFallback,
        );
        let state = InMemoryState::default();
        let tx = StarknetTransaction::new(
            starknet_node_types::TxHash::parse("0xabc").expect("valid tx hash"),
        );
        let context = BlockContext {
            block_number: 1,
            protocol_version: Version::parse("0.14.2").expect("valid version"),
        };
        let simulation = backend
            .simulate_tx(&tx, &state, &context)
            .expect("dual-mode simulation should succeed");
        assert_eq!(simulation.estimated_fee, 3);
    }

    #[test]
    fn selects_constants_by_protocol_version() {
        let selector = ProtocolVersionSelector::new([
            (
                Version::parse("0.14.0").expect("valid"),
                VersionedConstants {
                    id: "v14_0".to_string(),
                },
            ),
            (
                Version::parse("0.14.2").expect("valid"),
                VersionedConstants {
                    id: "v14_2".to_string(),
                },
            ),
        ]);

        let selected = selector
            .constants_for_block(&block(100, "0.14.2"))
            .expect("exact version");
        assert_eq!(selected.id, "v14_2");
    }

    #[test]
    fn rejects_missing_patch_for_protocol_constants_with_same_minor() {
        let selector = ProtocolVersionSelector::new([
            (
                Version::parse("0.14.0").expect("valid"),
                VersionedConstants {
                    id: "v14_0".to_string(),
                },
            ),
            (
                Version::parse("0.14.2").expect("valid"),
                VersionedConstants {
                    id: "v14_2".to_string(),
                },
            ),
        ]);

        let err = selector
            .constants_for_block(&block(100, "0.14.3"))
            .expect_err("must fail closed on missing patch constants");
        assert_eq!(
            err,
            ExecutionError::MissingConstants(Version::parse("0.14.3").expect("valid"))
        );
    }

    #[test]
    fn rejects_unknown_minor_for_protocol_constants() {
        let selector = ProtocolVersionSelector::new([(
            Version::parse("0.14.0").expect("valid"),
            VersionedConstants {
                id: "v14_0".to_string(),
            },
        )]);

        let err = selector
            .constants_for_block(&block(100, "0.15.0"))
            .expect_err("must fail");
        assert_eq!(
            err,
            ExecutionError::MissingConstants(Version::parse("0.15.0").expect("valid"))
        );
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_protocol_resolver_rejects_missing_patch_version() {
        let resolver = BlockifierProtocolVersionResolver::starknet_mainnet_defaults();
        let err = resolver
            .resolve_for_block(&Version::parse("0.14.3").expect("valid"))
            .expect_err("must fail closed for missing patch");
        assert_eq!(
            err,
            ExecutionError::MissingConstants(Version::parse("0.14.3").expect("valid"))
        );
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_protocol_resolver_supports_four_component_versions() {
        let resolver = BlockifierProtocolVersionResolver::starknet_mainnet_defaults();
        let resolved = resolver
            .resolve_for_block(&Version::parse("0.13.1-1").expect("valid"))
            .expect("resolve explicit 4th component mapping");
        assert_eq!(resolved.to_string(), "0.13.1.1");
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_backend_executes_empty_block_with_real_context() {
        let backend = BlockifierVmBackend::starknet_mainnet();
        let mut state = InMemoryState::default();

        let output = backend
            .execute_block(&empty_block(11, "0.14.2"), &mut state)
            .expect("execute empty block");
        assert!(output.receipts.is_empty());
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_backend_fails_closed_for_unconverted_transactions() {
        let backend = BlockifierVmBackend::starknet_mainnet();
        let mut state = InMemoryState::default();

        let err = backend
            .execute_block(&block(12, "0.14.2"), &mut state)
            .expect_err("must fail");
        match err {
            ExecutionError::Backend(message) => {
                assert!(message.contains("missing executable payload"));
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_backend_accepts_external_transaction_resolver() {
        let backend = BlockifierVmBackend::starknet_mainnet()
            .with_tx_resolver(Arc::new(StaticExecutableResolver));
        let mut state = InMemoryState::default();

        let output = backend
            .execute_block(&block(12, "0.14.2"), &mut state)
            .expect("execute via external resolver");
        assert_eq!(output.receipts.len(), 1);
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_backend_rejects_resolver_hash_mismatches() {
        let backend = BlockifierVmBackend::starknet_mainnet()
            .with_tx_resolver(Arc::new(MismatchedHashResolver));
        let mut state = InMemoryState::default();

        let err = backend
            .execute_block(&block(12, "0.14.2"), &mut state)
            .expect_err("must reject mismatched hashes");
        match err {
            ExecutionError::Backend(message) => {
                assert!(message.contains("resolver tx hash mismatch"));
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_backend_rejects_malformed_l1_handler_payloads() {
        let backend =
            BlockifierVmBackend::starknet_mainnet().with_tx_resolver(Arc::new(MalformedL1Resolver));
        let mut state = InMemoryState::default();

        let err = backend
            .execute_block(&block(12, "0.14.2"), &mut state)
            .expect_err("must reject malformed l1 handler payload");
        match err {
            ExecutionError::Backend(message) => {
                assert!(message.contains("invalid L1 handler tx"));
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_backend_rejects_account_txs_without_class_provider_integration() {
        let backend = BlockifierVmBackend::starknet_mainnet();
        let mut state = InMemoryState::default();
        let block = StarknetBlock {
            number: 12,
            parent_hash: "0xb".to_string(),
            state_root: "0xc".to_string(),
            timestamp: 1_700_000_012,
            sequencer_address: ContractAddress::parse("0x1").expect("valid contract address"),
            gas_prices: sample_gas_prices(),
            protocol_version: Version::parse("0.14.2").expect("valid version"),
            transactions: vec![executable_account_invoke_tx("0x123")],
        };

        let err = backend
            .execute_block(&block, &mut state)
            .expect_err("must fail for account tx without class provider");
        match err {
            ExecutionError::Backend(message) => {
                assert!(message.contains("class-provider integration is required"));
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_backend_prepares_class_provider_for_execution_and_simulation() {
        let provider = Arc::new(RecordingClassProvider::default());
        let backend = BlockifierVmBackend::starknet_mainnet()
            .with_class_provider(Arc::clone(&provider) as Arc<dyn BlockifierClassProvider>);
        let mut state = InMemoryState::default();
        let tx = executable_l1_handler_tx("0xabc");

        backend
            .execute_block(&empty_block(11, "0.14.2"), &mut state)
            .expect("execute empty block");
        backend
            .simulate_tx(&tx, &state, &context(12, "0.14.2"))
            .expect("simulate tx");

        let execution = provider
            .execution_blocks
            .lock()
            .expect("execution blocks lock")
            .clone();
        let simulation = provider
            .simulation_blocks
            .lock()
            .expect("simulation blocks lock")
            .clone();
        assert_eq!(execution, vec![11]);
        assert_eq!(simulation, vec![12]);
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_backend_fails_closed_when_class_provider_prepare_fails() {
        let backend = BlockifierVmBackend::starknet_mainnet()
            .with_class_provider(Arc::new(FailingPrepareClassProvider));
        let mut state = InMemoryState::default();

        let exec_err = backend
            .execute_block(&empty_block(11, "0.14.2"), &mut state)
            .expect_err("prepare failure must abort execution");
        match exec_err {
            ExecutionError::Backend(message) => {
                assert!(message.contains("class provider preparation failed for block execution"));
            }
            other => panic!("unexpected error variant: {other:?}"),
        }

        let sim_err = backend
            .simulate_tx(
                &executable_l1_handler_tx("0xabc"),
                &state,
                &context(12, "0.14.2"),
            )
            .expect_err("prepare failure must abort simulation");
        match sim_err {
            ExecutionError::Backend(message) => {
                assert!(message.contains("class provider preparation failed for simulation"));
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_backend_processes_non_empty_blocks_with_executable_payloads() {
        let backend = BlockifierVmBackend::starknet_mainnet();
        let mut state = InMemoryState::default();
        let block = StarknetBlock {
            number: 12,
            parent_hash: "0xb".to_string(),
            state_root: "0xc".to_string(),
            timestamp: 1_700_000_012,
            sequencer_address: ContractAddress::parse("0x1").expect("valid contract address"),
            gas_prices: sample_gas_prices(),
            protocol_version: Version::parse("0.14.2").expect("valid version"),
            transactions: vec![executable_l1_handler_tx("0xabc")],
        };

        let output = backend
            .execute_block(&block, &mut state)
            .expect("execute non-empty block");
        assert_eq!(output.receipts.len(), 1);
        assert_eq!(
            output.receipts[0].tx_hash,
            starknet_node_types::TxHash::parse("0xabc").expect("valid tx hash")
        );
        assert!(!output.receipts[0].execution_status);
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_backend_reuses_state_snapshot_between_blocks() {
        let backend = BlockifierVmBackend::starknet_mainnet();
        let clone_count = Arc::new(AtomicUsize::new(0));
        let mut state = CountingCloneState::new(Arc::clone(&clone_count));

        backend
            .execute_block(&empty_block(11, "0.14.2"), &mut state)
            .expect("first block");
        backend
            .execute_block(&empty_block(12, "0.14.2"), &mut state)
            .expect("second block");

        assert_eq!(clone_count.load(Ordering::SeqCst), 1);
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_backend_simulate_tx_executes_via_blockifier_context() {
        let backend = BlockifierVmBackend::starknet_mainnet();
        let state = InMemoryState::default();
        let tx = executable_l1_handler_tx("0xabc");

        let simulation = backend
            .simulate_tx(&tx, &state, &context(12, "0.14.2"))
            .expect("simulate tx");
        assert_eq!(
            simulation.receipt.tx_hash,
            starknet_node_types::TxHash::parse("0xabc").expect("valid tx hash")
        );
        assert_eq!(
            simulation.estimated_fee,
            u128::from(simulation.receipt.gas_consumed)
        );
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_backend_simulate_tx_fails_closed_on_missing_patch_constants() {
        let backend = BlockifierVmBackend::starknet_mainnet();
        let state = InMemoryState::default();
        let tx = executable_l1_handler_tx("0xabc");

        let err = backend
            .simulate_tx(&tx, &state, &context(12, "0.14.3"))
            .expect_err("must fail closed on missing patch constants");
        assert_eq!(
            err,
            ExecutionError::MissingConstants(Version::parse("0.14.3").expect("valid version"))
        );
    }

    #[cfg(feature = "blockifier-adapter")]
    #[test]
    fn blockifier_backend_simulate_tx_fails_closed_on_unknown_protocol_line() {
        let backend = BlockifierVmBackend::starknet_mainnet();
        let state = InMemoryState::default();
        let tx = executable_l1_handler_tx("0xabc");

        let err = backend
            .simulate_tx(&tx, &state, &context(12, "0.15.0"))
            .expect_err("must fail closed on unknown major.minor protocol line");
        assert_eq!(
            err,
            ExecutionError::MissingConstants(Version::parse("0.15.0").expect("valid version"))
        );
    }
}
