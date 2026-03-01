#![forbid(unsafe_code)]

use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use starknet_node_storage::{StorageBackend, StorageError};
use starknet_node_types::{
    BlockId, ContractAddress, StarknetBlock, StarknetFelt, StarknetStateDiff, TxHash,
};

const JSONRPC_VERSION: &str = "2.0";
const ERR_PARSE: i64 = -32700;
const ERR_INVALID_REQUEST: i64 = -32600;
const ERR_METHOD_NOT_FOUND: i64 = -32601;
const ERR_INVALID_PARAMS: i64 = -32602;
const ERR_INTERNAL: i64 = -32603;
const ERR_BLOCK_NOT_FOUND: i64 = -32001;
const ERR_TX_NOT_FOUND: i64 = -32003;
const MAX_BATCH_REQUESTS: usize = 256;
const INTERNAL_SERIALIZATION_ERROR_RESPONSE: &str = r#"{"jsonrpc":"2.0","error":{"code":-32603,"message":"internal serialization error"},"id":null}"#;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncStatus {
    pub starting_block_num: u64,
    pub current_block_num: u64,
    pub highest_block_num: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    #[serde(default)]
    pub id: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonRpcErrorObject {
    pub code: i64,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcErrorObject>,
    pub id: Value,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RpcError {
    #[error("invalid JSON-RPC request: {0}")]
    InvalidRequest(String),
    #[error("unknown method: {0}")]
    MethodNotFound(String),
    #[error("invalid params: {0}")]
    InvalidParams(String),
    #[error("requested block was not found")]
    BlockNotFound,
    #[error("requested transaction was not found")]
    TxNotFound,
    #[error("state read failed: {0}")]
    StateRead(String),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

pub struct StarknetRpcServer<'a> {
    storage: &'a dyn StorageBackend,
    chain_id: String,
    sync_status: Option<SyncStatus>,
}

impl<'a> StarknetRpcServer<'a> {
    pub fn new(storage: &'a dyn StorageBackend, chain_id: impl Into<String>) -> Self {
        Self {
            storage,
            chain_id: chain_id.into(),
            sync_status: None,
        }
    }

    pub fn with_sync_status(mut self, sync_status: SyncStatus) -> Self {
        self.sync_status = Some(sync_status);
        self
    }

    pub fn handle_raw(&self, raw: &str) -> String {
        match serde_json::from_str::<Value>(raw) {
            Ok(value) => self.handle_value(value),
            Err(error) => {
                let response = JsonRpcResponse {
                    jsonrpc: JSONRPC_VERSION.to_string(),
                    result: None,
                    error: Some(JsonRpcErrorObject {
                        code: ERR_PARSE,
                        message: format!("parse error: {error}"),
                    }),
                    id: Value::Null,
                };
                serialize_json_or_internal_error(&response)
            }
        }
    }

    fn handle_value(&self, value: Value) -> String {
        match value {
            Value::Array(items) => {
                if items.is_empty() {
                    return serialize_response(error_response(
                        Value::Null,
                        ERR_INVALID_REQUEST,
                        "empty batch request",
                    ));
                }
                if items.len() > MAX_BATCH_REQUESTS {
                    return serialize_response(error_response(
                        Value::Null,
                        ERR_INVALID_REQUEST,
                        format!(
                            "batch request too large: max {MAX_BATCH_REQUESTS}, got {}",
                            items.len()
                        ),
                    ));
                }
                let mut responses = Vec::with_capacity(items.len());
                for item in items {
                    match serde_json::from_value::<JsonRpcRequest>(item) {
                        Ok(request) => {
                            if let Some(response) = self.handle_request(request) {
                                responses.push(response);
                            }
                        }
                        Err(error) => responses.push(error_response(
                            Value::Null,
                            ERR_INVALID_REQUEST,
                            format!("invalid request object: {error}"),
                        )),
                    };
                }
                if responses.is_empty() {
                    return String::new();
                }
                serialize_json_or_internal_error(&responses)
            }
            other => {
                let response = match serde_json::from_value::<JsonRpcRequest>(other) {
                    Ok(request) => self.handle_request(request),
                    Err(error) => Some(error_response(
                        Value::Null,
                        ERR_INVALID_REQUEST,
                        format!("invalid request object: {error}"),
                    )),
                };
                match response {
                    Some(response) => serialize_response(response),
                    None => String::new(),
                }
            }
        }
    }

    pub fn handle_request(&self, request: JsonRpcRequest) -> Option<JsonRpcResponse> {
        let is_notification = request.id.is_none();
        let request_id = request.id.unwrap_or(Value::Null);
        if request.jsonrpc != JSONRPC_VERSION {
            if is_notification {
                return None;
            }
            return Some(error_response(
                request_id,
                ERR_INVALID_REQUEST,
                format!(
                    "unsupported jsonrpc version '{}', expected '{}'",
                    request.jsonrpc, JSONRPC_VERSION
                ),
            ));
        }

        match self.execute(request.method.as_str(), &request.params) {
            Ok(result) => {
                if is_notification {
                    return None;
                }
                Some(JsonRpcResponse {
                    jsonrpc: JSONRPC_VERSION.to_string(),
                    result: Some(result),
                    error: None,
                    id: request_id,
                })
            }
            Err(error) => {
                if is_notification {
                    return None;
                }
                Some(map_error_to_response(request_id, error))
            }
        }
    }

    fn execute(&self, method: &str, params: &Value) -> Result<Value, RpcError> {
        match method {
            "starknet_blockNumber" => self.block_number(params),
            "starknet_chainId" => self.chain_id(params),
            "starknet_getBlockWithTxHashes" => self.get_block_with_tx_hashes(params),
            "starknet_getBlockWithTxs" => self.get_block_with_txs(params),
            "starknet_getBlockTransactionCount" => self.get_block_transaction_count(params),
            "starknet_getTransactionByBlockIdAndIndex" => {
                self.get_transaction_by_block_id_and_index(params)
            }
            "starknet_getStateUpdate" => self.get_state_update(params),
            "starknet_getTransactionByHash" => self.get_transaction_by_hash(params),
            "starknet_getNonce" => self.get_nonce(params),
            "starknet_getStorageAt" => self.get_storage_at(params),
            "starknet_syncing" => self.syncing(params),
            _ => Err(RpcError::MethodNotFound(method.to_string())),
        }
    }

    fn block_number(&self, params: &Value) -> Result<Value, RpcError> {
        ensure_no_params(params)?;
        let number = self.storage.latest_block_number()?;
        Ok(json!(number))
    }

    fn chain_id(&self, params: &Value) -> Result<Value, RpcError> {
        ensure_no_params(params)?;
        Ok(json!(self.chain_id.as_str()))
    }

    fn get_block_with_txs(&self, params: &Value) -> Result<Value, RpcError> {
        let block_id = parse_block_id_param(params)?;
        let block = self
            .storage
            .get_block(block_id)?
            .ok_or(RpcError::BlockNotFound)?;
        Ok(block_to_json(&block))
    }

    fn get_block_with_tx_hashes(&self, params: &Value) -> Result<Value, RpcError> {
        let block_id = parse_block_id_param(params)?;
        let block = self
            .storage
            .get_block(block_id)?
            .ok_or(RpcError::BlockNotFound)?;
        Ok(block_with_hashes_to_json(&block))
    }

    fn get_block_transaction_count(&self, params: &Value) -> Result<Value, RpcError> {
        let block_id = parse_block_id_param(params)?;
        let block = self
            .storage
            .get_block(block_id)?
            .ok_or(RpcError::BlockNotFound)?;
        Ok(json!(block.transactions.len() as u64))
    }

    fn get_state_update(&self, params: &Value) -> Result<Value, RpcError> {
        let block_id = parse_block_id_param(params)?;
        let block = self
            .storage
            .get_block(block_id.clone())?
            .ok_or(RpcError::BlockNotFound)?;
        let state_diff = self
            .storage
            .get_state_diff(block.number)?
            .ok_or(RpcError::BlockNotFound)?;
        let old_root = if block.number <= 1 {
            "0x0".to_string()
        } else {
            self.storage
                .get_block(BlockId::Number(block.number.saturating_sub(1)))?
                .map(|parent| parent.state_root)
                .unwrap_or_else(|| "0x0".to_string())
        };
        Ok(json!({
            "block_hash": format!("0x{:x}", block.number),
            "new_root": block.state_root,
            "old_root": old_root,
            "state_diff": state_diff_to_json(&state_diff),
        }))
    }

    fn get_transaction_by_hash(&self, params: &Value) -> Result<Value, RpcError> {
        let requested_hash = parse_tx_hash_param(params)?;
        let (block_number, tx_index, tx) = self
            .storage
            .get_transaction_by_hash(&requested_hash)?
            .ok_or(RpcError::TxNotFound)?;
        Ok(json!({
            "transaction_hash": tx.hash,
            "block_number": block_number,
            "transaction_index": tx_index as u64,
            "type": "INVOKE",
        }))
    }

    fn get_transaction_by_block_id_and_index(&self, params: &Value) -> Result<Value, RpcError> {
        let (block_id, tx_index) = parse_block_id_and_index_params(params)?;
        let block = self
            .storage
            .get_block(block_id)?
            .ok_or(RpcError::BlockNotFound)?;
        let tx = block
            .transactions
            .get(tx_index)
            .ok_or(RpcError::TxNotFound)?;
        Ok(json!({
            "transaction_hash": tx.hash,
            "block_number": block.number,
            "transaction_index": tx_index as u64,
            "type": "INVOKE",
        }))
    }

    fn get_nonce(&self, params: &Value) -> Result<Value, RpcError> {
        let (block_id, contract_address) = parse_block_id_and_contract_params(params)?;
        let block_number = self.resolve_block_number(block_id)?;
        let state_reader = self.storage.get_state_reader(block_number)?;
        let nonce = state_reader
            .nonce_of(&contract_address)
            .map_err(|error| RpcError::StateRead(error.to_string()))?
            .unwrap_or_else(|| StarknetFelt::from(0_u8));
        Ok(json!(format!("{:#x}", nonce)))
    }

    fn get_storage_at(&self, params: &Value) -> Result<Value, RpcError> {
        let (contract_address, storage_key, block_id) = parse_storage_at_params(params)?;
        let block_number = self.resolve_block_number(block_id)?;
        let state_reader = self.storage.get_state_reader(block_number)?;
        let value = state_reader
            .get_storage(&contract_address, &storage_key)
            .map_err(|error| RpcError::StateRead(error.to_string()))?
            .unwrap_or_else(|| StarknetFelt::from(0_u8));
        Ok(json!(format!("{:#x}", value)))
    }

    fn syncing(&self, params: &Value) -> Result<Value, RpcError> {
        ensure_no_params(params)?;
        let Some(status) = &self.sync_status else {
            return Ok(json!(false));
        };
        if status.current_block_num >= status.highest_block_num {
            return Ok(json!(false));
        }
        Ok(json!({
            "starting_block_num": status.starting_block_num,
            "current_block_num": status.current_block_num,
            "highest_block_num": status.highest_block_num,
        }))
    }

    fn resolve_block_number(&self, block_id: BlockId) -> Result<u64, RpcError> {
        match block_id {
            BlockId::Latest => self.storage.latest_block_number().map_err(RpcError::from),
            BlockId::Number(number) => {
                let exists = self.storage.get_block(BlockId::Number(number))?.is_some();
                if !exists {
                    return Err(RpcError::BlockNotFound);
                }
                Ok(number)
            }
        }
    }
}

fn ensure_no_params(params: &Value) -> Result<(), RpcError> {
    match params {
        Value::Null => Ok(()),
        Value::Array(values) if values.is_empty() => Ok(()),
        Value::Object(map) if map.is_empty() => Ok(()),
        _ => Err(RpcError::InvalidParams(
            "this method does not accept params".to_string(),
        )),
    }
}

fn parse_block_id_param(params: &Value) -> Result<BlockId, RpcError> {
    let block_id_value = match params {
        Value::Array(values) if values.len() == 1 => &values[0],
        Value::Array(values) => {
            return Err(RpcError::InvalidParams(format!(
                "expected exactly one positional param, got {}",
                values.len()
            )));
        }
        Value::Object(map) => map.get("block_id").ok_or_else(|| {
            RpcError::InvalidParams("missing required key 'block_id'".to_string())
        })?,
        Value::Null => {
            return Err(RpcError::InvalidParams(
                "missing required block_id param".to_string(),
            ));
        }
        _ => {
            return Err(RpcError::InvalidParams(
                "params must be array or object".to_string(),
            ));
        }
    };
    parse_block_id_value(block_id_value)
}

fn parse_block_id_value(block_id_value: &Value) -> Result<BlockId, RpcError> {
    match block_id_value {
        Value::String(s) if s == "latest" => Ok(BlockId::Latest),
        Value::Object(obj) => {
            if let Some(number) = obj.get("block_number").and_then(Value::as_u64) {
                return Ok(BlockId::Number(number));
            }
            if let Some(tag) = obj.get("block_tag").and_then(Value::as_str)
                && tag == "latest"
            {
                return Ok(BlockId::Latest);
            }
            Err(RpcError::InvalidParams(
                "block_id object must include block_number or block_tag='latest'".to_string(),
            ))
        }
        _ => Err(RpcError::InvalidParams(
            "block_id must be 'latest' or an object with block_number/block_tag".to_string(),
        )),
    }
}

fn parse_block_id_and_index_params(params: &Value) -> Result<(BlockId, usize), RpcError> {
    let (block_id_value, index_value) = match params {
        Value::Array(values) if values.len() == 2 => (&values[0], &values[1]),
        Value::Array(values) => {
            return Err(RpcError::InvalidParams(format!(
                "expected exactly two positional params, got {}",
                values.len()
            )));
        }
        Value::Object(map) => (
            map.get("block_id").ok_or_else(|| {
                RpcError::InvalidParams("missing required key 'block_id'".to_string())
            })?,
            map.get("index").ok_or_else(|| {
                RpcError::InvalidParams("missing required key 'index'".to_string())
            })?,
        ),
        Value::Null => {
            return Err(RpcError::InvalidParams(
                "missing required block_id and index params".to_string(),
            ));
        }
        _ => {
            return Err(RpcError::InvalidParams(
                "params must be array or object".to_string(),
            ));
        }
    };
    let block_id = parse_block_id_value(block_id_value)?;
    let index_u64 = index_value
        .as_u64()
        .ok_or_else(|| RpcError::InvalidParams("index must be an unsigned integer".to_string()))?;
    let index = usize::try_from(index_u64)
        .map_err(|_| RpcError::InvalidParams("index is too large for this platform".to_string()))?;
    Ok((block_id, index))
}

fn parse_block_id_and_contract_params(
    params: &Value,
) -> Result<(BlockId, ContractAddress), RpcError> {
    let (block_id_value, contract_value) = match params {
        Value::Array(values) if values.len() == 2 => (&values[0], &values[1]),
        Value::Array(values) => {
            return Err(RpcError::InvalidParams(format!(
                "expected exactly two positional params, got {}",
                values.len()
            )));
        }
        Value::Object(map) => (
            map.get("block_id").ok_or_else(|| {
                RpcError::InvalidParams("missing required key 'block_id'".to_string())
            })?,
            map.get("contract_address").ok_or_else(|| {
                RpcError::InvalidParams("missing required key 'contract_address'".to_string())
            })?,
        ),
        Value::Null => {
            return Err(RpcError::InvalidParams(
                "missing required block_id and contract_address params".to_string(),
            ));
        }
        _ => {
            return Err(RpcError::InvalidParams(
                "params must be array or object".to_string(),
            ));
        }
    };
    let block_id = parse_block_id_value(block_id_value)?;
    let contract_address = parse_contract_address(contract_value, "contract_address")?;
    Ok((block_id, contract_address))
}

fn parse_storage_at_params(params: &Value) -> Result<(ContractAddress, String, BlockId), RpcError> {
    let (contract_value, key_value, block_id_value) = match params {
        Value::Array(values) if values.len() == 3 => (&values[0], &values[1], &values[2]),
        Value::Array(values) => {
            return Err(RpcError::InvalidParams(format!(
                "expected exactly three positional params, got {}",
                values.len()
            )));
        }
        Value::Object(map) => (
            map.get("contract_address").ok_or_else(|| {
                RpcError::InvalidParams("missing required key 'contract_address'".to_string())
            })?,
            map.get("key")
                .ok_or_else(|| RpcError::InvalidParams("missing required key 'key'".to_string()))?,
            map.get("block_id").ok_or_else(|| {
                RpcError::InvalidParams("missing required key 'block_id'".to_string())
            })?,
        ),
        Value::Null => {
            return Err(RpcError::InvalidParams(
                "missing required contract_address, key and block_id params".to_string(),
            ));
        }
        _ => {
            return Err(RpcError::InvalidParams(
                "params must be array or object".to_string(),
            ));
        }
    };
    let contract_address = parse_contract_address(contract_value, "contract_address")?;
    let key = parse_felt_hex(key_value, "key")?;
    let block_id = parse_block_id_value(block_id_value)?;
    Ok((contract_address, key, block_id))
}

fn parse_contract_address(value: &Value, field: &str) -> Result<ContractAddress, RpcError> {
    let raw = value
        .as_str()
        .ok_or_else(|| RpcError::InvalidParams(format!("{field} must be a string")))?;
    ContractAddress::parse(raw)
        .map_err(|error| RpcError::InvalidParams(format!("invalid {field} '{raw}': {error}")))
}

fn parse_felt_hex(value: &Value, field: &str) -> Result<String, RpcError> {
    let raw = value
        .as_str()
        .ok_or_else(|| RpcError::InvalidParams(format!("{field} must be a string")))?;
    StarknetFelt::from_str(raw)
        .map(|felt| format!("{:#x}", felt))
        .map_err(|error| RpcError::InvalidParams(format!("invalid {field} '{raw}': {error}")))
}

fn parse_tx_hash_param(params: &Value) -> Result<TxHash, RpcError> {
    let tx_hash_value = match params {
        Value::Array(values) if values.len() == 1 => &values[0],
        Value::Array(values) => {
            return Err(RpcError::InvalidParams(format!(
                "expected exactly one positional param, got {}",
                values.len()
            )));
        }
        Value::Object(map) => map.get("transaction_hash").ok_or_else(|| {
            RpcError::InvalidParams("missing required key 'transaction_hash'".to_string())
        })?,
        Value::Null => {
            return Err(RpcError::InvalidParams(
                "missing required transaction_hash param".to_string(),
            ));
        }
        _ => {
            return Err(RpcError::InvalidParams(
                "params must be array or object".to_string(),
            ));
        }
    };

    let raw_hash = tx_hash_value
        .as_str()
        .ok_or_else(|| RpcError::InvalidParams("transaction_hash must be a string".to_string()))?;
    TxHash::parse(raw_hash).map_err(|error| {
        RpcError::InvalidParams(format!("invalid transaction_hash '{raw_hash}': {error}"))
    })
}

fn block_to_json(block: &StarknetBlock) -> Value {
    json!({
        "number": block.number,
        "parent_hash": block.parent_hash,
        "state_root": block.state_root,
        "timestamp": block.timestamp,
        "sequencer_address": block.sequencer_address,
        "protocol_version": block.protocol_version.to_string(),
        "transactions": block.transactions.iter().map(|tx| json!({
            "hash": tx.hash,
        })).collect::<Vec<_>>(),
    })
}

fn block_with_hashes_to_json(block: &StarknetBlock) -> Value {
    json!({
        "number": block.number,
        "parent_hash": block.parent_hash,
        "state_root": block.state_root,
        "timestamp": block.timestamp,
        "sequencer_address": block.sequencer_address,
        "protocol_version": block.protocol_version.to_string(),
        "transactions": block.transactions.iter().map(|tx| tx.hash.as_ref()).collect::<Vec<_>>(),
    })
}

fn state_diff_to_json(diff: &StarknetStateDiff) -> Value {
    json!({
        "storage_diffs": diff.storage_diffs.iter().map(|(contract, writes)| {
            json!({
                "address": contract.as_ref(),
                "storage_entries": writes.iter().map(|(key, value)| json!({
                    "key": key,
                    "value": format!("{:#x}", value),
                })).collect::<Vec<_>>(),
            })
        }).collect::<Vec<_>>(),
        "nonces": diff.nonces.iter().map(|(contract, nonce)| json!({
            "contract_address": contract.as_ref(),
            "nonce": format!("{:#x}", nonce),
        })).collect::<Vec<_>>(),
        "declared_classes": diff.declared_classes.iter().map(|class_hash| json!({
            "class_hash": class_hash.as_ref(),
        })).collect::<Vec<_>>(),
    })
}

fn map_error_to_response(id: Value, error: RpcError) -> JsonRpcResponse {
    match error {
        RpcError::InvalidRequest(message) => error_response(id, ERR_INVALID_REQUEST, message),
        RpcError::MethodNotFound(method) => error_response(
            id,
            ERR_METHOD_NOT_FOUND,
            format!("method '{method}' is not supported"),
        ),
        RpcError::InvalidParams(message) => error_response(id, ERR_INVALID_PARAMS, message),
        RpcError::BlockNotFound => error_response(id, ERR_BLOCK_NOT_FOUND, "block not found"),
        RpcError::TxNotFound => error_response(id, ERR_TX_NOT_FOUND, "transaction not found"),
        RpcError::StateRead(message) => {
            error_response(id, ERR_INTERNAL, format!("state reader error: {message}"))
        }
        RpcError::Storage(error) => {
            error_response(id, ERR_INTERNAL, format!("storage backend error: {error}"))
        }
    }
}

fn error_response(id: Value, code: i64, message: impl Into<String>) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: JSONRPC_VERSION.to_string(),
        result: None,
        error: Some(JsonRpcErrorObject {
            code,
            message: message.into(),
        }),
        id,
    }
}

fn serialize_response(response: JsonRpcResponse) -> String {
    serialize_json_or_internal_error(&response)
}

fn serialize_json_or_internal_error<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|_| INTERNAL_SERIALIZATION_ERROR_RESPONSE.to_string())
}

#[cfg(test)]
mod tests {
    use semver::Version;
    use starknet_node_storage::InMemoryStorage;
    use starknet_node_types::{
        BlockGasPrices, ContractAddress, GasPricePerToken, InMemoryState, StarknetBlock,
        StarknetFelt, StarknetStateDiff, StarknetTransaction,
    };

    use super::*;

    fn sample_block(number: u64) -> StarknetBlock {
        StarknetBlock {
            number,
            parent_hash: if number == 0 {
                "0x0".to_string()
            } else {
                format!("0x{:x}", number - 1)
            },
            state_root: format!("0x{:x}", number + 100),
            timestamp: 1_700_000_000 + number,
            sequencer_address: ContractAddress::parse("0x1").expect("valid contract address"),
            gas_prices: BlockGasPrices {
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
            },
            protocol_version: Version::parse("0.14.2").expect("version"),
            transactions: vec![StarknetTransaction::new(
                starknet_node_types::TxHash::parse(format!("0x{:x}", number + 500))
                    .expect("valid tx hash"),
            )],
        }
    }

    fn seeded_server() -> StarknetRpcServer<'static> {
        let mut storage = InMemoryStorage::new(InMemoryState::default());
        let mut diff_1 = StarknetStateDiff::default();
        let contract_1 = ContractAddress::parse("0x1").expect("valid contract");
        diff_1
            .storage_diffs
            .entry(contract_1.clone())
            .or_default()
            .insert("0x10".to_string(), StarknetFelt::from(10_u64));
        diff_1.nonces.insert(contract_1, StarknetFelt::from(1_u64));
        storage
            .insert_block(sample_block(1), diff_1)
            .expect("insert");
        let mut diff_2 = StarknetStateDiff::default();
        let contract_2 = ContractAddress::parse("0x2").expect("valid contract");
        diff_2
            .storage_diffs
            .entry(contract_2.clone())
            .or_default()
            .insert("0x20".to_string(), StarknetFelt::from(20_u64));
        diff_2.nonces.insert(contract_2, StarknetFelt::from(2_u64));
        storage
            .insert_block(sample_block(2), diff_2)
            .expect("insert");
        let leaked: &'static mut InMemoryStorage = Box::leak(Box::new(storage));
        StarknetRpcServer::new(leaked, "SN_MAIN")
    }

    #[test]
    fn block_number_works() {
        let server = seeded_server();
        let raw = r#"{"jsonrpc":"2.0","id":1,"method":"starknet_blockNumber","params":[]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["result"], json!(2));
        assert_eq!(value["id"], json!(1));
    }

    #[test]
    fn chain_id_works() {
        let server = seeded_server();
        let raw = r#"{"jsonrpc":"2.0","id":"x","method":"starknet_chainId","params":[]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["result"], json!("SN_MAIN"));
    }

    #[test]
    fn get_block_with_txs_latest_works() {
        let server = seeded_server();
        let raw =
            r#"{"jsonrpc":"2.0","id":9,"method":"starknet_getBlockWithTxs","params":["latest"]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["result"]["number"], json!(2));
        assert_eq!(value["result"]["transactions"][0]["hash"], json!("0x1f6"));
    }

    #[test]
    fn get_block_with_txs_number_works() {
        let server = seeded_server();
        let raw = r#"{"jsonrpc":"2.0","id":2,"method":"starknet_getBlockWithTxs","params":[{"block_number":1}]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["result"]["number"], json!(1));
    }

    #[test]
    fn get_block_transaction_count_works() {
        let server = seeded_server();
        let raw = r#"{"jsonrpc":"2.0","id":7,"method":"starknet_getBlockTransactionCount","params":[{"block_number":2}]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["result"], json!(1));
    }

    #[test]
    fn get_state_update_works() {
        let server = seeded_server();
        let raw = r#"{"jsonrpc":"2.0","id":8,"method":"starknet_getStateUpdate","params":[{"block_number":2}]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["result"]["new_root"], json!("0x66"));
        assert_eq!(value["result"]["old_root"], json!("0x65"));
        assert_eq!(
            value["result"]["state_diff"]["storage_diffs"][0]["address"],
            json!("0x2")
        );
    }

    #[test]
    fn get_transaction_by_hash_works() {
        let server = seeded_server();
        let raw = r#"{"jsonrpc":"2.0","id":10,"method":"starknet_getTransactionByHash","params":["0x1f6"]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["result"]["transaction_hash"], json!("0x1f6"));
        assert_eq!(value["result"]["block_number"], json!(2));
        assert_eq!(value["result"]["transaction_index"], json!(0));
    }

    #[test]
    fn get_transaction_by_hash_returns_not_found() {
        let server = seeded_server();
        let raw = r#"{"jsonrpc":"2.0","id":11,"method":"starknet_getTransactionByHash","params":["0xdead"]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["error"]["code"], json!(ERR_TX_NOT_FOUND));
    }

    #[test]
    fn get_transaction_by_block_id_and_index_works() {
        let server = seeded_server();
        let raw = r#"{"jsonrpc":"2.0","id":14,"method":"starknet_getTransactionByBlockIdAndIndex","params":[{"block_number":2},0]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["result"]["transaction_hash"], json!("0x1f6"));
        assert_eq!(value["result"]["block_number"], json!(2));
        assert_eq!(value["result"]["transaction_index"], json!(0));
    }

    #[test]
    fn get_block_with_tx_hashes_works() {
        let server = seeded_server();
        let raw = r#"{"jsonrpc":"2.0","id":15,"method":"starknet_getBlockWithTxHashes","params":[{"block_number":2}]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["result"]["number"], json!(2));
        assert_eq!(value["result"]["transactions"], json!(["0x1f6"]));
    }

    #[test]
    fn get_nonce_works() {
        let server = seeded_server();
        let raw = r#"{"jsonrpc":"2.0","id":16,"method":"starknet_getNonce","params":[{"block_number":2},"0x2"]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["result"], json!("0x2"));
    }

    #[test]
    fn get_nonce_returns_zero_for_missing_contract() {
        let server = seeded_server();
        let raw = r#"{"jsonrpc":"2.0","id":17,"method":"starknet_getNonce","params":[{"block_number":2},"0x999"]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["result"], json!("0x0"));
    }

    #[test]
    fn get_storage_at_works() {
        let server = seeded_server();
        let raw = r#"{"jsonrpc":"2.0","id":18,"method":"starknet_getStorageAt","params":["0x2","0x20",{"block_number":2}]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["result"], json!("0x14"));
    }

    #[test]
    fn get_storage_at_returns_zero_for_missing_slot() {
        let server = seeded_server();
        let raw = r#"{"jsonrpc":"2.0","id":19,"method":"starknet_getStorageAt","params":["0x2","0xdead",{"block_number":2}]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["result"], json!("0x0"));
    }

    #[test]
    fn syncing_returns_false_without_status() {
        let server = seeded_server();
        let raw = r#"{"jsonrpc":"2.0","id":12,"method":"starknet_syncing","params":[]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["result"], json!(false));
    }

    #[test]
    fn syncing_returns_object_when_behind() {
        let server = seeded_server().with_sync_status(SyncStatus {
            starting_block_num: 10,
            current_block_num: 12,
            highest_block_num: 20,
        });
        let raw = r#"{"jsonrpc":"2.0","id":13,"method":"starknet_syncing","params":[]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["result"]["starting_block_num"], json!(10));
        assert_eq!(value["result"]["current_block_num"], json!(12));
        assert_eq!(value["result"]["highest_block_num"], json!(20));
    }

    #[test]
    fn get_block_with_txs_returns_not_found() {
        let server = seeded_server();
        let raw = r#"{"jsonrpc":"2.0","id":2,"method":"starknet_getBlockWithTxs","params":[{"block_number":99}]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["error"]["code"], json!(ERR_BLOCK_NOT_FOUND));
    }

    #[test]
    fn method_not_found_returns_standard_error() {
        let server = seeded_server();
        let raw = r#"{"jsonrpc":"2.0","id":3,"method":"starknet_unknown","params":[]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["error"]["code"], json!(ERR_METHOD_NOT_FOUND));
    }

    #[test]
    fn invalid_params_are_rejected() {
        let server = seeded_server();
        let raw = r#"{"jsonrpc":"2.0","id":4,"method":"starknet_getBlockWithTxs","params":[]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["error"]["code"], json!(ERR_INVALID_PARAMS));
    }

    #[test]
    fn invalid_jsonrpc_version_is_rejected() {
        let server = seeded_server();
        let raw = r#"{"jsonrpc":"1.0","id":5,"method":"starknet_blockNumber","params":[]}"#;
        let value: Value = serde_json::from_str(&server.handle_raw(raw)).expect("response json");
        assert_eq!(value["error"]["code"], json!(ERR_INVALID_REQUEST));
    }

    #[test]
    fn parse_error_is_reported() {
        let server = seeded_server();
        let value: Value = serde_json::from_str(&server.handle_raw("{not-json"))
            .expect("parse error response json");
        assert_eq!(value["error"]["code"], json!(ERR_PARSE));
        assert_eq!(value["id"], Value::Null);
    }

    #[test]
    fn batch_requests_return_ordered_responses() {
        let server = seeded_server();
        let raw = r#"[
          {"jsonrpc":"2.0","id":1,"method":"starknet_blockNumber","params":[]},
          {"jsonrpc":"2.0","id":2,"method":"starknet_chainId","params":[]}
        ]"#;
        let values: Value = serde_json::from_str(&server.handle_raw(raw)).expect("batch json");
        assert!(values.is_array());
        let arr = values.as_array().expect("array");
        assert_eq!(arr[0]["id"], json!(1));
        assert_eq!(arr[0]["result"], json!(2));
        assert_eq!(arr[1]["id"], json!(2));
        assert_eq!(arr[1]["result"], json!("SN_MAIN"));
    }

    #[test]
    fn oversized_batch_is_rejected() {
        let server = seeded_server();
        let mut batch = Vec::new();
        for id in 0..(MAX_BATCH_REQUESTS + 1) {
            batch.push(json!({
                "jsonrpc": "2.0",
                "id": id as u64,
                "method": "starknet_blockNumber",
                "params": [],
            }));
        }
        let raw = serde_json::to_string(&batch).expect("serialize oversized batch");
        let value: Value = serde_json::from_str(&server.handle_raw(&raw)).expect("response json");
        assert_eq!(value["error"]["code"], json!(ERR_INVALID_REQUEST));
        assert!(
            value["error"]["message"]
                .as_str()
                .expect("error message")
                .contains("batch request too large")
        );
    }

    #[test]
    fn notifications_do_not_emit_responses() {
        let server = seeded_server();
        let raw = r#"{"jsonrpc":"2.0","method":"starknet_blockNumber","params":[]}"#;
        let response = server.handle_raw(raw);
        assert!(response.is_empty());
    }

    #[test]
    fn notification_batch_with_only_notifications_returns_empty_payload() {
        let server = seeded_server();
        let raw = r#"[
          {"jsonrpc":"2.0","method":"starknet_blockNumber","params":[]},
          {"jsonrpc":"2.0","method":"starknet_chainId","params":[]}
        ]"#;
        let response = server.handle_raw(raw);
        assert!(response.is_empty());
    }

    #[test]
    fn mixed_batch_responds_only_to_requests_with_ids() {
        let server = seeded_server();
        let raw = r#"[
          {"jsonrpc":"2.0","method":"starknet_blockNumber","params":[]},
          {"jsonrpc":"2.0","id":9,"method":"starknet_chainId","params":[]}
        ]"#;
        let values: Value = serde_json::from_str(&server.handle_raw(raw)).expect("batch json");
        let arr = values.as_array().expect("array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["id"], json!(9));
        assert_eq!(arr[0]["result"], json!("SN_MAIN"));
    }

    struct FailingSerialize;

    impl serde::Serialize for FailingSerialize {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("intentional failure"))
        }
    }

    #[test]
    fn serializer_fallback_returns_internal_error_payload() {
        let raw = serialize_json_or_internal_error(&FailingSerialize);
        let value: Value = serde_json::from_str(&raw).expect("valid fallback JSON");
        assert_eq!(value["error"]["code"], json!(ERR_INTERNAL));
        assert_eq!(
            value["error"]["message"],
            json!("internal serialization error")
        );
    }
}
