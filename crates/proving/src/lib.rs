#![forbid(unsafe_code)]

use starknet_node_execution::CasmTrace;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StarkProof {
    pub block_number: u64,
    pub proof_bytes: Vec<u8>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProvingError {
    #[error("trace provider failed: {0}")]
    TraceProvider(String),
    #[error("prover failed: {0}")]
    Prover(String),
    #[error("traces unavailable for block {block_number}")]
    TraceUnavailable { block_number: u64 },
    #[error("trace set for block {block_number} is empty")]
    EmptyTraceSet { block_number: u64 },
    #[error("proof is invalid: {0}")]
    InvalidProof(String),
}

pub trait ProvingBackend: Send + Sync {
    fn verify_proof(&self, proof: &StarkProof) -> Result<bool, ProvingError>;
}

#[derive(Debug, Default)]
pub struct StwoVerifyOnly;

impl StwoVerifyOnly {
    pub fn new() -> Self {
        Self
    }
}

impl ProvingBackend for StwoVerifyOnly {
    fn verify_proof(&self, proof: &StarkProof) -> Result<bool, ProvingError> {
        if proof.proof_bytes.is_empty() {
            return Err(ProvingError::InvalidProof(
                "empty proof payload is not accepted".to_string(),
            ));
        }
        Ok(true)
    }
}

pub trait TraceProvider: Send + Sync {
    fn traces_for_block(&self, block_number: u64) -> Result<Option<Vec<CasmTrace>>, ProvingError>;
}

pub trait TraceProver: Send + Sync {
    fn prove(&self, block_number: u64, traces: &[CasmTrace]) -> Result<StarkProof, ProvingError>;
}

pub struct ProvingPipeline<T, P> {
    trace_provider: T,
    prover: P,
}

impl<T, P> ProvingPipeline<T, P> {
    pub fn new(trace_provider: T, prover: P) -> Self {
        Self {
            trace_provider,
            prover,
        }
    }
}

impl<T, P> ProvingPipeline<T, P>
where
    T: TraceProvider,
    P: TraceProver,
{
    pub fn prove_block(&self, block_number: u64) -> Result<StarkProof, ProvingError> {
        let traces = self
            .trace_provider
            .traces_for_block(block_number)?
            .ok_or(ProvingError::TraceUnavailable { block_number })?;
        if traces.is_empty() {
            return Err(ProvingError::EmptyTraceSet { block_number });
        }
        self.prover.prove(block_number, &traces)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use super::*;

    struct FixedTraceProvider {
        traces_by_block: BTreeMap<u64, Vec<CasmTrace>>,
    }

    impl TraceProvider for FixedTraceProvider {
        fn traces_for_block(
            &self,
            block_number: u64,
        ) -> Result<Option<Vec<CasmTrace>>, ProvingError> {
            Ok(self.traces_by_block.get(&block_number).cloned())
        }
    }

    #[derive(Default)]
    struct RecordingProver {
        calls: Mutex<Vec<(u64, usize)>>,
    }

    impl TraceProver for RecordingProver {
        fn prove(
            &self,
            block_number: u64,
            traces: &[CasmTrace],
        ) -> Result<StarkProof, ProvingError> {
            self.calls
                .lock()
                .expect("calls mutex should not be poisoned")
                .push((block_number, traces.len()));
            Ok(StarkProof {
                block_number,
                proof_bytes: vec![0xAB, 0xCD],
            })
        }
    }

    #[test]
    fn verify_only_rejects_empty_proof_payload() {
        let verifier = StwoVerifyOnly::new();
        let err = verifier
            .verify_proof(&StarkProof {
                block_number: 7,
                proof_bytes: Vec::new(),
            })
            .expect_err("empty proofs must fail");
        assert_eq!(
            err,
            ProvingError::InvalidProof("empty proof payload is not accepted".to_string())
        );
    }

    #[test]
    fn verify_only_accepts_non_empty_proof_payload() {
        let verifier = StwoVerifyOnly::new();
        let valid = verifier
            .verify_proof(&StarkProof {
                block_number: 7,
                proof_bytes: vec![1, 2, 3],
            })
            .expect("non-empty payload is accepted");
        assert!(valid);
    }

    #[test]
    fn pipeline_fails_when_traces_are_missing() {
        let provider = FixedTraceProvider {
            traces_by_block: BTreeMap::new(),
        };
        let prover = RecordingProver::default();
        let pipeline = ProvingPipeline::new(provider, prover);

        let err = pipeline
            .prove_block(42)
            .expect_err("missing traces must fail");
        assert_eq!(err, ProvingError::TraceUnavailable { block_number: 42 });
        let calls = pipeline
            .prover
            .calls
            .lock()
            .expect("calls mutex should not be poisoned")
            .clone();
        assert!(
            calls.is_empty(),
            "prover must not be called when traces are missing"
        );
    }

    #[test]
    fn pipeline_fails_when_trace_set_is_empty() {
        let provider = FixedTraceProvider {
            traces_by_block: BTreeMap::from([(42, Vec::new())]),
        };
        let prover = RecordingProver::default();
        let pipeline = ProvingPipeline::new(provider, prover);

        let err = pipeline
            .prove_block(42)
            .expect_err("empty trace set must fail");
        assert_eq!(err, ProvingError::EmptyTraceSet { block_number: 42 });
        let calls = pipeline
            .prover
            .calls
            .lock()
            .expect("calls mutex should not be poisoned")
            .clone();
        assert!(
            calls.is_empty(),
            "prover must not be called when traces are empty"
        );
    }

    #[test]
    fn pipeline_proves_block_with_available_traces() {
        let provider = FixedTraceProvider {
            traces_by_block: BTreeMap::from([(
                42,
                vec![CasmTrace { steps: 10 }, CasmTrace { steps: 8 }],
            )]),
        };
        let prover = RecordingProver::default();
        let pipeline = ProvingPipeline::new(provider, prover);

        let proof = pipeline.prove_block(42).expect("proof generation succeeds");
        assert_eq!(proof.block_number, 42);
        assert_eq!(proof.proof_bytes, vec![0xAB, 0xCD]);

        let calls = pipeline
            .prover
            .calls
            .lock()
            .expect("calls mutex should not be poisoned")
            .clone();
        assert_eq!(calls, vec![(42, 2)]);
    }
}
