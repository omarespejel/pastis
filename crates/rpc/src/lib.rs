#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use starknet_node_storage::{StorageBackend, StorageError};
use starknet_node_types::{BlockId, StarknetBlock};

const JSONRPC_VERSION: &str = "2.0";
const ERR_PARSE: i64 = -32700;
const ERR_INVALID_REQUEST: i64 = -32600;
const ERR_METHOD_NOT_FOUND: i64 = -32601;
const ERR_INVALID_PARAMS: i64 = -32602;
const ERR_INTERNAL: i64 = -32603;
const ERR_BLOCK_NOT_FOUND: i64 = -32001;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    #[serde(default)]
    pub id: Value,
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
    #[error(transparent)]
    Storage(#[from] StorageError),
}

pub struct StarknetRpcServer<'a> {
    storage: &'a dyn StorageBackend,
    chain_id: String,
}

impl<'a> StarknetRpcServer<'a> {
    pub fn new(storage: &'a dyn StorageBackend, chain_id: impl Into<String>) -> Self {
        Self {
            storage,
            chain_id: chain_id.into(),
        }
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
                serde_json::to_string(&response).expect("serialize parse error response")
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
                let mut responses = Vec::with_capacity(items.len());
                for item in items {
                    let response = match serde_json::from_value::<JsonRpcRequest>(item) {
                        Ok(request) => self.handle_request(request),
                        Err(error) => error_response(
                            Value::Null,
                            ERR_INVALID_REQUEST,
                            format!("invalid request object: {error}"),
                        ),
                    };
                    responses.push(response);
                }
                serde_json::to_string(&responses).expect("serialize batch response")
            }
            other => {
                let response = match serde_json::from_value::<JsonRpcRequest>(other) {
                    Ok(request) => self.handle_request(request),
                    Err(error) => error_response(
                        Value::Null,
                        ERR_INVALID_REQUEST,
                        format!("invalid request object: {error}"),
                    ),
                };
                serialize_response(response)
            }
        }
    }

    pub fn handle_request(&self, request: JsonRpcRequest) -> JsonRpcResponse {
        if request.jsonrpc != JSONRPC_VERSION {
            return error_response(
                request.id,
                ERR_INVALID_REQUEST,
                format!(
                    "unsupported jsonrpc version '{}', expected '{}'",
                    request.jsonrpc, JSONRPC_VERSION
                ),
            );
        }

        match self.execute(request.method.as_str(), &request.params) {
            Ok(result) => JsonRpcResponse {
                jsonrpc: JSONRPC_VERSION.to_string(),
                result: Some(result),
                error: None,
                id: request.id,
            },
            Err(error) => map_error_to_response(request.id, error),
        }
    }

    fn execute(&self, method: &str, params: &Value) -> Result<Value, RpcError> {
        match method {
            "starknet_blockNumber" => self.block_number(params),
            "starknet_chainId" => self.chain_id(params),
            "starknet_getBlockWithTxs" => self.get_block_with_txs(params),
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
    serde_json::to_string(&response).expect("serialize json-rpc response")
}

#[cfg(test)]
mod tests {
    use semver::Version;
    use starknet_node_storage::InMemoryStorage;
    use starknet_node_types::{
        BlockGasPrices, ContractAddress, GasPricePerToken, InMemoryState, StarknetBlock,
        StarknetStateDiff, StarknetTransaction,
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
            sequencer_address: ContractAddress::from("0x1"),
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
            transactions: vec![StarknetTransaction::new(format!("0x{:x}", number + 500))],
        }
    }

    fn seeded_server() -> StarknetRpcServer<'static> {
        let mut storage = InMemoryStorage::new(InMemoryState::default());
        storage
            .insert_block(sample_block(1), StarknetStateDiff::default())
            .expect("insert");
        storage
            .insert_block(sample_block(2), StarknetStateDiff::default())
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
}
