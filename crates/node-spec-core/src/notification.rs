use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationV1 {
    pub block_number: u64,
    pub tx_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationV2 {
    pub block_number: u64,
    pub tx_count: u64,
    pub event_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StarknetExExNotification {
    V1(NotificationV1),
    V2(NotificationV2),
}

#[derive(Debug, thiserror::Error)]
pub enum NotificationDecodeError {
    #[error("failed to decode WAL entry: {0}")]
    Decode(String),
}

pub fn decode_wal_entry(bytes: &[u8]) -> Result<StarknetExExNotification, NotificationDecodeError> {
    match bincode::deserialize::<StarknetExExNotification>(bytes) {
        Ok(notification) => Ok(notification),
        Err(enum_error) => bincode::deserialize::<NotificationV1>(bytes)
            .map(StarknetExExNotification::V1)
            .map_err(|legacy_error| {
                NotificationDecodeError::Decode(format!(
                    "enum decode error: {enum_error}; legacy decode error: {legacy_error}"
                ))
            }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_versioned_v2_entry() {
        let encoded = bincode::serialize(&StarknetExExNotification::V2(NotificationV2 {
            block_number: 42,
            tx_count: 6,
            event_count: 9,
        }))
        .expect("serialize v2");

        let decoded = decode_wal_entry(&encoded).expect("decode v2");
        assert_eq!(
            decoded,
            StarknetExExNotification::V2(NotificationV2 {
                block_number: 42,
                tx_count: 6,
                event_count: 9,
            })
        );
    }

    #[test]
    fn decodes_legacy_v1_payload_without_enum_wrapper() {
        let legacy_bytes = bincode::serialize(&NotificationV1 {
            block_number: 9,
            tx_count: 3,
        })
        .expect("serialize v1");

        let decoded = decode_wal_entry(&legacy_bytes).expect("decode legacy");
        assert_eq!(
            decoded,
            StarknetExExNotification::V1(NotificationV1 {
                block_number: 9,
                tx_count: 3,
            })
        );
    }
}
