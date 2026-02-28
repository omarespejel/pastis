use bincode::Options;
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

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum NotificationDecodeError {
    #[error("wal entry size {size} exceeds max {max}")]
    EntryTooLarge { size: usize, max: usize },
    #[error("failed to decode WAL entry: {0}")]
    Decode(String),
    #[error("decoded notification field '{field}' value {value} exceeds max {max}")]
    ContentLimitExceeded {
        field: &'static str,
        value: u64,
        max: u64,
    },
}

const LEGACY_V1_ENTRY_SIZE_BYTES: usize = core::mem::size_of::<u64>() * 2;
const MAX_NOTIFICATION_ENTRY_BYTES: usize = 1024 * 1024;
const MAX_NOTIFICATION_TX_COUNT: u64 = 1_000_000;
const MAX_NOTIFICATION_EVENT_COUNT: u64 = 10_000_000;

fn wal_bincode_options() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
}

fn validate_notification_content(
    notification: &StarknetExExNotification,
) -> Result<(), NotificationDecodeError> {
    match notification {
        StarknetExExNotification::V1(v1) => {
            if v1.tx_count > MAX_NOTIFICATION_TX_COUNT {
                return Err(NotificationDecodeError::ContentLimitExceeded {
                    field: "tx_count",
                    value: v1.tx_count,
                    max: MAX_NOTIFICATION_TX_COUNT,
                });
            }
        }
        StarknetExExNotification::V2(v2) => {
            if v2.tx_count > MAX_NOTIFICATION_TX_COUNT {
                return Err(NotificationDecodeError::ContentLimitExceeded {
                    field: "tx_count",
                    value: v2.tx_count,
                    max: MAX_NOTIFICATION_TX_COUNT,
                });
            }
            if v2.event_count > MAX_NOTIFICATION_EVENT_COUNT {
                return Err(NotificationDecodeError::ContentLimitExceeded {
                    field: "event_count",
                    value: v2.event_count,
                    max: MAX_NOTIFICATION_EVENT_COUNT,
                });
            }
        }
    }
    Ok(())
}

pub fn decode_wal_entry(bytes: &[u8]) -> Result<StarknetExExNotification, NotificationDecodeError> {
    if bytes.len() > MAX_NOTIFICATION_ENTRY_BYTES {
        return Err(NotificationDecodeError::EntryTooLarge {
            size: bytes.len(),
            max: MAX_NOTIFICATION_ENTRY_BYTES,
        });
    }

    match wal_bincode_options().deserialize::<StarknetExExNotification>(bytes) {
        Ok(notification) => {
            validate_notification_content(&notification)?;
            Ok(notification)
        }
        Err(enum_error) if bytes.len() == LEGACY_V1_ENTRY_SIZE_BYTES => {
            let notification = wal_bincode_options()
                .deserialize::<NotificationV1>(bytes)
                .map(StarknetExExNotification::V1)
                .map_err(|legacy_error| {
                    NotificationDecodeError::Decode(format!(
                        "enum decode error: {enum_error}; legacy decode error: {legacy_error}"
                    ))
                })?;
            validate_notification_content(&notification)?;
            Ok(notification)
        }
        Err(enum_error) => Err(NotificationDecodeError::Decode(format!(
            "enum decode error: {enum_error}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_versioned_v2_entry() {
        let encoded = wal_bincode_options()
            .serialize(&StarknetExExNotification::V2(NotificationV2 {
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
        let legacy_bytes = wal_bincode_options()
            .serialize(&NotificationV1 {
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

    #[test]
    fn rejects_v1_fallback_for_non_legacy_payload_lengths() {
        let mut legacy_plus_tail = wal_bincode_options()
            .serialize(&NotificationV1 {
                block_number: 9,
                tx_count: 3,
            })
            .expect("serialize v1");
        legacy_plus_tail.push(0xAA);

        let err = decode_wal_entry(&legacy_plus_tail).expect_err("must fail");
        assert!(matches!(err, NotificationDecodeError::Decode(_)));
    }

    #[test]
    fn rejects_oversized_wal_entry_before_deserialization() {
        let bytes = vec![0u8; MAX_NOTIFICATION_ENTRY_BYTES + 1];
        let err = decode_wal_entry(&bytes).expect_err("must fail");
        assert_eq!(
            err,
            NotificationDecodeError::EntryTooLarge {
                size: MAX_NOTIFICATION_ENTRY_BYTES + 1,
                max: MAX_NOTIFICATION_ENTRY_BYTES,
            }
        );
    }

    #[test]
    fn rejects_decoded_entries_that_exceed_content_limits() {
        let encoded = wal_bincode_options()
            .serialize(&StarknetExExNotification::V2(NotificationV2 {
                block_number: 42,
                tx_count: MAX_NOTIFICATION_TX_COUNT + 1,
                event_count: 9,
            }))
            .expect("serialize v2");

        let err = decode_wal_entry(&encoded).expect_err("must fail");
        assert_eq!(
            err,
            NotificationDecodeError::ContentLimitExceeded {
                field: "tx_count",
                value: MAX_NOTIFICATION_TX_COUNT + 1,
                max: MAX_NOTIFICATION_TX_COUNT,
            }
        );
    }
}
