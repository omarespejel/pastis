#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use node_spec_core::exex_delivery::ExExHandleMeta;
use node_spec_core::notification::StarknetExExNotification;
use starknet_node_exex_manager::{
    ExExCredentials, ExExManager, ExExRegistration, NotificationSink, SinkTrust,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtelMetricsSnapshot {
    pub notifications_total: u64,
    pub v1_notifications_total: u64,
    pub v2_notifications_total: u64,
    pub tx_total: u64,
    pub event_total: u64,
    pub block_regressions_total: u64,
    pub last_block_number: Option<u64>,
}

#[derive(Debug, Default)]
struct OtelState {
    notifications_total: AtomicU64,
    v1_notifications_total: AtomicU64,
    v2_notifications_total: AtomicU64,
    tx_total: AtomicU64,
    event_total: AtomicU64,
    block_regressions_total: AtomicU64,
    last_block_number: Mutex<Option<u64>>,
}

#[derive(Clone, Debug, Default)]
pub struct OtelNotificationSink {
    state: Arc<OtelState>,
}

impl OtelNotificationSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> Result<OtelMetricsSnapshot, String> {
        let last_block_number = self
            .state
            .last_block_number
            .lock()
            .map_err(|_| "otel sink state lock poisoned".to_string())?
            .to_owned();
        Ok(OtelMetricsSnapshot {
            notifications_total: self.state.notifications_total.load(Ordering::Relaxed),
            v1_notifications_total: self.state.v1_notifications_total.load(Ordering::Relaxed),
            v2_notifications_total: self.state.v2_notifications_total.load(Ordering::Relaxed),
            tx_total: self.state.tx_total.load(Ordering::Relaxed),
            event_total: self.state.event_total.load(Ordering::Relaxed),
            block_regressions_total: self.state.block_regressions_total.load(Ordering::Relaxed),
            last_block_number,
        })
    }

    pub fn into_registration(
        self,
        name: impl Into<String>,
        depends_on: &[&str],
        priority: u32,
    ) -> ExExRegistration {
        ExExRegistration {
            meta: ExExHandleMeta {
                name: name.into(),
                depends_on: depends_on.iter().map(|dep| dep.to_string()).collect(),
                priority,
            },
            trust: SinkTrust::TrustedInProcess,
            sink: Box::new(self),
        }
    }
}

impl NotificationSink for OtelNotificationSink {
    fn on_notification(
        &mut self,
        notification: Arc<StarknetExExNotification>,
    ) -> Result<(), String> {
        let (block_number, tx_count, event_count, is_v2) = match notification.as_ref() {
            StarknetExExNotification::V1(v1) => (v1.block_number, v1.tx_count, 0, false),
            StarknetExExNotification::V2(v2) => {
                (v2.block_number, v2.tx_count, v2.event_count, true)
            }
        };

        self.state
            .notifications_total
            .fetch_add(1, Ordering::Relaxed);
        self.state.tx_total.fetch_add(tx_count, Ordering::Relaxed);
        self.state
            .event_total
            .fetch_add(event_count, Ordering::Relaxed);
        if is_v2 {
            self.state
                .v2_notifications_total
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.state
                .v1_notifications_total
                .fetch_add(1, Ordering::Relaxed);
        }

        let mut last_block = self
            .state
            .last_block_number
            .lock()
            .map_err(|_| "otel sink state lock poisoned".to_string())?;
        if let Some(previous) = *last_block
            && block_number <= previous
        {
            self.state
                .block_regressions_total
                .fetch_add(1, Ordering::Relaxed);
        }
        *last_block = Some(block_number);
        Ok(())
    }
}

pub fn register_otel_sink(
    manager: &mut ExExManager,
    sink: OtelNotificationSink,
    registration_name: &str,
    registration_token: &str,
    depends_on: &[&str],
    priority: u32,
) -> Result<(), String> {
    let registration = sink.into_registration(registration_name, depends_on, priority);
    let credentials = ExExCredentials {
        exex_tokens: HashMap::from([(
            registration_name.to_string(),
            registration_token.to_string(),
        )]),
    };
    manager
        .register(vec![registration], &credentials)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use node_spec_core::notification::{NotificationV1, NotificationV2, StarknetExExNotification};

    use super::*;

    #[test]
    fn records_v1_and_v2_notification_totals() {
        let mut sink = OtelNotificationSink::new();
        sink.on_notification(Arc::new(StarknetExExNotification::V1(NotificationV1 {
            block_number: 10,
            tx_count: 3,
        })))
        .expect("v1");
        sink.on_notification(Arc::new(StarknetExExNotification::V2(NotificationV2 {
            block_number: 11,
            tx_count: 7,
            event_count: 9,
        })))
        .expect("v2");

        let snapshot = sink.snapshot().expect("snapshot");
        assert_eq!(snapshot.notifications_total, 2);
        assert_eq!(snapshot.v1_notifications_total, 1);
        assert_eq!(snapshot.v2_notifications_total, 1);
        assert_eq!(snapshot.tx_total, 10);
        assert_eq!(snapshot.event_total, 9);
        assert_eq!(snapshot.block_regressions_total, 0);
        assert_eq!(snapshot.last_block_number, Some(11));
    }

    #[test]
    fn records_block_regression_when_block_number_moves_backwards() {
        let mut sink = OtelNotificationSink::new();
        sink.on_notification(Arc::new(StarknetExExNotification::V2(NotificationV2 {
            block_number: 40,
            tx_count: 1,
            event_count: 2,
        })))
        .expect("first");
        sink.on_notification(Arc::new(StarknetExExNotification::V2(NotificationV2 {
            block_number: 39,
            tx_count: 1,
            event_count: 2,
        })))
        .expect("second");

        let snapshot = sink.snapshot().expect("snapshot");
        assert_eq!(snapshot.block_regressions_total, 1);
        assert_eq!(snapshot.last_block_number, Some(39));
    }

    #[test]
    fn integrates_with_exex_manager_delivery() {
        let mut manager = ExExManager::new(8, [("otel".to_string(), "token".to_string())]);
        let sink = OtelNotificationSink::new();
        let reader = sink.clone();
        register_otel_sink(&mut manager, sink, "otel", "token", &[], 100).expect("register");

        manager
            .enqueue(StarknetExExNotification::V2(NotificationV2 {
                block_number: 1,
                tx_count: 5,
                event_count: 8,
            }))
            .expect("enqueue 1");
        manager
            .enqueue(StarknetExExNotification::V1(NotificationV1 {
                block_number: 2,
                tx_count: 4,
            }))
            .expect("enqueue 2");

        let drained_1 = manager.drain_one().expect("drain").expect("id");
        let drained_2 = manager.drain_one().expect("drain").expect("id");
        assert_eq!(drained_1, 1);
        assert_eq!(drained_2, 2);

        let snapshot = reader.snapshot().expect("snapshot");
        assert_eq!(snapshot.notifications_total, 2);
        assert_eq!(snapshot.v1_notifications_total, 1);
        assert_eq!(snapshot.v2_notifications_total, 1);
        assert_eq!(snapshot.tx_total, 9);
        assert_eq!(snapshot.event_total, 8);
        assert_eq!(snapshot.last_block_number, Some(2));
        assert_eq!(snapshot.block_regressions_total, 0);
    }

    #[test]
    fn registration_helper_rejects_invalid_token() {
        let mut manager = ExExManager::new(8, [("otel".to_string(), "token".to_string())]);
        let sink = OtelNotificationSink::new();
        let err = register_otel_sink(&mut manager, sink, "otel", "wrong", &[], 100)
            .expect_err("must fail");
        assert!(err.contains("registration authentication failed"));
    }
}
