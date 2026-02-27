use std::collections::{HashMap, VecDeque};

use node_spec_core::exex_delivery::{DeliveryPlanError, ExExHandleMeta, build_delivery_tiers};
use node_spec_core::notification::{
    NotificationDecodeError, NotificationV1, StarknetExExNotification, decode_wal_entry,
};

pub trait NotificationSink: Send {
    fn on_notification(&mut self, notification: StarknetExExNotification) -> Result<(), String>;
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryWal {
    entries: Vec<Vec<u8>>,
}

impl InMemoryWal {
    pub fn append(&mut self, entry: Vec<u8>) {
        self.entries.push(entry);
    }

    pub fn append_raw_legacy_v1(&mut self, v1: NotificationV1) {
        let encoded = bincode::serialize(&v1).expect("serialize v1");
        self.entries.push(encoded);
    }

    pub fn entries(&self) -> &[Vec<u8>] {
        &self.entries
    }
}

pub struct ExExRegistration {
    pub meta: ExExHandleMeta,
    pub sink: Box<dyn NotificationSink>,
}

#[derive(Debug, thiserror::Error)]
pub enum ManagerError {
    #[error("registration failed: {0}")]
    Registration(#[from] DeliveryPlanError),
    #[error("notification buffer is full")]
    CapacityExceeded,
    #[error("missing sink for exex '{0}'")]
    MissingSink(String),
    #[error("sink '{name}' failed: {message}")]
    SinkFailure { name: String, message: String },
    #[error("failed to encode notification to WAL: {0}")]
    WalEncode(String),
    #[error("failed to decode notification from WAL: {0}")]
    WalDecode(#[from] NotificationDecodeError),
}

pub struct ExExManager {
    tiers: Vec<Vec<String>>,
    sinks: HashMap<String, Box<dyn NotificationSink>>,
    buffer: VecDeque<(u64, StarknetExExNotification)>,
    next_id: u64,
    max_capacity: usize,
    wal: InMemoryWal,
}

impl ExExManager {
    pub fn new(max_capacity: usize) -> Self {
        Self {
            tiers: Vec::new(),
            sinks: HashMap::new(),
            buffer: VecDeque::new(),
            next_id: 1,
            max_capacity,
            wal: InMemoryWal::default(),
        }
    }

    pub fn wal(&self) -> &InMemoryWal {
        &self.wal
    }

    pub fn wal_mut(&mut self) -> &mut InMemoryWal {
        &mut self.wal
    }

    pub fn register(&mut self, registrations: Vec<ExExRegistration>) -> Result<(), ManagerError> {
        let metas: Vec<ExExHandleMeta> = registrations.iter().map(|r| r.meta.clone()).collect();
        self.tiers = build_delivery_tiers(&metas)?;
        self.sinks = registrations
            .into_iter()
            .map(|registration| (registration.meta.name, registration.sink))
            .collect();
        Ok(())
    }

    pub fn has_capacity(&self) -> bool {
        self.buffer.len() < self.max_capacity
    }

    pub fn enqueue(&mut self, notification: StarknetExExNotification) -> Result<u64, ManagerError> {
        if !self.has_capacity() {
            return Err(ManagerError::CapacityExceeded);
        }

        let encoded = bincode::serialize(&notification).map_err(|error| {
            ManagerError::WalEncode(format!("bincode serialize failed: {error}"))
        })?;
        self.wal.append(encoded);

        let notification_id = self.next_id;
        self.next_id += 1;
        self.buffer.push_back((notification_id, notification));
        Ok(notification_id)
    }

    pub fn drain_one(&mut self) -> Result<Option<u64>, ManagerError> {
        let Some((notification_id, notification)) = self.buffer.pop_front() else {
            return Ok(None);
        };

        if self.tiers.is_empty() {
            let mut names: Vec<String> = self.sinks.keys().cloned().collect();
            names.sort();
            for name in names {
                let sink = self
                    .sinks
                    .get_mut(&name)
                    .ok_or_else(|| ManagerError::MissingSink(name.clone()))?;
                sink.on_notification(notification.clone())
                    .map_err(|message| ManagerError::SinkFailure {
                        name: name.clone(),
                        message,
                    })?;
            }
            return Ok(Some(notification_id));
        }

        for tier in &self.tiers {
            for name in tier {
                let sink = self
                    .sinks
                    .get_mut(name)
                    .ok_or_else(|| ManagerError::MissingSink(name.clone()))?;
                sink.on_notification(notification.clone())
                    .map_err(|message| ManagerError::SinkFailure {
                        name: name.clone(),
                        message,
                    })?;
            }
        }

        Ok(Some(notification_id))
    }

    pub fn replay_wal(&self) -> Result<Vec<StarknetExExNotification>, ManagerError> {
        let mut notifications = Vec::with_capacity(self.wal.entries().len());
        for raw in self.wal.entries() {
            notifications.push(decode_wal_entry(raw)?);
        }
        Ok(notifications)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use node_spec_core::notification::{NotificationV2, StarknetExExNotification};

    use super::*;

    struct RecordingSink {
        name: String,
        order_log: Arc<Mutex<Vec<String>>>,
    }

    impl RecordingSink {
        fn new(name: &str, order_log: Arc<Mutex<Vec<String>>>) -> Self {
            Self {
                name: name.to_string(),
                order_log,
            }
        }
    }

    impl NotificationSink for RecordingSink {
        fn on_notification(
            &mut self,
            _notification: StarknetExExNotification,
        ) -> Result<(), String> {
            self.order_log
                .lock()
                .expect("lock order log")
                .push(self.name.clone());
            Ok(())
        }
    }

    fn reg(name: &str, deps: &[&str], log: Arc<Mutex<Vec<String>>>) -> ExExRegistration {
        ExExRegistration {
            meta: ExExHandleMeta {
                name: name.to_string(),
                depends_on: deps.iter().map(|d| d.to_string()).collect(),
                priority: 1,
            },
            sink: Box::new(RecordingSink::new(name, log)),
        }
    }

    fn sample_notification() -> StarknetExExNotification {
        StarknetExExNotification::V2(NotificationV2 {
            block_number: 7,
            tx_count: 3,
            event_count: 5,
        })
    }

    #[test]
    fn rejects_dependency_cycles_at_registration() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut manager = ExExManager::new(16);
        let err = manager
            .register(vec![
                reg("a", &["b"], log.clone()),
                reg("b", &["a"], log.clone()),
            ])
            .expect_err("must fail");

        assert!(matches!(
            err,
            ManagerError::Registration(DeliveryPlanError::DependencyCycle)
        ));
    }

    #[test]
    fn enforces_notification_capacity() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut manager = ExExManager::new(1);
        manager
            .register(vec![reg("otel", &[], log.clone())])
            .expect("register");

        manager.enqueue(sample_notification()).expect("first");
        let err = manager
            .enqueue(sample_notification())
            .expect_err("second should fail");
        assert!(matches!(err, ManagerError::CapacityExceeded));
    }

    #[test]
    fn delivers_notifications_in_dependency_order() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut manager = ExExManager::new(8);
        manager
            .register(vec![
                reg("otel", &[], log.clone()),
                reg("btcfi", &["otel"], log.clone()),
                reg("mcp", &["btcfi"], log.clone()),
            ])
            .expect("register");

        manager.enqueue(sample_notification()).expect("enqueue");
        manager.drain_one().expect("drain");

        let calls = log.lock().expect("lock log").clone();
        assert_eq!(
            calls,
            vec!["otel".to_string(), "btcfi".to_string(), "mcp".to_string()]
        );
    }

    #[test]
    fn replays_wal_with_legacy_payloads() {
        let mut manager = ExExManager::new(4);
        manager.enqueue(sample_notification()).expect("enqueue v2");
        manager.wal_mut().append_raw_legacy_v1(NotificationV1 {
            block_number: 5,
            tx_count: 1,
        });

        let replayed = manager.replay_wal().expect("replay");
        assert_eq!(replayed.len(), 2);
        assert!(matches!(
            replayed[1],
            StarknetExExNotification::V1(NotificationV1 {
                block_number: 5,
                tx_count: 1
            })
        ));
    }
}
