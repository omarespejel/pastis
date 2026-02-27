use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::thread;

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

type SharedSink = Arc<Mutex<Box<dyn NotificationSink>>>;

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
    sinks: HashMap<String, SharedSink>,
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
            .map(|registration| {
                (
                    registration.meta.name,
                    Arc::new(Mutex::new(registration.sink)),
                )
            })
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
            self.deliver_tier(&names, &notification)?;
            return Ok(Some(notification_id));
        }

        for tier in &self.tiers {
            // Deterministic barrier: all sinks in current tier must finish before the next tier.
            self.deliver_tier(tier, &notification)?;
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

    fn deliver_tier(
        &self,
        tier: &[String],
        notification: &StarknetExExNotification,
    ) -> Result<(), ManagerError> {
        let mut workers = Vec::with_capacity(tier.len());
        for name in tier {
            let sink = self
                .sinks
                .get(name)
                .cloned()
                .ok_or_else(|| ManagerError::MissingSink(name.clone()))?;
            let sink_name = name.clone();
            let cloned_notification = notification.clone();
            let worker = thread::spawn(move || -> Result<(), ManagerError> {
                let mut sink_guard = sink.lock().map_err(|_| ManagerError::SinkFailure {
                    name: sink_name.clone(),
                    message: "sink mutex poisoned".to_string(),
                })?;
                sink_guard
                    .on_notification(cloned_notification)
                    .map_err(|message| ManagerError::SinkFailure {
                        name: sink_name,
                        message,
                    })
            });
            workers.push((name.clone(), worker));
        }

        // Deterministic failure ordering: join in planned tier order, not completion order.
        for (name, worker) in workers {
            match worker.join() {
                Ok(result) => result?,
                Err(_) => {
                    return Err(ManagerError::SinkFailure {
                        name,
                        message: "sink thread panicked".to_string(),
                    });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

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

    struct SlowSink {
        name: String,
        delay: Duration,
        timeline: Arc<Mutex<HashMap<String, (Instant, Instant)>>>,
    }

    impl SlowSink {
        fn new(
            name: &str,
            delay: Duration,
            timeline: Arc<Mutex<HashMap<String, (Instant, Instant)>>>,
        ) -> Self {
            Self {
                name: name.to_string(),
                delay,
                timeline,
            }
        }
    }

    impl NotificationSink for SlowSink {
        fn on_notification(
            &mut self,
            _notification: StarknetExExNotification,
        ) -> Result<(), String> {
            let start = Instant::now();
            std::thread::sleep(self.delay);
            let end = Instant::now();
            self.timeline
                .lock()
                .expect("lock timeline")
                .insert(self.name.clone(), (start, end));
            Ok(())
        }
    }

    struct FailingSink {
        message: String,
        delay: Duration,
    }

    impl NotificationSink for FailingSink {
        fn on_notification(
            &mut self,
            _notification: StarknetExExNotification,
        ) -> Result<(), String> {
            std::thread::sleep(self.delay);
            Err(self.message.clone())
        }
    }

    struct PanickingSink;

    impl NotificationSink for PanickingSink {
        fn on_notification(
            &mut self,
            _notification: StarknetExExNotification,
        ) -> Result<(), String> {
            panic!("intentional sink panic for test");
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

    #[test]
    fn delivers_tiers_in_parallel_with_barrier_between_tiers() {
        let timeline = Arc::new(Mutex::new(HashMap::<String, (Instant, Instant)>::new()));
        let mut manager = ExExManager::new(8);
        manager
            .register(vec![
                ExExRegistration {
                    meta: ExExHandleMeta {
                        name: "tier1-a".to_string(),
                        depends_on: vec![],
                        priority: 1,
                    },
                    sink: Box::new(SlowSink::new(
                        "tier1-a",
                        Duration::from_millis(70),
                        timeline.clone(),
                    )),
                },
                ExExRegistration {
                    meta: ExExHandleMeta {
                        name: "tier1-b".to_string(),
                        depends_on: vec![],
                        priority: 1,
                    },
                    sink: Box::new(SlowSink::new(
                        "tier1-b",
                        Duration::from_millis(70),
                        timeline.clone(),
                    )),
                },
                ExExRegistration {
                    meta: ExExHandleMeta {
                        name: "tier2".to_string(),
                        depends_on: vec!["tier1-a".to_string(), "tier1-b".to_string()],
                        priority: 1,
                    },
                    sink: Box::new(SlowSink::new(
                        "tier2",
                        Duration::from_millis(5),
                        timeline.clone(),
                    )),
                },
            ])
            .expect("register");

        let started = Instant::now();
        manager.enqueue(sample_notification()).expect("enqueue");
        manager.drain_one().expect("drain");
        let elapsed = started.elapsed();

        let times = timeline.lock().expect("lock timeline");
        let tier1_a = times.get("tier1-a").expect("tier1-a");
        let tier1_b = times.get("tier1-b").expect("tier1-b");
        let tier2 = times.get("tier2").expect("tier2");

        assert!(elapsed < Duration::from_millis(120));
        assert!(tier2.0 >= tier1_a.1);
        assert!(tier2.0 >= tier1_b.1);
    }

    #[test]
    fn picks_failures_in_deterministic_tier_order() {
        let mut manager = ExExManager::new(8);
        manager
            .register(vec![
                ExExRegistration {
                    meta: ExExHandleMeta {
                        name: "alpha".to_string(),
                        depends_on: vec![],
                        priority: 1,
                    },
                    sink: Box::new(FailingSink {
                        message: "alpha-failure".to_string(),
                        delay: Duration::from_millis(35),
                    }),
                },
                ExExRegistration {
                    meta: ExExHandleMeta {
                        name: "zeta".to_string(),
                        depends_on: vec![],
                        priority: 1,
                    },
                    sink: Box::new(FailingSink {
                        message: "zeta-failure".to_string(),
                        delay: Duration::from_millis(1),
                    }),
                },
            ])
            .expect("register");

        manager.enqueue(sample_notification()).expect("enqueue");
        let err = manager.drain_one().expect_err("must fail");
        assert!(matches!(
            err,
            ManagerError::SinkFailure { name, message }
            if name == "alpha" && message == "alpha-failure"
        ));
    }

    #[test]
    fn wal_recovery_is_stable_under_concurrent_enqueues() {
        struct NoopSink;
        impl NotificationSink for NoopSink {
            fn on_notification(
                &mut self,
                _notification: StarknetExExNotification,
            ) -> Result<(), String> {
                Ok(())
            }
        }

        let manager = Arc::new(Mutex::new(ExExManager::new(1_024)));
        manager
            .lock()
            .expect("lock manager")
            .register(vec![ExExRegistration {
                meta: ExExHandleMeta {
                    name: "noop".to_string(),
                    depends_on: vec![],
                    priority: 1,
                },
                sink: Box::new(NoopSink),
            }])
            .expect("register");

        let producers = 8usize;
        let per_producer = 50usize;
        let mut threads = Vec::new();
        for _ in 0..producers {
            let manager = Arc::clone(&manager);
            threads.push(thread::spawn(move || {
                for _ in 0..per_producer {
                    manager
                        .lock()
                        .expect("lock manager")
                        .enqueue(sample_notification())
                        .expect("enqueue");
                }
            }));
        }

        for t in threads {
            t.join().expect("join producer");
        }

        let replayed = manager
            .lock()
            .expect("lock manager")
            .replay_wal()
            .expect("replay wal");
        assert_eq!(replayed.len(), producers * per_producer);
    }

    #[test]
    fn reports_panicking_sink_name_deterministically() {
        let mut manager = ExExManager::new(8);
        manager
            .register(vec![ExExRegistration {
                meta: ExExHandleMeta {
                    name: "panic-sink".to_string(),
                    depends_on: vec![],
                    priority: 1,
                },
                sink: Box::new(PanickingSink),
            }])
            .expect("register");

        manager.enqueue(sample_notification()).expect("enqueue");
        let err = manager.drain_one().expect_err("must fail on panic");
        assert!(matches!(
            err,
            ManagerError::SinkFailure { name, message }
            if name == "panic-sink" && message == "sink thread panicked"
        ));
    }
}
