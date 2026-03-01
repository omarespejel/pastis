#![forbid(unsafe_code)]

use std::collections::{HashMap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
#[cfg(test)]
use std::thread;
use std::time::{Duration, Instant};

use node_spec_core::exex_delivery::{DeliveryPlanError, ExExHandleMeta, build_delivery_tiers};
use node_spec_core::notification::{
    NotificationDecodeError, NotificationV1, StarknetExExNotification, decode_wal_entry,
};
use rayon::ThreadPool;
use rayon::prelude::*;

pub trait NotificationSink: Send {
    // In-process sinks are trusted-only. Untrusted logic must run out-of-process and interact
    // through authenticated APIs rather than direct in-process registration.
    fn on_notification(
        &mut self,
        notification: Arc<StarknetExExNotification>,
    ) -> Result<(), String>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkTrust {
    TrustedInProcess,
    Untrusted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExExCredentials {
    pub exex_tokens: HashMap<String, String>,
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryWal {
    entries: VecDeque<Vec<u8>>,
}

impl InMemoryWal {
    pub fn append(&mut self, entry: Vec<u8>) {
        self.entries.push_back(entry);
    }

    pub fn append_raw_legacy_v1(&mut self, v1: NotificationV1) -> Result<(), String> {
        let encoded = bincode::serialize(&v1)
            .map_err(|error| format!("bincode serialize v1 failed: {error}"))?;
        self.entries.push_back(encoded);
        Ok(())
    }

    pub fn entries(&self) -> &VecDeque<Vec<u8>> {
        &self.entries
    }

    pub fn acknowledge_one(&mut self) {
        self.entries.pop_front();
    }
}

pub struct ExExRegistration {
    pub meta: ExExHandleMeta,
    pub trust: SinkTrust,
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
    #[error("multiple sinks failed in tier: {failures:?}")]
    TierFailures { failures: Vec<(String, String)> },
    #[error("failed to encode notification to WAL: {0}")]
    WalEncode(String),
    #[error("failed to decode notification from WAL: {0}")]
    WalDecode(#[from] NotificationDecodeError),
    #[error("WAL contains {entries} entries, exceeding replay safety limit {max}")]
    WalTooManyEntries { entries: usize, max: usize },
    #[error("notification id counter overflowed")]
    NotificationIdOverflow,
    #[error("registration authentication failed")]
    RegistrationAuthFailed,
    #[error("registration credentials missing for exex '{name}'")]
    RegistrationCredentialsMissing { name: String },
    #[error("registration rejected for non-allowlisted exex '{name}'")]
    RegistrationNotAllowed { name: String },
    #[error("registration rejected for untrusted exex '{name}'")]
    UntrustedExExRejected { name: String },
}

pub struct ExExManager {
    tiers: Vec<Vec<String>>,
    sinks: HashMap<String, SharedSink>,
    sink_failures: HashMap<String, u32>,
    disabled_sinks_until: HashMap<String, Instant>,
    buffer: VecDeque<(u64, Arc<StarknetExExNotification>)>,
    next_id: u64,
    max_capacity: usize,
    wal: InMemoryWal,
    delivery_pool: Option<ThreadPool>,
    registration_tokens: HashMap<String, String>,
    sink_failure_cooldown: Duration,
}

const MAX_PARALLEL_DELIVERY_WORKERS: usize = 32;
const MAX_WAL_REPLAY_ENTRIES: usize = 100_000;
const MAX_CONSECUTIVE_SINK_FAILURES: u32 = 3;
const DEFAULT_SINK_FAILURE_COOLDOWN: Duration = Duration::from_secs(300);

impl ExExManager {
    pub fn new(
        max_capacity: usize,
        registration_tokens: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        let delivery_pool = match rayon::ThreadPoolBuilder::new()
            .num_threads(MAX_PARALLEL_DELIVERY_WORKERS.max(1))
            .build()
        {
            Ok(pool) => Some(pool),
            Err(error) => {
                eprintln!(
                    "warning: failed to initialize ExEx delivery threadpool; falling back to sequential delivery: {error}"
                );
                None
            }
        };
        Self {
            tiers: Vec::new(),
            sinks: HashMap::new(),
            sink_failures: HashMap::new(),
            disabled_sinks_until: HashMap::new(),
            buffer: VecDeque::new(),
            next_id: 1,
            max_capacity: max_capacity.clamp(1, MAX_WAL_REPLAY_ENTRIES),
            wal: InMemoryWal::default(),
            delivery_pool,
            registration_tokens: registration_tokens.into_iter().collect(),
            sink_failure_cooldown: DEFAULT_SINK_FAILURE_COOLDOWN,
        }
    }

    pub fn wal(&self) -> &InMemoryWal {
        &self.wal
    }

    pub fn wal_mut(&mut self) -> &mut InMemoryWal {
        &mut self.wal
    }

    pub fn register(
        &mut self,
        registrations: Vec<ExExRegistration>,
        credentials: &ExExCredentials,
    ) -> Result<(), ManagerError> {
        for registration in &registrations {
            let expected_token = self
                .registration_tokens
                .get(&registration.meta.name)
                .ok_or(ManagerError::RegistrationNotAllowed {
                    name: registration.meta.name.clone(),
                })?;
            let provided_token = credentials.exex_tokens.get(&registration.meta.name).ok_or(
                ManagerError::RegistrationCredentialsMissing {
                    name: registration.meta.name.clone(),
                },
            )?;
            if provided_token != expected_token {
                return Err(ManagerError::RegistrationAuthFailed);
            }
            if registration.trust != SinkTrust::TrustedInProcess {
                return Err(ManagerError::UntrustedExExRejected {
                    name: registration.meta.name.clone(),
                });
            }
        }
        let metas: Vec<ExExHandleMeta> = registrations.iter().map(|r| r.meta.clone()).collect();
        self.tiers = build_delivery_tiers(&metas)?;
        self.sink_failures.clear();
        self.disabled_sinks_until.clear();
        self.sinks = registrations
            .into_iter()
            .map(|registration| {
                (
                    registration.meta.name,
                    Arc::new(Mutex::new(registration.sink)),
                )
            })
            .collect();
        for name in self.sinks.keys() {
            self.sink_failures.insert(name.clone(), 0);
        }
        Ok(())
    }

    pub fn has_capacity(&self) -> bool {
        self.buffer.len() < self.max_capacity
    }

    pub fn enqueue(&mut self, notification: StarknetExExNotification) -> Result<u64, ManagerError> {
        if !self.has_capacity() || self.wal.entries().len() >= self.max_capacity {
            return Err(ManagerError::CapacityExceeded);
        }

        let notification_id = self.next_id;
        let next_id = self
            .next_id
            .checked_add(1)
            .ok_or(ManagerError::NotificationIdOverflow)?;
        let notification = Arc::new(notification);
        let encoded = bincode::serialize(notification.as_ref()).map_err(|error| {
            ManagerError::WalEncode(format!("bincode serialize failed: {error}"))
        })?;
        self.wal.append(encoded);
        self.next_id = next_id;
        self.buffer.push_back((notification_id, notification));
        Ok(notification_id)
    }

    pub fn drain_one(&mut self) -> Result<Option<u64>, ManagerError> {
        let Some((notification_id, notification)) = self.buffer.front() else {
            return Ok(None);
        };
        let notification_id = *notification_id;
        let notification = Arc::clone(notification);

        if self.tiers.is_empty() {
            if self.sinks.is_empty() {
                return Err(ManagerError::MissingSink(
                    "no sinks registered for delivery".to_string(),
                ));
            }
            let mut names: Vec<String> = self.sinks.keys().cloned().collect();
            names.sort();
            self.deliver_tier(&names, &notification)?;
            self.buffer.pop_front();
            self.wal.acknowledge_one();
            return Ok(Some(notification_id));
        }

        let tiers = self.tiers.clone();
        for tier in &tiers {
            // Deterministic barrier: all sinks in current tier must finish before the next tier.
            self.deliver_tier(tier, &notification)?;
        }

        self.buffer.pop_front();
        self.wal.acknowledge_one();
        Ok(Some(notification_id))
    }

    pub fn replay_wal(&self) -> Result<Vec<StarknetExExNotification>, ManagerError> {
        if self.wal.entries().len() > MAX_WAL_REPLAY_ENTRIES {
            return Err(ManagerError::WalTooManyEntries {
                entries: self.wal.entries().len(),
                max: MAX_WAL_REPLAY_ENTRIES,
            });
        }
        let mut notifications = Vec::with_capacity(self.wal.entries().len());
        for raw in self.wal.entries() {
            notifications.push(decode_wal_entry(raw)?);
        }
        Ok(notifications)
    }

    fn refresh_reenabled_sinks(&mut self, now: Instant) {
        let reenabled: Vec<String> = self
            .disabled_sinks_until
            .iter()
            .filter_map(|(name, until)| {
                if now >= *until {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect();
        for name in reenabled {
            self.disabled_sinks_until.remove(&name);
            self.sink_failures.insert(name.clone(), 0);
            eprintln!("info: re-enabled ExEx sink after cooldown: {name}");
        }
    }

    #[cfg(test)]
    fn with_sink_failure_cooldown_for_tests(mut self, cooldown: Duration) -> Self {
        self.sink_failure_cooldown = cooldown;
        self
    }

    fn deliver_tier(
        &mut self,
        tier: &[String],
        notification: &Arc<StarknetExExNotification>,
    ) -> Result<(), ManagerError> {
        self.refresh_reenabled_sinks(Instant::now());
        let active_tier: Vec<String> = tier
            .iter()
            .filter(|name| !self.disabled_sinks_until.contains_key(*name))
            .cloned()
            .collect();
        if active_tier.is_empty() {
            return Err(ManagerError::MissingSink(
                "no active sinks available for delivery".to_string(),
            ));
        }

        let mut tier_failures: Vec<(String, String)> = Vec::new();

        for chunk in active_tier.chunks(MAX_PARALLEL_DELIVERY_WORKERS.max(1)) {
            let results: Vec<(String, Result<(), ManagerError>)> = if let Some(pool) =
                &self.delivery_pool
            {
                pool.install(|| {
                    chunk
                        .par_iter()
                        .map(|name| {
                            let result = catch_unwind(AssertUnwindSafe(|| {
                                self.deliver_to_sink(name, notification)
                            }))
                            .unwrap_or_else(|panic_payload| {
                                Err(ManagerError::SinkFailure {
                                    name: name.clone(),
                                    message: format!(
                                        "sink panicked: {}",
                                        panic_payload_message(panic_payload)
                                    ),
                                })
                            });
                            (name.clone(), result)
                        })
                        .collect()
                })
            } else {
                chunk
                    .iter()
                    .map(|name| {
                        let result = catch_unwind(AssertUnwindSafe(|| {
                            self.deliver_to_sink(name, notification)
                        }))
                        .unwrap_or_else(|panic_payload| {
                            Err(ManagerError::SinkFailure {
                                name: name.clone(),
                                message: format!(
                                    "sink panicked: {}",
                                    panic_payload_message(panic_payload)
                                ),
                            })
                        });
                        (name.clone(), result)
                    })
                    .collect()
            };

            for (name, result) in results {
                match result {
                    Ok(()) => {
                        self.sink_failures.insert(name, 0);
                    }
                    Err(err) => {
                        let failures = self.sink_failures.entry(name.clone()).or_insert(0);
                        *failures = failures.saturating_add(1);
                        if *failures >= MAX_CONSECUTIVE_SINK_FAILURES {
                            self.disabled_sinks_until
                                .insert(name.clone(), Instant::now() + self.sink_failure_cooldown);
                            eprintln!(
                                "warning: disabling ExEx sink '{name}' after {MAX_CONSECUTIVE_SINK_FAILURES} consecutive failures"
                            );
                            continue;
                        }
                        match err {
                            ManagerError::SinkFailure {
                                name: failed_name,
                                message,
                            } => tier_failures.push((failed_name, message)),
                            other => tier_failures.push((name, other.to_string())),
                        }
                    }
                }
            }
        }

        if tier_failures.len() == 1 {
            let (name, message) = tier_failures.remove(0);
            return Err(ManagerError::SinkFailure { name, message });
        }
        if !tier_failures.is_empty() {
            return Err(ManagerError::TierFailures {
                failures: tier_failures,
            });
        }

        Ok(())
    }

    fn deliver_to_sink(
        &self,
        name: &str,
        notification: &Arc<StarknetExExNotification>,
    ) -> Result<(), ManagerError> {
        let sink = self
            .sinks
            .get(name)
            .cloned()
            .ok_or_else(|| ManagerError::MissingSink(name.to_string()))?;
        let mut sink_guard = sink.lock().map_err(|_| ManagerError::SinkFailure {
            name: name.to_string(),
            message: "sink mutex poisoned".to_string(),
        })?;
        sink_guard
            .on_notification(Arc::clone(notification))
            .map_err(|message| ManagerError::SinkFailure {
                name: name.to_string(),
                message,
            })
    }

    #[cfg(test)]
    fn is_sink_disabled(&self, name: &str) -> bool {
        self.disabled_sinks_until.contains_key(name)
    }
}

fn panic_payload_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "unknown panic payload".to_string()
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
            _notification: Arc<StarknetExExNotification>,
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
            _notification: Arc<StarknetExExNotification>,
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
            _notification: Arc<StarknetExExNotification>,
        ) -> Result<(), String> {
            std::thread::sleep(self.delay);
            Err(self.message.clone())
        }
    }

    struct FlakySink {
        remaining_failures: u32,
        success_count: Arc<Mutex<u32>>,
    }

    impl NotificationSink for FlakySink {
        fn on_notification(
            &mut self,
            _notification: Arc<StarknetExExNotification>,
        ) -> Result<(), String> {
            if self.remaining_failures > 0 {
                self.remaining_failures -= 1;
                return Err("flaky-failure".to_string());
            }
            let mut count = self.success_count.lock().expect("lock success count");
            *count += 1;
            Ok(())
        }
    }

    struct PanickingSink;

    impl NotificationSink for PanickingSink {
        fn on_notification(
            &mut self,
            _notification: Arc<StarknetExExNotification>,
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
            trust: SinkTrust::TrustedInProcess,
            sink: Box::new(RecordingSink::new(name, log)),
        }
    }

    fn token_for(name: &str) -> String {
        format!("token-{name}")
    }

    fn credentials_for(names: &[&str]) -> ExExCredentials {
        ExExCredentials {
            exex_tokens: names
                .iter()
                .map(|name| ((*name).to_string(), token_for(name)))
                .collect(),
        }
    }

    fn manager(max_capacity: usize, allowed_names: &[&str]) -> ExExManager {
        ExExManager::new(
            max_capacity,
            allowed_names
                .iter()
                .map(|name| ((*name).to_string(), token_for(name))),
        )
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
        let mut manager = manager(16, &["a", "b"]);
        let creds = credentials_for(&["a", "b"]);
        let err = manager
            .register(
                vec![reg("a", &["b"], log.clone()), reg("b", &["a"], log.clone())],
                &creds,
            )
            .expect_err("must fail");

        assert!(matches!(
            err,
            ManagerError::Registration(DeliveryPlanError::DependencyCycle)
        ));
    }

    #[test]
    fn rejects_registration_with_invalid_token() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut manager = manager(8, &["otel"]);
        let wrong = ExExCredentials {
            exex_tokens: HashMap::from([("otel".to_string(), "wrong-token".to_string())]),
        };
        let err = manager
            .register(vec![reg("otel", &[], log)], &wrong)
            .expect_err("must reject invalid token");
        assert!(matches!(err, ManagerError::RegistrationAuthFailed));
    }

    #[test]
    fn rejects_registration_when_credentials_missing_for_exex() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut manager = manager(8, &["otel"]);
        let creds = ExExCredentials {
            exex_tokens: HashMap::new(),
        };
        let err = manager
            .register(vec![reg("otel", &[], log)], &creds)
            .expect_err("must reject missing per-exex credential");
        assert!(matches!(
            err,
            ManagerError::RegistrationCredentialsMissing { name } if name == "otel"
        ));
    }

    #[test]
    fn rejects_registration_for_non_allowlisted_exex() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut manager = manager(8, &["otel"]);
        let creds = credentials_for(&["btcfi"]);
        let err = manager
            .register(vec![reg("btcfi", &[], log)], &creds)
            .expect_err("must reject non-allowlisted name");
        assert!(matches!(
            err,
            ManagerError::RegistrationNotAllowed { name } if name == "btcfi"
        ));
    }

    #[test]
    fn rejects_untrusted_exex_registrations() {
        let mut manager = manager(8, &["otel"]);
        let creds = credentials_for(&["otel"]);
        let err = manager
            .register(
                vec![ExExRegistration {
                    meta: ExExHandleMeta {
                        name: "otel".to_string(),
                        depends_on: vec![],
                        priority: 1,
                    },
                    trust: SinkTrust::Untrusted,
                    sink: Box::new(PanickingSink),
                }],
                &creds,
            )
            .expect_err("must reject untrusted sinks");
        assert!(matches!(
            err,
            ManagerError::UntrustedExExRejected { name } if name == "otel"
        ));
    }

    #[test]
    fn enforces_notification_capacity() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut manager = manager(1, &["otel"]);
        let creds = credentials_for(&["otel"]);
        manager
            .register(vec![reg("otel", &[], log.clone())], &creds)
            .expect("register");

        manager.enqueue(sample_notification()).expect("first");
        let err = manager
            .enqueue(sample_notification())
            .expect_err("second should fail");
        assert!(matches!(err, ManagerError::CapacityExceeded));
    }

    #[test]
    fn zero_capacity_configuration_is_hardened_to_one_slot() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut manager = manager(0, &["otel"]);
        let creds = credentials_for(&["otel"]);
        manager
            .register(vec![reg("otel", &[], log.clone())], &creds)
            .expect("register");

        manager
            .enqueue(sample_notification())
            .expect("first enqueue should fit hardened one-slot capacity");
        let err = manager
            .enqueue(sample_notification())
            .expect_err("second enqueue should exceed one-slot capacity");
        assert!(matches!(err, ManagerError::CapacityExceeded));

        manager.drain_one().expect("drain");
        assert!(log.lock().expect("lock log").contains(&"otel".to_string()));
    }

    #[test]
    fn delivers_notifications_in_dependency_order() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut manager = manager(8, &["otel", "btcfi", "mcp"]);
        let creds = credentials_for(&["otel", "btcfi", "mcp"]);
        manager
            .register(
                vec![
                    reg("otel", &[], log.clone()),
                    reg("btcfi", &["otel"], log.clone()),
                    reg("mcp", &["btcfi"], log.clone()),
                ],
                &creds,
            )
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
        let mut manager = manager(4, &[]);
        manager.enqueue(sample_notification()).expect("enqueue v2");
        manager
            .wal_mut()
            .append_raw_legacy_v1(NotificationV1 {
                block_number: 5,
                tx_count: 1,
            })
            .expect("append legacy");

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
        let mut manager = manager(8, &["tier1-a", "tier1-b", "tier2"]);
        let creds = credentials_for(&["tier1-a", "tier1-b", "tier2"]);
        manager
            .register(
                vec![
                    ExExRegistration {
                        meta: ExExHandleMeta {
                            name: "tier1-a".to_string(),
                            depends_on: vec![],
                            priority: 1,
                        },
                        trust: SinkTrust::TrustedInProcess,
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
                        trust: SinkTrust::TrustedInProcess,
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
                        trust: SinkTrust::TrustedInProcess,
                        sink: Box::new(SlowSink::new(
                            "tier2",
                            Duration::from_millis(5),
                            timeline.clone(),
                        )),
                    },
                ],
                &creds,
            )
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
        let mut manager = manager(8, &["alpha", "zeta"]);
        let creds = credentials_for(&["alpha", "zeta"]);
        manager
            .register(
                vec![
                    ExExRegistration {
                        meta: ExExHandleMeta {
                            name: "alpha".to_string(),
                            depends_on: vec![],
                            priority: 1,
                        },
                        trust: SinkTrust::TrustedInProcess,
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
                        trust: SinkTrust::TrustedInProcess,
                        sink: Box::new(FailingSink {
                            message: "zeta-failure".to_string(),
                            delay: Duration::from_millis(1),
                        }),
                    },
                ],
                &creds,
            )
            .expect("register");

        manager.enqueue(sample_notification()).expect("enqueue");
        let err = manager.drain_one().expect_err("must fail");
        match err {
            ManagerError::TierFailures { failures } => {
                assert_eq!(
                    failures,
                    vec![
                        ("alpha".to_string(), "alpha-failure".to_string()),
                        ("zeta".to_string(), "zeta-failure".to_string()),
                    ]
                );
            }
            other => panic!("expected TierFailures, got {other:?}"),
        }
    }

    #[test]
    fn retains_notification_in_buffer_when_delivery_fails() {
        let mut manager = manager(8, &["failing"]);
        let creds = credentials_for(&["failing"]);
        manager
            .register(
                vec![ExExRegistration {
                    meta: ExExHandleMeta {
                        name: "failing".to_string(),
                        depends_on: vec![],
                        priority: 1,
                    },
                    trust: SinkTrust::TrustedInProcess,
                    sink: Box::new(FailingSink {
                        message: "boom".to_string(),
                        delay: Duration::from_millis(1),
                    }),
                }],
                &creds,
            )
            .expect("register");
        manager.enqueue(sample_notification()).expect("enqueue");

        let err = manager.drain_one().expect_err("must fail");
        assert!(matches!(
            err,
            ManagerError::SinkFailure { name, message }
            if name == "failing" && message == "boom"
        ));
        assert_eq!(manager.buffer.len(), 1);
    }

    #[test]
    fn disables_sink_after_repeated_failures_to_unblock_delivery() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut manager = manager(8, &["always-fail", "healthy"]);
        let creds = credentials_for(&["always-fail", "healthy"]);
        manager
            .register(
                vec![
                    ExExRegistration {
                        meta: ExExHandleMeta {
                            name: "always-fail".to_string(),
                            depends_on: vec![],
                            priority: 1,
                        },
                        trust: SinkTrust::TrustedInProcess,
                        sink: Box::new(FailingSink {
                            message: "permanent-failure".to_string(),
                            delay: Duration::from_millis(1),
                        }),
                    },
                    reg("healthy", &[], log.clone()),
                ],
                &creds,
            )
            .expect("register");
        manager.enqueue(sample_notification()).expect("enqueue");

        let first = manager.drain_one().expect_err("first failure");
        assert!(matches!(
            first,
            ManagerError::SinkFailure { name, message }
            if name == "always-fail" && message == "permanent-failure"
        ));
        let second = manager.drain_one().expect_err("second failure");
        assert!(matches!(
            second,
            ManagerError::SinkFailure { name, message }
            if name == "always-fail" && message == "permanent-failure"
        ));

        let third = manager.drain_one().expect("circuit breaker should unblock");
        assert_eq!(third, Some(1));
        assert!(manager.is_sink_disabled("always-fail"));
        assert!(manager.buffer.is_empty());
        assert!(manager.wal().entries().is_empty());

        let calls = log.lock().expect("lock log");
        assert!(!calls.is_empty());
    }

    #[test]
    fn re_enables_sink_after_cooldown_and_successful_retry() {
        let success_count = Arc::new(Mutex::new(0_u32));
        let mut manager =
            manager(8, &["flaky"]).with_sink_failure_cooldown_for_tests(Duration::from_millis(5));
        let creds = credentials_for(&["flaky"]);
        manager
            .register(
                vec![ExExRegistration {
                    meta: ExExHandleMeta {
                        name: "flaky".to_string(),
                        depends_on: vec![],
                        priority: 1,
                    },
                    trust: SinkTrust::TrustedInProcess,
                    sink: Box::new(FlakySink {
                        remaining_failures: 3,
                        success_count: Arc::clone(&success_count),
                    }),
                }],
                &creds,
            )
            .expect("register");

        manager.enqueue(sample_notification()).expect("enqueue 1");
        manager.drain_one().expect_err("failure 1");
        manager.drain_one().expect_err("failure 2");
        let third = manager
            .drain_one()
            .expect("third failure should disable sink");
        assert_eq!(third, Some(1));
        assert!(manager.is_sink_disabled("flaky"));

        manager.enqueue(sample_notification()).expect("enqueue 2");
        let err = manager
            .drain_one()
            .expect_err("disabled sink must keep notification queued");
        assert!(matches!(err, ManagerError::MissingSink(_)));
        assert!(manager.is_sink_disabled("flaky"));
        assert_eq!(manager.buffer.len(), 1);

        std::thread::sleep(Duration::from_millis(10));
        let fourth = manager
            .drain_one()
            .expect("sink should recover after cooldown");
        assert_eq!(fourth, Some(2));
        assert!(!manager.is_sink_disabled("flaky"));
        assert_eq!(*success_count.lock().expect("lock success count"), 1);
    }

    #[test]
    fn compacts_wal_after_successful_delivery() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut manager = manager(8, &["otel"]);
        let creds = credentials_for(&["otel"]);
        manager
            .register(vec![reg("otel", &[], log)], &creds)
            .expect("register");
        manager.enqueue(sample_notification()).expect("enqueue");
        assert_eq!(manager.wal().entries().len(), 1);

        manager.drain_one().expect("drain");
        assert!(manager.wal().entries().is_empty());
    }

    #[test]
    fn wal_recovery_is_stable_under_concurrent_enqueues() {
        struct NoopSink;
        impl NotificationSink for NoopSink {
            fn on_notification(
                &mut self,
                _notification: Arc<StarknetExExNotification>,
            ) -> Result<(), String> {
                Ok(())
            }
        }

        let manager = Arc::new(Mutex::new(manager(1_024, &["noop"])));
        let creds = credentials_for(&["noop"]);
        manager
            .lock()
            .expect("lock manager")
            .register(
                vec![ExExRegistration {
                    meta: ExExHandleMeta {
                        name: "noop".to_string(),
                        depends_on: vec![],
                        priority: 1,
                    },
                    trust: SinkTrust::TrustedInProcess,
                    sink: Box::new(NoopSink),
                }],
                &creds,
            )
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
        let mut manager = manager(8, &["panic-sink"]);
        let creds = credentials_for(&["panic-sink"]);
        manager
            .register(
                vec![ExExRegistration {
                    meta: ExExHandleMeta {
                        name: "panic-sink".to_string(),
                        depends_on: vec![],
                        priority: 1,
                    },
                    trust: SinkTrust::TrustedInProcess,
                    sink: Box::new(PanickingSink),
                }],
                &creds,
            )
            .expect("register");

        manager.enqueue(sample_notification()).expect("enqueue");
        let err = manager.drain_one().expect_err("must fail on panic");
        assert!(matches!(
            err,
            ManagerError::SinkFailure { name, message }
            if name == "panic-sink" && message.contains("intentional sink panic for test")
        ));
    }

    #[test]
    fn rejects_enqueue_when_notification_id_overflows() {
        let mut manager = manager(8, &[]);
        manager.next_id = u64::MAX;

        let err = manager
            .enqueue(sample_notification())
            .expect_err("must fail");
        assert!(matches!(err, ManagerError::NotificationIdOverflow));
        assert!(manager.buffer.is_empty());
        assert!(manager.wal().entries().is_empty());
    }
}
