use std::sync::OnceLock;
use std::time::Duration;

macro_rules! node_settings {
    ($($field:ident: $ty:ty = $default:expr, $parsed:ty, $wrap:expr, $valid:expr, $requirement:literal;)*) => {
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct NodeSettings { $(pub $field: $ty,)* pub transport_unbounded: bool }

        impl Default for NodeSettings {
            fn default() -> Self { Self { $($field: $default,)* transport_unbounded: false } }
        }

        #[derive(Clone, Debug, Default, PartialEq, Eq)]
        pub struct NodeSettingsOverrides { $(pub $field: Option<$parsed>,)* }

        impl NodeSettingsOverrides {
            pub const KEYS: &'static [&'static str] = &[$(stringify!($field),)*];

            pub fn set(&mut self, key: &str, value: &str) -> Result<(), String> {
                match key {
                    $(stringify!($field) => {
                        let parsed: $parsed = value.parse().map_err(|_| format!("`{key}`: expected a representable unsigned integer, got {value:?}"))?;
                        if !($valid)(parsed) { return Err(format!("`{key}` {}", $requirement)); }
                        self.$field = Some(parsed);
                    })*
                    _ => return Err(format!("unknown node settings key `{key}`")),
                }
                Ok(())
            }

            pub fn validate(&self) -> Result<(), String> {
                $(if self.$field.is_some_and(|value| !($valid)(value)) {
                    return Err(format!("`{}` {}", stringify!($field), $requirement));
                })*
                Ok(())
            }

            pub fn resolve(&self, lookup: impl Fn(&str) -> Option<String>) -> Result<(NodeSettings, Vec<String>), String> {
                self.validate()?;
                let mut settings = NodeSettings::default();
                let mut legacy = Vec::new();
                $(let alias = format!("VALS_{}", stringify!($field).to_uppercase());
                let env_value: Option<$parsed> = lookup(&alias).map(|value| {
                    let parsed: $parsed = value.parse().map_err(|_| format!("{alias} (config key `{}`): expected a representable unsigned integer, got {value:?}", stringify!($field)))?;
                    if !($valid)(parsed) { return Err(format!("{alias} (config key `{}`) {}", stringify!($field), $requirement)); }
                    Ok(parsed)
                }).transpose()?;
                match (self.$field, env_value) {
                    (Some(config), Some(env)) if config != env => return Err(format!("conflicting `{}`={config} and {alias}={env}; remove the legacy environment variable or make both values equal", stringify!($field))),
                    (Some(config), _) => settings.$field = ($wrap)(config),
                    (None, Some(env)) => { legacy.push(alias); settings.$field = ($wrap)(env); }
                    (None, None) => {}
                })*
                settings.transport_unbounded = match lookup("VALS_TRANSPORT_UNBOUNDED").as_deref() {
                    None | Some("0") => false,
                    Some("1") => true,
                    Some(value) => return Err(format!("VALS_TRANSPORT_UNBOUNDED must be 0 or 1, got {value:?}")),
                };
                if settings.transport_unbounded && self.has_transport_keys() {
                    return Err("VALS_TRANSPORT_UNBOUNDED=1 conflicts with transport_* config keys".into());
                }
                Ok((settings, legacy))
            }

            pub fn has_transport_keys(&self) -> bool {
                false $(|| (stringify!($field).starts_with("transport_") && self.$field.is_some()))*
            }
        }
    };
}

node_settings! {
    rehome_copy_page_entries: u32 = 4096, u32, |v| v, |v| v > 0, "must be > 0";
    rehome_copy_pages_per_tick: u32 = 1, u32, |v| v, |v| v > 0, "must be > 0";
    rehome_copy_hold_ms: u64 = 60_000, u64, |v| v, |_: u64| true, "must be an unsigned integer";
    flush_tail_epochs: u64 = 256, u64, |v| v, |_: u64| true, "must be an unsigned integer";
    flush_max_bytes: u64 = 4 * 1024 * 1024 * 1024, u64, |v| v, |_: u64| true, "must be an unsigned integer";
    flush_interval_ms: Option<u64> = None, u64, Some, |v| v >= 100, "must be >= 100";
    run_compact_threshold: usize = 8, usize, |v| v, |v| v >= 2, "must be >= 2";
    compact_step_segments: usize = 4, usize, |v| v, |v| v >= 1, "must be >= 1";
    device_dealloc_segments_per_tick: u32 = 0, u32, |v| v, |_: u32| true, "must be an unsigned integer";
    compact_interval_ms: Option<u64> = None, u64, Some, |v| v >= 200, "must be >= 200";
    maint_compact_lanes: usize = 2, usize, |v| v, |v| (1..=4).contains(&v), "must be in 1..=4";
    cleanup_min_confirms: Option<usize> = None, usize, Some, |v| v >= 1, "must be >= 1";
    cleanup_idle_burst_ms: u64 = 1000, u64, |v| v, |_: u64| true, "must be an unsigned integer";
    ram_budget_bytes: Option<u64> = None, u64, Some, |_: u64| true, "must be an unsigned integer";
    ram_maint_reserve_bytes: Option<u64> = None, u64, Some, |_: u64| true, "must be an unsigned integer";
    ram_dma_scratch_bytes: u64 = 0, u64, |v| v, |_: u64| true, "must be an unsigned integer";
    ae_seed_concurrency: usize = 4, usize, |v| v, |_: usize| true, "must be an unsigned integer";
    ae_seed_shed_cooldown_ms: u64 = 10_000, u64, |v| v, |_: u64| true, "must be an unsigned integer";
    ae_seed_isolation: u8 = 1, u8, |v| v, |v| v <= 1, "must be 0 or 1";
    ae_seed_duty_pct: u64 = 25, u64, |v| v, |v| (1..=100).contains(&v), "must be in 1..=100";
    ae_seed_journal_max_bytes: u64 = 256 * 1024 * 1024, u64, |v| v, |_: u64| true, "must be an unsigned integer";
    ae_seed_pin_max_bytes: u64 = 64 * 1024 * 1024 * 1024, u64, |v| v, |_: u64| true, "must be an unsigned integer";
    ae_span_regroup: u8 = 1, u8, |v| v, |v| v <= 1, "must be 0 or 1";
    transport_max_connections: usize = 4096, usize, |v| v, |v| v > 0, "must be > 0";
    transport_max_requests: usize = 32_768, usize, |v| v, |v| v > 0, "must be > 0";
    transport_max_queued_bytes: usize = 512 * 1024 * 1024, usize, |v| v, |v| v > 0, "must be > 0";
    transport_initial_timeout_ms: u64 = 5000, u64, |v| v, |v| v > 0, "must be > 0";
    transport_frame_timeout_ms: u64 = 30_000, u64, |v| v, |v| v > 0, "must be > 0";
    transport_idle_timeout_ms: u64 = 120_000, u64, |v| v, |v| v > 0, "must be > 0";
    transport_write_timeout_ms: u64 = 30_000, u64, |v| v, |v| v > 0, "must be > 0";
    scan_max_inflight: usize = 8, usize, |v| v, |v| v > 0, "must be > 0";
    batch_apply_gate: usize = 64, usize, |v| v, |v| (1..=256).contains(&v), "must be in 1..=256";
    admit_band_pct: u64 = 6, u64, |v| v, |v| (1..=100).contains(&v), "must be in 1..=100";
    integrity_fail_fast_attempts: u32 = 3, u32, |v| v, |v| v != 1, "must be 0 (disabled) or >= 2";
    integrity_fail_fast_secs: u64 = 30, u64, |v| v, |_: u64| true, "must be an unsigned integer";
}

impl NodeSettings {
    pub fn resolve_ram_defaults(mut self) -> Self {
        if self.ram_budget_bytes.is_none() {
            let budget = crate::ram::default_budget();
            if budget.is_none() {
                log::warn!("RAM admission control disabled: /proc/meminfo MemTotal is unavailable; set ram_budget_bytes explicitly");
            }
            self.ram_budget_bytes = Some(budget.unwrap_or(0));
        }
        let budget = self.ram_budget_bytes.unwrap_or(0);
        self.ram_maint_reserve_bytes = Some(if budget == 0 { 0 } else {
            self.ram_maint_reserve_bytes.unwrap_or(budget / 100 * 19)
        });
        self
    }

    pub fn flush_interval(&self, sweep: Duration) -> Duration {
        self.flush_interval_ms.map(Duration::from_millis)
            .unwrap_or_else(|| sweep.saturating_mul(4).clamp(Duration::from_millis(100), Duration::from_secs(1)))
    }

    pub fn compact_interval(&self, sweep: Duration) -> Duration {
        self.compact_interval_ms.map(Duration::from_millis)
            .unwrap_or_else(|| sweep.saturating_mul(20).max(Duration::from_millis(200)))
    }

    pub fn transport_limits(&self) -> crate::transport::TransportLimits {
        crate::transport::TransportLimits {
            max_connections: if self.transport_unbounded { usize::MAX } else { self.transport_max_connections },
            max_requests: if self.transport_unbounded { usize::MAX } else { self.transport_max_requests },
            max_queued_bytes: if self.transport_unbounded { usize::MAX } else { self.transport_max_queued_bytes },
            initial_timeout: (!self.transport_unbounded).then(|| Duration::from_millis(self.transport_initial_timeout_ms)),
            frame_timeout: (!self.transport_unbounded).then(|| Duration::from_millis(self.transport_frame_timeout_ms)),
            idle_timeout: (!self.transport_unbounded).then(|| Duration::from_millis(self.transport_idle_timeout_ms)),
            write_timeout: (!self.transport_unbounded).then(|| Duration::from_millis(self.transport_write_timeout_ms)),
        }
    }

    pub fn configure_process(&self) -> Result<(), String> {
        let requested = ProcessSettings::from(self);
        let active = PROCESS.get_or_init(|| requested.clone());
        if *active != requested {
            return Err(format!("node resources conflict with process-stable configuration {active:?}"));
        }
        crate::transport::configure(self.transport_limits())?;
        vals_store::configure_admit_band_pct(self.admit_band_pct)?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProcessSettings {
    pub ram_budget_bytes: Option<u64>,
    pub ram_maint_reserve_bytes: Option<u64>,
    pub ram_dma_scratch_bytes: u64,
    pub ae_seed_concurrency: usize,
    pub ae_seed_shed_cooldown_ms: u64,
    pub ae_seed_isolation: u8,
    pub ae_seed_duty_pct: u64,
    pub ae_seed_journal_max_bytes: u64,
    pub ae_seed_pin_max_bytes: u64,
    pub ae_span_regroup: u8,
    pub scan_max_inflight: usize,
}

impl From<&NodeSettings> for ProcessSettings {
    fn from(s: &NodeSettings) -> Self {
        Self {
            ram_budget_bytes: s.ram_budget_bytes,
            ram_maint_reserve_bytes: s.ram_maint_reserve_bytes,
            ram_dma_scratch_bytes: s.ram_dma_scratch_bytes,
            ae_seed_concurrency: s.ae_seed_concurrency,
            ae_seed_shed_cooldown_ms: s.ae_seed_shed_cooldown_ms,
            ae_seed_isolation: s.ae_seed_isolation,
            ae_seed_duty_pct: s.ae_seed_duty_pct,
            ae_seed_journal_max_bytes: s.ae_seed_journal_max_bytes,
            ae_seed_pin_max_bytes: s.ae_seed_pin_max_bytes,
            ae_span_regroup: s.ae_span_regroup,
            scan_max_inflight: s.scan_max_inflight,
        }
    }
}

static PROCESS: OnceLock<ProcessSettings> = OnceLock::new();

pub(crate) fn process() -> &'static ProcessSettings {
    PROCESS.get_or_init(|| ProcessSettings::from(&NodeSettings::default()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_derived_intervals() {
        let (s, legacy) = NodeSettingsOverrides::default().resolve(|_| None).unwrap();
        assert_eq!(s, NodeSettings::default());
        assert!(legacy.is_empty());
        assert_eq!(s.transport_limits(), crate::transport::TransportLimits::default());
        assert_eq!(s.flush_interval(Duration::from_millis(1)), Duration::from_millis(100));
        assert_eq!(s.flush_interval(Duration::from_secs(5)), Duration::from_secs(1));
        assert_eq!(s.compact_interval(Duration::from_secs(5)), Duration::from_secs(100));
        assert_eq!(s.rehome_copy_page_entries, 4096);
        assert_eq!(s.rehome_copy_pages_per_tick, 1);
        assert_eq!(s.rehome_copy_hold_ms, 60_000);
        assert_eq!(s.flush_tail_epochs, 256);
        assert_eq!(s.flush_max_bytes, 4 * 1024 * 1024 * 1024);
        assert_eq!(s.run_compact_threshold, 8);
        assert_eq!(s.compact_step_segments, 4);
        assert_eq!(s.device_dealloc_segments_per_tick, 0);
        assert_eq!(s.maint_compact_lanes, 2);
        assert_eq!(s.cleanup_min_confirms, None);
        assert_eq!(s.ram_budget_bytes, None);
        assert_eq!(s.ram_maint_reserve_bytes, None);
        assert_eq!(s.ram_dma_scratch_bytes, 0);
        assert_eq!(s.ae_seed_concurrency, 4);
        assert_eq!(s.ae_seed_shed_cooldown_ms, 10_000);
        assert_eq!(s.ae_seed_isolation, 1);
        assert_eq!(s.ae_seed_duty_pct, 25);
        assert_eq!(s.ae_seed_journal_max_bytes, 256 * 1024 * 1024);
        assert_eq!(s.ae_seed_pin_max_bytes, 64 * 1024 * 1024 * 1024);
        assert_eq!(s.ae_span_regroup, 1);
        assert_eq!(s.scan_max_inflight, 8);
        assert_eq!(s.batch_apply_gate, 64);
        assert_eq!(s.admit_band_pct, 6);
        assert_eq!(s.integrity_fail_fast_attempts, 3);
        assert_eq!(s.integrity_fail_fast_secs, 30);
    }

    #[test]
    fn every_key_precedence_and_malformed() {
        for &key in NodeSettingsOverrides::KEYS {
            let alias = format!("VALS_{}", key.to_uppercase());
            let mut overrides = NodeSettingsOverrides::default();
            overrides.set(key, "200").or_else(|_| overrides.set(key, "2"))
                .or_else(|_| overrides.set(key, "1")).unwrap();
            let value = match key {
                "maint_compact_lanes" | "admit_band_pct" | "ae_seed_duty_pct" => "2",
                "ae_seed_isolation" | "ae_span_regroup" => "1",
                _ => "200",
            };
            assert!(overrides.resolve(|name| (name == alias).then(|| value.into())).unwrap().1.is_empty());
            assert_eq!(NodeSettingsOverrides::default().resolve(|name| (name == alias).then(|| value.into())).unwrap().1, [alias.clone()]);
            assert!(overrides.resolve(|name| (name == alias).then(|| "201".into())).is_err());
            assert!(overrides.resolve(|name| (name == alias).then(|| "bad".into())).unwrap_err().contains(&alias));
            assert!(overrides.set(key, "-1").is_err());
            assert!(overrides.set(key, "18446744073709551616").is_err());
        }
    }

    #[test]
    fn batch_apply_gate_bounds_and_alias() {
        for limit in [1, 64, 256] {
            let mut overrides = NodeSettingsOverrides::default();
            overrides.set("batch_apply_gate", &limit.to_string()).unwrap();
            assert_eq!(overrides.resolve(|_| None).unwrap().0.batch_apply_gate, limit);
            let (settings, aliases) = NodeSettingsOverrides::default().resolve(|name| {
                (name == "VALS_BATCH_APPLY_GATE").then(|| limit.to_string())
            }).unwrap();
            assert_eq!(settings.batch_apply_gate, limit);
            assert_eq!(aliases, ["VALS_BATCH_APPLY_GATE"]);
        }
        for limit in [0, 257] {
            let overrides = NodeSettingsOverrides { batch_apply_gate: Some(limit), ..Default::default() };
            assert!(overrides.validate().is_err());
        }
    }

    #[test]
    fn ram_defaults_and_disabled_reserve() {
        let settings = NodeSettings { ram_budget_bytes: Some(199), ..Default::default() }.resolve_ram_defaults();
        assert_eq!(settings.ram_maint_reserve_bytes, Some(19));
        let settings = NodeSettings { ram_budget_bytes: Some(0), ram_maint_reserve_bytes: Some(999), ..Default::default() }.resolve_ram_defaults();
        assert_eq!(settings.ram_maint_reserve_bytes, Some(0));
        for key in ["ram_budget_bytes", "ram_maint_reserve_bytes", "ram_dma_scratch_bytes",
            "ae_seed_concurrency", "ae_seed_shed_cooldown_ms", "ae_seed_pin_max_bytes", "rehome_copy_hold_ms",
            "flush_tail_epochs", "flush_max_bytes", "device_dealloc_segments_per_tick",
            "integrity_fail_fast_attempts", "integrity_fail_fast_secs"] {
            assert!(NodeSettingsOverrides::default().set(key, "0").is_ok());
        }
    }

    #[test]
    fn ranges_and_unbounded_conflict() {
        for (key, value) in [
            ("rehome_copy_page_entries", "0"), ("rehome_copy_pages_per_tick", "0"),
            ("flush_interval_ms", "99"), ("compact_interval_ms", "199"),
            ("run_compact_threshold", "1"), ("compact_step_segments", "0"),
            ("maint_compact_lanes", "5"), ("cleanup_min_confirms", "0"),
            ("scan_max_inflight", "0"), ("admit_band_pct", "101"),
            ("batch_apply_gate", "0"), ("batch_apply_gate", "257"),
            ("transport_max_connections", "0"), ("transport_initial_timeout_ms", "0"),
            ("integrity_fail_fast_attempts", "1"),
            ("ae_seed_isolation", "2"), ("ae_span_regroup", "2"), ("ae_seed_duty_pct", "0"), ("ae_seed_duty_pct", "101"),
        ] {
            assert!(NodeSettingsOverrides::default().set(key, value).is_err());
            let alias = format!("VALS_{}", key.to_uppercase());
            assert!(NodeSettingsOverrides::default().resolve(|name| (name == alias).then(|| value.into())).is_err());
        }
        let s = NodeSettingsOverrides::default().resolve(|name| (name == "VALS_TRANSPORT_UNBOUNDED").then(|| "1".into())).unwrap().0;
        assert_eq!(s.transport_limits().initial_timeout, None);
        let overrides = NodeSettingsOverrides { transport_max_requests: Some(20), ..Default::default() };
        assert!(overrides.resolve(|name| (name == "VALS_TRANSPORT_UNBOUNDED").then(|| "1".into())).is_err());
    }
}
