use super::push::*;

fn parse_bool(value: &str) -> Result<bool, String> {
    match value {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        _ => Err(format!("expected true/false, yes/no, or 1/0, got {value:?}")),
    }
}

fn parse_int<T: std::str::FromStr>(value: &str) -> Result<T, String> {
    value.parse().map_err(|_| format!("expected a representable unsigned integer, got {value:?}"))
}

macro_rules! placement_settings {
    ($($field:ident: $ty:ty = $default:expr, $env:literal, $parse:ident, $valid:expr, $requirement:literal;)*) => {
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct PdPlacementSettings {
            $(pub $field: $ty,)*
        }

        impl Default for PdPlacementSettings {
            fn default() -> Self {
                Self { $($field: $default,)* }
            }
        }

        #[derive(Clone, Debug, Default, PartialEq, Eq)]
        pub struct PdPlacementOverrides {
            $(pub $field: Option<$ty>,)*
        }

        impl PdPlacementSettings {
            fn validate_fields(&self) -> Result<(), String> {
                $(if !($valid)(self.$field) {
                    return Err(format!("`{}` ({}) {}", stringify!($field), $env, $requirement));
                })*
                Ok(())
            }

            pub fn validate(&self) -> Result<(), String> {
                self.validate_fields()?;
                if self.balance_low_pct > self.balance_high_pct {
                    return Err("`balance_low_pct` (VALS_PD_BALANCE_LOW_PCT) must be <= `balance_high_pct` (VALS_PD_BALANCE_HIGH_PCT)".into());
                }
                Ok(())
            }
        }

        impl PdPlacementOverrides {
            pub const KEYS: &'static [&'static str] = &[$(stringify!($field),)*];

            pub fn set(&mut self, key: &str, value: &str) -> Result<(), String> {
                match key {
                    $(stringify!($field) => {
                        let parsed: $ty = $parse(value).map_err(|e| format!("`{key}`: {e}"))?;
                        if !($valid)(parsed) {
                            return Err(format!("`{key}` {}", $requirement));
                        }
                        self.$field = Some(parsed);
                    })*
                    _ => return Err(format!("unknown PD placement key `{key}`")),
                }
                Ok(())
            }

            pub fn validate(&self) -> Result<(), String> {
                let mut settings = PdPlacementSettings::default();
                $(if let Some(value) = self.$field { settings.$field = value; })*
                settings.validate_fields()?;
                if let (Some(low), Some(high)) = (self.balance_low_pct, self.balance_high_pct) {
                    if low > high {
                        return Err("`balance_low_pct` must be <= `balance_high_pct`".into());
                    }
                }
                Ok(())
            }

            pub fn resolve(
                &self,
                lookup: impl Fn(&str) -> Option<String>,
            ) -> Result<(PdPlacementSettings, Vec<&'static str>), String> {
                self.validate()?;
                let mut settings = PdPlacementSettings::default();
                let mut legacy = Vec::new();
                $(let env_value: Option<$ty> = lookup($env).map(|value| {
                    let parsed: $ty = $parse(&value).map_err(|e| format!("{} (config key `{}`): {e}", $env, stringify!($field)))?;
                    if !($valid)(parsed) {
                        return Err(format!("{} (config key `{}`) {}", $env, stringify!($field), $requirement));
                    }
                    Ok(parsed)
                }).transpose()?;
                settings.$field = match (self.$field, env_value) {
                    (Some(config), Some(env)) if config != env => {
                        return Err(format!("conflicting `{}`={config} and {}={env}; remove the legacy environment variable or make both values equal", stringify!($field), $env));
                    }
                    (Some(config), _) => config,
                    (None, Some(env)) => { legacy.push($env); env }
                    (None, None) => settings.$field,
                };)*
                settings.validate()?;
                Ok((settings, legacy))
            }
        }
    };
}

placement_settings! {
    range_split_threshold: u64 = DEFAULT_RANGE_SPLIT_THRESHOLD, "VALS_PD_RANGE_SPLIT_THRESHOLD", parse_int, |_: u64| true, "must be an unsigned integer";
    range_load_split_ops: u64 = DEFAULT_RANGE_LOAD_SPLIT_OPS, "VALS_PD_RANGE_LOAD_SPLIT_OPS", parse_int, |_: u64| true, "must be an unsigned integer";
    range_split_poll_ms: u64 = DEFAULT_RANGE_SPLIT_POLL_MS, "VALS_PD_RANGE_SPLIT_POLL_MS", parse_int, |v| v > 0, "must be > 0";
    range_splits_per_poll: usize = DEFAULT_RANGE_SPLITS_PER_POLL, "VALS_PD_RANGE_SPLITS_PER_POLL", parse_int, |v| v > 0, "must be > 0";
    range_merge_threshold: u64 = DEFAULT_RANGE_MERGE_THRESHOLD, "VALS_PD_RANGE_MERGE_THRESHOLD", parse_int, |_: u64| true, "must be an unsigned integer";
    range_merge_covered: bool = true, "VALS_PD_RANGE_MERGE_COVERED", parse_bool, |_: bool| true, "must be a boolean";
    single_range_warn_entries: u64 = DEFAULT_SINGLE_RANGE_WARN_ENTRIES, "VALS_PD_SINGLE_RANGE_WARN_ENTRIES", parse_int, |_: u64| true, "must be an unsigned integer";
    range_concentration_warn_pct: u64 = DEFAULT_RANGE_CONCENTRATION_WARN_PCT, "VALS_PD_RANGE_CONCENTRATION_WARN_PCT", parse_int, |_: u64| true, "must be an unsigned integer";
    rehome_enabled: bool = true, "VALS_PD_REHOME_ENABLED", parse_bool, |_: bool| true, "must be a boolean";
    max_concurrent_rehomes: usize = 1, "VALS_PD_MAX_CONCURRENT_REHOMES", parse_int, |v| v > 0, "must be > 0";
    rehome_report_deadline_ms: u64 = 60_000, "VALS_PD_REHOME_REPORT_DEADLINE_MS", parse_int, |_: u64| true, "must be an unsigned integer";
    rehome_recovery_defer_ms: u64 = 1_800_000, "VALS_PD_REHOME_RECOVERY_DEFER_MS", parse_int, |_: u64| true, "must be an unsigned integer";
    opaque_range_maps: bool = false, "VALS_PD_OPAQUE_RANGE_MAPS", parse_bool, |_: bool| true, "must be a boolean";
    balance_execute: bool = false, "VALS_PD_BALANCE_EXECUTE", parse_bool, |_: bool| true, "must be a boolean";
    balance_high_pct: u64 = DEFAULT_BALANCE_HIGH_PCT, "VALS_PD_BALANCE_HIGH_PCT", parse_int, |v| v >= 100, "must be >= 100";
    balance_low_pct: u64 = DEFAULT_BALANCE_LOW_PCT, "VALS_PD_BALANCE_LOW_PCT", parse_int, |v| v >= 100, "must be >= 100";
    balance_min_improvement_pct: u64 = DEFAULT_BALANCE_MIN_IMPROVEMENT_PCT, "VALS_PD_BALANCE_MIN_IMPROVEMENT_PCT", parse_int, |_: u64| true, "must be an unsigned integer";
    balance_token_search_limit: u64 = DEFAULT_BALANCE_TOKEN_SEARCH_LIMIT, "VALS_PD_BALANCE_TOKEN_SEARCH_LIMIT", parse_int, |v| v > 0, "must be > 0";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_pusher_defaults() {
        let (settings, legacy) = PdPlacementOverrides::default().resolve(|_| None).unwrap();
        assert!(legacy.is_empty());
        assert_eq!(settings, PdPlacementSettings::default());
        assert_eq!(settings.range_split_threshold, 1_000_000);
        assert_eq!(settings.range_load_split_ops, 0);
        assert_eq!(settings.range_split_poll_ms, 5000);
        assert_eq!(settings.range_splits_per_poll, 16);
        assert_eq!(settings.range_merge_threshold, 0);
        assert!(settings.range_merge_covered);
        assert_eq!(settings.single_range_warn_entries, 1_000_000);
        assert_eq!(settings.range_concentration_warn_pct, 80);
        assert!(settings.rehome_enabled);
        assert_eq!(settings.max_concurrent_rehomes, 1);
        assert_eq!(settings.rehome_report_deadline_ms, 60_000);
        assert_eq!(settings.rehome_recovery_defer_ms, 1_800_000);
        assert!(!settings.opaque_range_maps);
        assert!(!settings.balance_execute);
        assert_eq!(settings.balance_high_pct, 120);
        assert_eq!(settings.balance_low_pct, 105);
        assert_eq!(settings.balance_min_improvement_pct, 5);
        assert_eq!(settings.balance_token_search_limit, 4096);
    }

    #[test]
    fn resolution_precedence_and_validation() {
        let env = |value: &str| {
            let value = value.to_string();
            move |name: &str| (name == "VALS_PD_REHOME_ENABLED").then(|| value.clone())
        };
        let overrides = PdPlacementOverrides { rehome_enabled: Some(false), ..Default::default() };
        assert!(!overrides.resolve(|_| None).unwrap().0.rehome_enabled);
        let (settings, legacy) = PdPlacementOverrides::default().resolve(env("0")).unwrap();
        assert!(!settings.rehome_enabled);
        assert_eq!(legacy, ["VALS_PD_REHOME_ENABLED"]);
        assert!(overrides.resolve(env("no")).unwrap().1.is_empty());
        let conflict = overrides.resolve(env("yes")).unwrap_err();
        assert!(conflict.contains("`rehome_enabled`"));
        assert!(conflict.contains("VALS_PD_REHOME_ENABLED"));
        assert!(overrides.resolve(env("typo")).unwrap_err().contains("expected true/false"));
        for (name, value) in [
            ("VALS_PD_RANGE_SPLIT_POLL_MS", "0"),
            ("VALS_PD_RANGE_SPLITS_PER_POLL", "0"),
            ("VALS_PD_MAX_CONCURRENT_REHOMES", "0"),
            ("VALS_PD_BALANCE_HIGH_PCT", "99"),
            ("VALS_PD_BALANCE_LOW_PCT", "99"),
            ("VALS_PD_BALANCE_TOKEN_SEARCH_LIMIT", "0"),
            ("VALS_PD_RANGE_SPLIT_THRESHOLD", "-1"),
            ("VALS_PD_RANGE_LOAD_SPLIT_OPS", "18446744073709551616"),
        ] {
            let error = PdPlacementOverrides::default().resolve(|key| (key == name).then(|| value.into())).unwrap_err();
            assert!(error.contains(name), "{error}");
        }
        let overrides = PdPlacementOverrides { balance_high_pct: Some(100), ..Default::default() };
        assert!(overrides.resolve(|_| None).unwrap_err().contains("balance_low_pct"));
        assert!(overrides.resolve(|key| (key == "VALS_PD_BALANCE_LOW_PCT").then(|| "100".into())).is_ok());
    }
}
