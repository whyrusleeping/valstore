use std::collections::{BTreeSet, HashMap};

use crate::ring::NodeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ordering {
    Sorted,
    Unsorted,
}

pub type ZoneId = u32;

pub const DEFAULT_ZONE: ZoneId = 0;

pub fn ordering_to_u8(o: Ordering) -> u8 {
    match o {
        Ordering::Unsorted => 0,
        Ordering::Sorted => 1,
    }
}

pub fn ordering_from_u8(b: u8) -> Option<Ordering> {
    match b {
        0 => Some(Ordering::Unsorted),
        1 => Some(Ordering::Sorted),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZoneConfig {
    pub name: String,
    pub rf: usize,
    pub ordering: Ordering,
    pub scope: Option<BTreeSet<NodeId>>,
}

impl ZoneConfig {
    pub fn default_config(global_rf: usize) -> Self {
        ZoneConfig {
            name: "default".to_string(),
            rf: global_rf,
            ordering: Ordering::Unsorted,
            scope: None,
        }
    }

    pub fn validate(&self, members: &BTreeSet<NodeId>) -> Result<(), ZoneConfigError> {
        if self.name.is_empty()
            || self.name.trim() != self.name
            || self.name.chars().any(char::is_whitespace)
        {
            return Err(ZoneConfigError(
                "zone name must be nonempty and contain no whitespace".into(),
            ));
        }
        u32::try_from(self.name.len())
            .map_err(|_| ZoneConfigError("zone name is too long for the wire format".into()))?;
        let rf = u32::try_from(self.rf)
            .map_err(|_| ZoneConfigError("zone RF exceeds the wire-format limit".into()))?;
        if rf == 0 {
            return Err(ZoneConfigError("zone RF must be >= 1".into()));
        }
        let eligible = match &self.scope {
            None => members.len(),
            Some(scope) => {
                if scope.is_empty() {
                    return Err(ZoneConfigError("zone scope must contain at least one node".into()));
                }
                u32::try_from(scope.len()).map_err(|_| {
                    ZoneConfigError("zone scope exceeds the wire-format limit".into())
                })?;
                if let Some(node) = scope.iter().find(|node| !members.contains(node)) {
                    return Err(ZoneConfigError(format!(
                        "zone scope contains node {node}, which is not a PD member"
                    )));
                }
                scope.len()
            }
        };
        if self.rf > eligible {
            return Err(ZoneConfigError(format!(
                "zone RF {} exceeds its eligible membership of {eligible}",
                self.rf
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZoneConfigError(pub String);

impl std::fmt::Display for ZoneConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ZoneConfigError {}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Catalog {
    zones: HashMap<ZoneId, ZoneConfig>,
    by_name: HashMap<String, ZoneId>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog::default()
    }

    pub fn with_default(global_rf: usize) -> Self {
        let mut c = Catalog::new();
        c.insert(DEFAULT_ZONE, ZoneConfig::default_config(global_rf));
        c
    }

    pub fn get(&self, id: ZoneId) -> Option<&ZoneConfig> {
        self.zones.get(&id)
    }

    pub fn by_name(&self, name: &str) -> Option<ZoneId> {
        self.by_name.get(name).copied()
    }

    pub fn contains(&self, id: ZoneId) -> bool {
        self.zones.contains_key(&id)
    }

    pub fn len(&self) -> usize {
        self.zones.len()
    }

    pub fn is_empty(&self) -> bool {
        self.zones.is_empty()
    }

    pub fn next_id(&self) -> ZoneId {
        self.zones.keys().copied().max().map_or(0, |m| m + 1)
    }

    pub fn insert(&mut self, id: ZoneId, cfg: ZoneConfig) {
        if let Some(old) = self.zones.get(&id) {
            if old.name != cfg.name {
                self.by_name.remove(&old.name);
            }
        }
        self.by_name.insert(cfg.name.clone(), id);
        self.zones.insert(id, cfg);
    }

    pub fn remove(&mut self, id: ZoneId) -> Option<ZoneConfig> {
        let cfg = self.zones.remove(&id)?;
        self.by_name.remove(&cfg.name);
        Some(cfg)
    }

    pub fn iter(&self) -> impl Iterator<Item = (ZoneId, &ZoneConfig)> {
        self.zones.iter().map(|(&id, cfg)| (id, cfg))
    }

    pub fn ids(&self) -> Vec<ZoneId> {
        let mut ids: Vec<ZoneId> = self.zones.keys().copied().collect();
        ids.sort_unstable();
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_catalog_has_zone_zero() {
        let c = Catalog::with_default(3);
        assert_eq!(c.len(), 1);
        let cfg = c.get(DEFAULT_ZONE).unwrap();
        assert_eq!(cfg.rf, 3);
        assert_eq!(cfg.ordering, Ordering::Unsorted);
        assert!(cfg.scope.is_none());
        assert_eq!(c.by_name("default"), Some(DEFAULT_ZONE));
    }

    #[test]
    fn next_id_is_max_plus_one() {
        let mut c = Catalog::with_default(3);
        assert_eq!(c.next_id(), 1);
        c.insert(
            1,
            ZoneConfig {
                name: "a".into(),
                rf: 2,
                ordering: Ordering::Unsorted,
                scope: None,
            },
        );
        assert_eq!(c.next_id(), 2);
        c.insert(
            5,
            ZoneConfig {
                name: "b".into(),
                rf: 1,
                ordering: Ordering::Unsorted,
                scope: None,
            },
        );
        assert_eq!(c.next_id(), 6);
    }

    #[test]
    fn empty_catalog_starts_at_zero() {
        assert_eq!(Catalog::new().next_id(), 0);
    }

    #[test]
    fn name_lookup_and_removal_stay_consistent() {
        let mut c = Catalog::with_default(3);
        let cfg = ZoneConfig {
            name: "tenant-a".into(),
            rf: 2,
            ordering: Ordering::Unsorted,
            scope: Some([1, 2, 3].into_iter().collect()),
        };
        c.insert(1, cfg.clone());
        assert_eq!(c.by_name("tenant-a"), Some(1));
        assert_eq!(c.get(1), Some(&cfg));

        let removed = c.remove(1).unwrap();
        assert_eq!(removed.name, "tenant-a");
        assert_eq!(c.by_name("tenant-a"), None);
        assert!(!c.contains(1));
        assert!(c.contains(DEFAULT_ZONE));
    }

    #[test]
    fn zone_validation_rejects_malformed_and_lossy_configs() {
        let members: BTreeSet<NodeId> = [1, 2].into_iter().collect();
        for cfg in [
            ZoneConfig {
                name: "".into(),
                rf: 1,
                ordering: Ordering::Unsorted,
                scope: None,
            },
            ZoneConfig {
                name: "two words".into(),
                rf: 1,
                ordering: Ordering::Unsorted,
                scope: None,
            },
            ZoneConfig {
                name: "zero".into(),
                rf: 0,
                ordering: Ordering::Unsorted,
                scope: None,
            },
            ZoneConfig {
                name: "too-wide".into(),
                rf: 3,
                ordering: Ordering::Unsorted,
                scope: None,
            },
            ZoneConfig {
                name: "bad-scope".into(),
                rf: 1,
                ordering: Ordering::Unsorted,
                scope: Some([3].into_iter().collect()),
            },
        ] {
            assert!(cfg.validate(&members).is_err(), "accepted {cfg:?}");
        }

        #[cfg(target_pointer_width = "64")]
        assert!(ZoneConfig {
            name: "lossy-rf".into(),
            rf: u32::MAX as usize + 1,
            ordering: Ordering::Unsorted,
            scope: None,
        }
        .validate(&members)
        .unwrap_err()
        .to_string()
        .contains("wire-format"));
    }

    #[test]
    fn replacing_a_zone_drops_the_old_name() {
        let mut c = Catalog::new();
        c.insert(
            1,
            ZoneConfig {
                name: "old".into(),
                rf: 1,
                ordering: Ordering::Unsorted,
                scope: None,
            },
        );
        c.insert(
            1,
            ZoneConfig {
                name: "new".into(),
                rf: 1,
                ordering: Ordering::Unsorted,
                scope: None,
            },
        );
        assert_eq!(c.by_name("old"), None);
        assert_eq!(c.by_name("new"), Some(1));
        assert_eq!(c.len(), 1);
    }
}
