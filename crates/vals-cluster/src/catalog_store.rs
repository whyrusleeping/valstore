use std::path::PathBuf;

use crate::ring::NodeId;
use crate::zone::{ordering_from_u8, ordering_to_u8, Catalog, ZoneConfig, ZoneId};

pub trait CatalogStore: Send {
    fn load(&self) -> Option<Catalog>;
    fn store(&self, catalog: &Catalog) -> std::io::Result<()>;
}

const CAT_MAGIC: u32 = 0x5443_4156;
const CAT_VERSION: u16 = 1;

fn encode_catalog(catalog: &Catalog) -> Vec<u8> {
    let mut entries: Vec<(ZoneId, &ZoneConfig)> = catalog.iter().collect();
    entries.sort_unstable_by_key(|(id, _)| *id);

    let mut b = Vec::new();
    b.extend_from_slice(&CAT_MAGIC.to_le_bytes());
    b.extend_from_slice(&CAT_VERSION.to_le_bytes());
    b.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (id, cfg) in entries {
        b.extend_from_slice(&id.to_le_bytes());
        b.extend_from_slice(&(cfg.rf as u32).to_le_bytes());
        b.push(ordering_to_u8(cfg.ordering));
        let name = cfg.name.as_bytes();
        b.extend_from_slice(&(name.len() as u32).to_le_bytes());
        b.extend_from_slice(name);
        match &cfg.scope {
            None => b.push(0),
            Some(set) => {
                b.push(1);
                b.extend_from_slice(&(set.len() as u32).to_le_bytes());
                for n in set {
                    b.extend_from_slice(&n.to_le_bytes());
                }
            }
        }
    }
    let crc = vals_store::crc::crc32(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    b
}

struct Cursor<'a> {
    b: &'a [u8],
    off: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], &'static str> {
        let end = self.off.checked_add(n).ok_or("length overflow")?;
        let s = self.b.get(self.off..end).ok_or("truncated")?;
        self.off = end;
        Ok(s)
    }
    fn u32(&mut self) -> Result<u32, &'static str> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u16(&mut self) -> Result<u16, &'static str> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u8(&mut self) -> Result<u8, &'static str> {
        Ok(self.take(1)?[0])
    }
}

fn decode_catalog(bytes: &[u8]) -> Result<Catalog, &'static str> {
    if bytes.len() < 4 {
        return Err("truncated");
    }
    let body = &bytes[..bytes.len() - 4];
    let stored = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
    if vals_store::crc::crc32(body) != stored {
        return Err("crc mismatch");
    }

    let mut c = Cursor { b: body, off: 0 };
    if c.u32()? != CAT_MAGIC {
        return Err("bad magic");
    }
    if c.u16()? != CAT_VERSION {
        return Err("unsupported version");
    }
    let count = c.u32()? as usize;
    let mut catalog = Catalog::new();
    for _ in 0..count {
        let id = c.u32()?;
        let rf = c.u32()? as usize;
        let ordering = ordering_from_u8(c.u8()?).ok_or("bad ordering byte")?;
        let name_len = c.u32()? as usize;
        let name = std::str::from_utf8(c.take(name_len)?)
            .map_err(|_| "non-utf8 zone name")?
            .to_string();
        let scope = match c.u8()? {
            0 => None,
            1 => {
                let n = c.u32()? as usize;
                let mut set = std::collections::BTreeSet::new();
                for _ in 0..n {
                    set.insert(c.u32()? as NodeId);
                }
                Some(set)
            }
            _ => return Err("bad scope tag"),
        };
        catalog.insert(
            id,
            ZoneConfig {
                name,
                rf,
                ordering,
                scope,
            },
        );
    }
    if c.off != body.len() {
        return Err("trailing bytes");
    }
    Ok(catalog)
}

pub struct FileCatalogStore {
    path: PathBuf,
}

impl FileCatalogStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        FileCatalogStore { path: path.into() }
    }
}

impl CatalogStore for FileCatalogStore {
    fn load(&self) -> Option<Catalog> {
        let bytes = std::fs::read(&self.path).ok()?;
        match decode_catalog(&bytes) {
            Ok(cat) => Some(cat),
            Err(why) => {
                log::warn!(
                    "ignoring corrupt persisted zone catalog at {}: {why} \
                     (falling back to lazy zone materialization)",
                    self.path.display()
                );
                None
            }
        }
    }

    fn store(&self, catalog: &Catalog) -> std::io::Result<()> {
        use std::io::Write;
        let tmp = self.path.with_extension("tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&encode_catalog(catalog))?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        if let Some(dir) = self.path.parent() {
            if let Ok(d) = std::fs::File::open(dir) {
                let _ = d.sync_all();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zone::Ordering;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    fn temp_path(tag: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, AtomicOrdering::Relaxed);
        std::env::temp_dir().join(format!("vals-catalog-{}-{}-{}", std::process::id(), tag, n))
    }

    fn sample() -> Catalog {
        let mut c = Catalog::with_default(2);
        c.insert(
            1,
            ZoneConfig {
                name: "orders".to_string(),
                rf: 3,
                ordering: Ordering::Sorted,
                scope: None,
            },
        );
        c.insert(
            7,
            ZoneConfig {
                name: "tenant-x".to_string(),
                rf: 2,
                ordering: Ordering::Unsorted,
                scope: Some([2u32, 5, 9].into_iter().collect()),
            },
        );
        c
    }

    #[test]
    fn roundtrips_through_a_file() {
        let path = temp_path("roundtrip");
        let store = FileCatalogStore::new(&path);
        assert!(store.load().is_none(), "absent file loads as None");

        let cat = sample();
        store.store(&cat).unwrap();
        let loaded = store.load().expect("present");
        assert_eq!(loaded, cat, "reload must equal what was persisted");

        assert_eq!(loaded.get(1).unwrap().ordering, Ordering::Sorted);
        assert_eq!(loaded.get(7).unwrap().ordering, Ordering::Unsorted);
        assert_eq!(
            loaded.get(7).unwrap().scope,
            Some([2u32, 5, 9].into_iter().collect())
        );
    }

    #[test]
    fn corrupt_file_loads_as_none_not_panic() {
        let path = temp_path("corrupt");
        let store = FileCatalogStore::new(&path);
        store.store(&sample()).unwrap();

        let mut bytes = std::fs::read(&path).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        assert!(store.load().is_none());

        std::fs::write(&path, &bytes[..3]).unwrap();
        assert!(store.load().is_none());
    }

    #[test]
    fn empty_catalog_roundtrips() {
        let path = temp_path("empty");
        let store = FileCatalogStore::new(&path);
        store.store(&Catalog::new()).unwrap();
        assert_eq!(store.load().unwrap(), Catalog::new());
    }
}
