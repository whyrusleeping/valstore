use std::sync::OnceLock;

static SECRET: OnceLock<Box<[u8]>> = OnceLock::new();

pub fn init_cluster_secret(secret: impl AsRef<[u8]>) -> bool {
    let s = secret.as_ref();
    assert!(!s.is_empty(), "cluster control secret must be non-empty");
    SECRET.set(s.into()).is_ok()
}

pub(crate) fn token() -> &'static [u8] {
    SECRET.get().map(|b| &b[..]).unwrap_or(&[])
}

pub(crate) fn verify(token: &[u8]) -> bool {
    match SECRET.get() {
        None => true,
        Some(s) => ct_eq(s, token),
    }
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= (x ^ y) as usize;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ct_eq_matches_equality() {
        assert!(ct_eq(b"", b""));
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"ab"));
        assert!(!ct_eq(b"", b"a"));
        assert!(!ct_eq(b"\0\0", b"\0"));
    }

    #[test]
    fn unconfigured_gate_is_open() {
        assert!(verify(b""));
        assert!(verify(b"anything"));
    }
}
