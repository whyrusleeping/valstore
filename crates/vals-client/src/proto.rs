pub const HELLO_MAGIC: [u8; 4] = *b"VWP1";

pub const PROTO_VERSION: u16 = 2;

pub const MIN_PROTO_VERSION: u16 = 2;

pub const FEATURE_OPAQUE_RANGE_MAP: u64 = 1 << 0;

pub const FEATURE_MIXED_BATCH_READ: u64 = 1 << 1;

pub const FEATURE_RESUMABLE_DIGESTS: u64 = 1 << 2;

pub const FEATURE_RANGE_TOMBSTONES: u64 = 1 << 3;

pub const FEATURE_MULTI_SCAN: u64 = 1 << 4;

pub const FEATURE_REVERSE_SCAN: u64 = 1 << 5;

pub const FEATURES: u64 = FEATURE_REVERSE_SCAN | FEATURE_MULTI_SCAN | FEATURE_OPAQUE_RANGE_MAP
    | FEATURE_MIXED_BATCH_READ
    | FEATURE_RESUMABLE_DIGESTS
    | FEATURE_RANGE_TOMBSTONES;

pub const HELLO_BODY_LEN: usize = 4 + 2 + 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Negotiated {
    pub version: u16,
    pub features: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HelloError {
    BadMagic,
    Malformed,
    Unsupported(u16),
}

pub fn encode_hello() -> Vec<u8> {
    let mut f = Vec::with_capacity(4 + HELLO_BODY_LEN);
    f.extend_from_slice(&(HELLO_BODY_LEN as u32).to_le_bytes());
    f.extend_from_slice(&HELLO_MAGIC);
    f.extend_from_slice(&PROTO_VERSION.to_le_bytes());
    f.extend_from_slice(&FEATURES.to_le_bytes());
    f
}

pub fn decode_hello(block: &[u8]) -> Result<(u16, u64), HelloError> {
    if block.len() != HELLO_BODY_LEN {
        return Err(HelloError::Malformed);
    }
    if block[0..4] != HELLO_MAGIC {
        return Err(HelloError::BadMagic);
    }
    let version = u16::from_le_bytes(block[4..6].try_into().unwrap());
    let features = u64::from_le_bytes(block[6..14].try_into().unwrap());
    Ok((version, features))
}

pub fn negotiate(peer_version: u16, peer_features: u64) -> Result<Negotiated, HelloError> {
    let version = PROTO_VERSION.min(peer_version);
    if version < MIN_PROTO_VERSION {
        return Err(HelloError::Unsupported(peer_version));
    }
    Ok(Negotiated {
        version,
        features: FEATURES & peer_features,
    })
}

pub fn client_handshake_blocking<S: std::io::Read + std::io::Write>(
    s: &mut S,
) -> std::io::Result<Negotiated> {
    s.write_all(&encode_hello())?;
    read_hello_blocking(s)
}

fn read_hello_blocking<S: std::io::Read>(s: &mut S) -> std::io::Result<Negotiated> {
    let invalid = |m: &str| std::io::Error::new(std::io::ErrorKind::InvalidData, m.to_string());
    let mut lenb = [0u8; 4];
    s.read_exact(&mut lenb)?;
    let len = u32::from_le_bytes(lenb) as usize;
    if len != HELLO_BODY_LEN {
        return Err(invalid("handshake: hello frame has wrong length"));
    }
    let mut block = vec![0u8; len];
    s.read_exact(&mut block)?;
    let (v, f) = decode_hello(&block).map_err(|e| invalid(&format!("handshake: {e:?}")))?;
    negotiate(v, f).map_err(|e| invalid(&format!("handshake: {e:?}")))
}

#[doc(hidden)]
pub fn server_handshake_blocking<S: std::io::Read + std::io::Write>(
    s: &mut S,
) -> std::io::Result<Negotiated> {
    let n = read_hello_blocking(s)?;
    s.write_all(&encode_hello())?;
    Ok(n)
}

#[doc(hidden)]
pub const fn test_negotiated() -> Negotiated {
    Negotiated {
        version: PROTO_VERSION,
        features: FEATURES,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hello_block(version: u16, features: u64) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&HELLO_MAGIC);
        b.extend_from_slice(&version.to_le_bytes());
        b.extend_from_slice(&features.to_le_bytes());
        b
    }

    #[test]
    fn encode_decode_roundtrip() {
        let frame = encode_hello();
        let len = u32::from_le_bytes(frame[0..4].try_into().unwrap()) as usize;
        assert_eq!(len, HELLO_BODY_LEN);
        assert_eq!(frame.len(), 4 + HELLO_BODY_LEN);
        let (v, f) = decode_hello(&frame[4..]).unwrap();
        assert_eq!(v, PROTO_VERSION);
        assert_eq!(f, FEATURES);
    }

    #[test]
    fn negotiate_equal() {
        let n = negotiate(PROTO_VERSION, FEATURES).unwrap();
        assert_eq!(n.version, PROTO_VERSION);
        assert_eq!(n.features, FEATURES);
    }

    #[test]
    fn negotiate_peer_newer_picks_ours() {
        let n = negotiate(PROTO_VERSION + 5, FEATURES | 0xF0).unwrap();
        assert_eq!(n.version, PROTO_VERSION);
        assert_eq!(n.features, FEATURES & (FEATURES | 0xF0));
    }

    #[test]
    fn negotiate_peer_too_old_refused() {
        let old = MIN_PROTO_VERSION - 1;
        assert_eq!(negotiate(old, 0), Err(HelloError::Unsupported(old)));
    }

    #[test]
    fn decode_bad_magic() {
        let mut block = hello_block(PROTO_VERSION, FEATURES);
        block[0] ^= 0xFF;
        assert_eq!(decode_hello(&block), Err(HelloError::BadMagic));
    }

    #[test]
    fn decode_truncated() {
        let block = hello_block(PROTO_VERSION, FEATURES);
        assert_eq!(
            decode_hello(&block[..HELLO_BODY_LEN - 1]),
            Err(HelloError::Malformed)
        );
    }

    #[test]
    fn decode_arbitrary_version() {
        let block = hello_block(7, 0xAB);
        assert_eq!(decode_hello(&block).unwrap(), (7, 0xAB));
    }
}
