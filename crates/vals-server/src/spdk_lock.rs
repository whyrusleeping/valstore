#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;

pub struct DeviceLocks {
    _files: Vec<File>,
}

impl DeviceLocks {
    pub fn acquire(pci_addrs: &[String]) -> Result<Self, std::io::Error> {
        let mut names = pci_addrs.to_vec();
        names.sort();
        names.dedup();
        if names.len() != pci_addrs.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "configured SPDK PCI identities contain a duplicate",
            ));
        }
        let mut files = Vec::with_capacity(names.len());
        for pci in names {
            let encoded = pci
                .as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let path = format!("/run/lock/valsd-spdk-{encoded}.lock");
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .mode(0o600)
                .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
                .open(&path)
                .map_err(|e| std::io::Error::new(e.kind(), format!("open {path}: {e}")))?;
            // SAFETY: flock only borrows this valid file descriptor for the call.
            let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if rc != 0 {
                let e = std::io::Error::last_os_error();
                return Err(std::io::Error::new(
                    e.kind(),
                    format!(
                        "exclusive ownership of SPDK device {pci} is unavailable ({path}: {e})"
                    ),
                ));
            }
            files.push(file);
        }
        Ok(DeviceLocks { _files: files })
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn a_second_owner_cannot_lock_the_same_device() {
        let pci = vec![format!("test-only-{}", std::process::id())];
        let first = DeviceLocks::acquire(&pci).unwrap();
        assert!(DeviceLocks::acquire(&pci).is_err());
        drop(first);
        DeviceLocks::acquire(&pci).unwrap();
    }
}
