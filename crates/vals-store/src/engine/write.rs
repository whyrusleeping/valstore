use super::*;

impl<D: BlockDevice> Engine<D> {
    pub fn put(&mut self, key: &[u8], value: &[u8], version: u128) -> Result<(), EngineError> {
        self.put_kind(key, value, version, false)
    }

    pub fn put_kind(
        &mut self,
        key: &[u8],
        value: &[u8],
        version: u128,
        tombstone: bool,
    ) -> Result<(), EngineError> {
        self.put_kind_inner(key, value, version, tombstone, false)
    }

    pub fn put_kind_authoritative(
        &mut self,
        key: &[u8],
        value: &[u8],
        version: u128,
        tombstone: bool,
    ) -> Result<AuthoritativePut, EngineError> {
        let old_version = self
            .winner_directory
            .as_ref()
            .and_then(|directory| {
                crate::attribution::record_lookup(crate::attribution::LookupCaller::Apply);
                directory.get(key)
            })
            .map(|entry| entry.version);
        if old_version.is_some_and(|old| version <= old) {
            return Ok(AuthoritativePut {
                old_version,
                written: false,
            });
        }
        self.put_kind_inner(key, value, version, tombstone, true)?;
        Ok(AuthoritativePut {
            old_version,
            written: true,
        })
    }

    fn put_kind_inner(
        &mut self,
        key: &[u8],
        value: &[u8],
        version: u128,
        tombstone: bool,
        authoritative: bool,
    ) -> Result<(), EngineError> {
        if self.poisoned {
            return Err(EngineError::Poisoned);
        }
        if !authoritative && self.index.get(key).is_some_and(|old| version < old.version) {
            return Ok(());
        }
        let epoch_before = self.log.next_epoch();
        let (seq, ptr) = self.log.append_tombstone(
            &self.dev,
            &mut self.alloc,
            RecordKind::Put,
            tombstone,
            version,
            key,
            value,
        )?;
        let candidate = IndexEntry {
            ptr,
            seq,
            version,
            tombstone,
        };
        self.index.put(key, candidate);
        if tombstone {
            self.mutable_tombstones = Some(note_mutable_tombstone(self.mutable_tombstones, version, seq));
        }
        self.bump_mutable_generation();
        if let Some(directory) = self.winner_directory.as_mut() {
            directory.insert_max(key, candidate);
        }
        if self.log.next_epoch() != epoch_before {
            self.refresh_data_reserve();
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], version: u128) -> Result<bool, EngineError> {
        if self.poisoned {
            return Err(EngineError::Poisoned);
        }
        if self.flush_in_flight {
            return Err(EngineError::FlushInFlight);
        }
        if self.index.get(key).is_none() {
            return Ok(false);
        }
        let epoch_before = self.log.next_epoch();
        self.log.append(
            &self.dev,
            &mut self.alloc,
            RecordKind::Delete,
            version,
            key,
            &[],
        )?;
        self.index.remove(key);
        self.bump_mutable_generation();
        if self.winner_directory.is_some() {
            let replacement = self.lookup_runs(key)?.map(|entry| IndexEntry {
                ptr: entry.ptr,
                seq: entry.seq,
                version: entry.version,
                tombstone: entry.tombstone,
            });
            let directory = self.winner_directory.as_mut().unwrap();
            match replacement {
                Some(entry) => {
                    directory.insert(key, entry);
                }
                None => {
                    directory.remove(key);
                }
            }
        }
        if self.log.next_epoch() != epoch_before {
            self.refresh_data_reserve();
        }
        Ok(true)
    }

}

