use super::*;

pub struct RunPin {
    _token: Arc<()>,
    epoch: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunPinState {
    Valid,
    Revoked,
    OverLimit,
}

pub struct PinnedRuns<D: BlockDevice> {
    pub pin: Option<RunPin>,
    pub sources: Vec<run::RunStreamSource<D>>,
}

#[derive(Default)]
pub(super) struct RunPins {
    live: Vec<(std::sync::Weak<()>, Vec<run::RunHandle>)>,
    retired: Vec<(run::RunHandle, u64)>,
    retired_segs: u64,
    epoch: u64,
    revocations: u64,
    limit_revocations: u64,
    revoked_by_limit: bool,
    limit_bytes: Option<u64>,
}

impl RunPins {
    fn prune(&mut self) {
        self.live.retain(|(token, _)| token.strong_count() > 0);
    }

    fn holds(&self, handle: &run::RunHandle) -> bool {
        self.live.iter().any(|(_, handles)| handles.contains(handle))
    }

    fn held(&self) -> impl Iterator<Item = &(run::RunHandle, u64)> {
        self.retired.iter().filter(|(handle, _)| {
            self.live
                .iter()
                .any(|(token, handles)| token.strong_count() > 0 && handles.contains(handle))
        })
    }

    fn held_segs(&self) -> u64 {
        self.held().map(|(handle, _)| handle.segs.len() as u64).sum()
    }
}

pub(super) fn retained_index_bytes<D: BlockDevice>(run: &run::DeviceRun<D>) -> u64 {
    if run.has_sidecar() { 0 } else { run.resident_stats().fence_bytes }
}

impl<D: BlockDevice> Engine<D> {
    pub fn pin_runs(&self, pin: bool) -> PinnedRuns<D> {
        let sources = self
            .runs
            .iter()
            .map(|or| or.run.stream_source(&self.alloc))
            .collect();
        let pin = pin.then(|| {
            let token = Arc::new(());
            let mut pins = self.run_pins.borrow_mut();
            pins.prune();
            pins.live.push((Arc::downgrade(&token), self.run_handles()));
            RunPin { _token: token, epoch: pins.epoch }
        });
        PinnedRuns { pin, sources }
    }

    pub fn run_pin_valid(&self, pin: &RunPin) -> bool {
        pin.epoch == self.run_pins.borrow().epoch
    }

    pub fn run_pin_state(&self, pin: &RunPin) -> RunPinState {
        let pins = self.run_pins.borrow();
        if pin.epoch == pins.epoch {
            RunPinState::Valid
        } else if pins.revoked_by_limit {
            RunPinState::OverLimit
        } else {
            RunPinState::Revoked
        }
    }

    pub fn set_run_pin_limit(&self, bytes: Option<u64>) {
        self.run_pins.borrow_mut().limit_bytes = bytes;
    }

    pub fn pinned_retired_bytes(&self) -> u64 {
        self.run_pins.borrow().retired_segs * self.seg_bytes_u64()
    }

    pub fn held_retired_bytes(&self) -> u64 {
        self.run_pins.borrow().held_segs() * self.seg_bytes_u64()
    }

    pub fn held_retired_index_bytes(&self) -> u64 {
        self.run_pins.borrow().held().map(|(_, index_bytes)| index_bytes).sum()
    }

    pub fn segment_in_use(&self, seg: SegmentId) -> bool {
        !self.alloc.is_free(seg)
    }

    pub fn run_pin_revocations(&self) -> u64 {
        self.run_pins.borrow().revocations
    }

    pub fn run_pin_limit_revocations(&self) -> u64 {
        self.run_pins.borrow().limit_revocations
    }

    pub fn run_pins_reapable(&self) -> bool {
        let released = {
            let pins = self.run_pins.borrow();
            pins.retired.iter().any(|(handle, _)| {
                !pins
                    .live
                    .iter()
                    .any(|(token, handles)| token.strong_count() > 0 && handles.contains(handle))
            })
        };
        released || self.run_pin_pressure()
    }

    fn seg_bytes_u64(&self) -> u64 {
        self.alloc.blocks_per_segment() * self.block_size as u64
    }

    pub(super) fn extend_pinned_segments(&self, keep: &mut std::collections::HashSet<SegmentId>) {
        for (handle, _) in &self.run_pins.borrow().retired {
            keep.extend(handle.segs.iter().copied());
        }
    }

    pub(super) fn has_pinned_retired_runs(&self) -> bool {
        !self.run_pins.borrow().retired.is_empty()
    }

    fn run_pin_pressure(&self) -> bool {
        #[cfg(test)]
        if self.test_run_pin_pressure {
            return true;
        }
        let retired = self.run_pins.borrow().retired_segs;
        retired != 0
            && (self.admission_shed_probability() > 0.0
                || self.at_maintenance_floor()
                || retired >= self.alloc.free_segments())
    }

    pub(super) fn retire_removed_runs(&mut self, removed: Vec<(run::RunHandle, u64)>) -> usize {
        let mut freed = 0usize;
        {
            let pins = self.run_pins.get_mut();
            pins.prune();
            for (handle, index_bytes) in removed {
                if pins.holds(&handle) {
                    pins.retired_segs += handle.segs.len() as u64;
                    pins.retired.push((handle, index_bytes));
                    continue;
                }
                for seg in &handle.segs {
                    self.alloc
                        .free(*seg)
                        .expect("retire_removed_runs: freeing a removed run segment");
                    freed += 1;
                }
            }
        }
        let seg_bytes = self.seg_bytes_u64();
        let over_limit = {
            let pins = self.run_pins.get_mut();
            pins.limit_bytes.is_some_and(|limit| pins.held_segs() * seg_bytes > limit)
        };
        if self.run_pin_pressure() {
            freed += self.revoke_pins(false);
        } else if over_limit {
            freed += self.revoke_pins(true);
        }
        freed
    }

    pub fn reap_run_pins(&mut self) -> usize {
        if self.run_pin_pressure() {
            let freed = self.revoke_pins(false);
            self.after_pinned_free(freed);
            return freed;
        }
        let mut released: Vec<SegmentId> = Vec::new();
        {
            let pins = self.run_pins.get_mut();
            pins.prune();
            let mut kept = Vec::with_capacity(pins.retired.len());
            for (handle, index_bytes) in std::mem::take(&mut pins.retired) {
                if pins.holds(&handle) {
                    kept.push((handle, index_bytes));
                } else {
                    pins.retired_segs -= handle.segs.len() as u64;
                    released.extend(handle.segs);
                }
            }
            pins.retired = kept;
        }
        for seg in &released {
            self.alloc
                .free(*seg)
                .expect("reap_run_pins: freeing a pinned run segment");
        }
        self.after_pinned_free(released.len());
        released.len()
    }

    pub fn revoke_run_pins(&mut self) -> usize {
        let freed = self.revoke_pins(false);
        self.after_pinned_free(freed);
        freed
    }

    fn revoke_pins(&mut self, by_limit: bool) -> usize {
        let retired = {
            let pins = self.run_pins.get_mut();
            pins.epoch += 1;
            if by_limit {
                pins.limit_revocations += 1;
            } else {
                pins.revocations += 1;
            }
            pins.revoked_by_limit = by_limit;
            pins.live.clear();
            pins.retired_segs = 0;
            std::mem::take(&mut pins.retired)
        };
        let mut freed = 0usize;
        for (handle, _) in retired {
            for seg in handle.segs {
                self.alloc
                    .free(seg)
                    .expect("revoke_run_pins: freeing a pinned run segment");
                freed += 1;
            }
        }
        freed
    }

    fn after_pinned_free(&mut self, freed: usize) {
        if freed > 0 {
            self.bump_storage_generation();
            self.refresh_data_reserve();
        }
    }

    pub(super) fn forget_run_pins_on_truncate(&mut self) {
        let pins = self.run_pins.get_mut();
        pins.epoch += 1;
        pins.revoked_by_limit = false;
        pins.live.clear();
        pins.retired.clear();
        pins.retired_segs = 0;
    }

    #[cfg(test)]
    pub(super) fn set_run_pin_pressure_for_test(&mut self, on: bool) {
        self.test_run_pin_pressure = on;
    }
}
