use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RingConfig {
    pub vnodes: u32,
    pub rf: u32,
}

impl Default for RingConfig {
    fn default() -> Self {
        RingConfig {
            vnodes: crate::ring::DEFAULT_VNODES,
            rf: 3,
        }
    }
}

pub type AddressedMembers = Vec<(NodeId, u32, Vec<SocketAddr>)>;

pub struct PlacementDriver<D: BlockDevice> {
    pub(super) partition: Partition<D>,
    validation_members: AddressedMembers,
    validation_zones: Catalog,
    validation_next_zone_id: ZoneId,
    validation_global_rf: usize,
    state_index: StateIndex,
}

impl<D: BlockDevice> PlacementDriver<D> {
    pub(super) fn from_partition(partition: Partition<D>) -> Result<Self, PartitionError> {
        let mut driver = PlacementDriver {
            partition,
            validation_members: Vec::new(),
            validation_zones: Catalog::new(),
            validation_next_zone_id: DEFAULT_ZONE + 1,
            validation_global_rf: 1,
            state_index: StateIndex::default(),
        };
        driver.sync_state_index();
        driver.refresh_validation_state();
        let members = driver.validation_member_ids();
        let mut names = BTreeSet::new();
        for (zone_id, cfg) in driver.validation_zones.iter() {
            cfg.validate(&members).map_err(|error| {
                Self::invariant(format!(
                    "persisted zone {zone_id} ({:?}) is invalid: {error}",
                    cfg.name
                ))
            })?;
            if !names.insert(&cfg.name) {
                return Err(Self::invariant(format!(
                    "persisted catalog contains duplicate zone name {:?}",
                    cfg.name
                )));
            }
        }
        Ok(driver)
    }

    fn refresh_validation_state(&mut self) {
        self.validation_members = self.members_addressed();
        self.validation_zones = self.zones();
        self.validation_next_zone_id = self.next_zone_id();
    }

    fn prepare_authoritative_mutation(&mut self) -> Result<(), PartitionError> {
        if !self.partition.is_leader() {
            return Err(PartitionError::NotLeader);
        }
        if !self.partition.is_caught_up_leader() {
            return Err(PartitionError::NotReady);
        }
        self.refresh_validation_state();
        Ok(())
    }

    fn validation_member_ids(&self) -> BTreeSet<NodeId> {
        self.validation_members.iter().map(|(node, _, _)| *node).collect()
    }

    fn invariant(message: impl Into<String>) -> PartitionError {
        PartitionError::Invariant(message.into())
    }

    pub(super) fn set_global_rf(&mut self, rf: usize) -> Result<(), PartitionError> {
        self.refresh_validation_state();
        if rf == 0 {
            return Err(Self::invariant("global RF must be >= 1"));
        }
        if let Some(recorded) = self.ring_rf().filter(|&recorded| recorded as usize != rf) {
            return Err(Self::invariant(format!(
                "this PD's committed state records ring rf {recorded}, but its config sets rf = {rf}; \
                 an existing cluster cannot change rf: set rf = {recorded} again \
                 (docs/growing.md#replication-factor)"
            )));
        }
        if !self.validation_members.is_empty() && self.validation_members.len() < rf {
            return Err(Self::invariant(format!(
                "global RF {rf} exceeds persisted membership of {}",
                self.validation_members.len()
            )));
        }
        self.validation_global_rf = rf;
        Ok(())
    }

    pub(super) fn global_rf(&self) -> usize {
        self.validation_global_rf
    }

    pub fn new_single(id: u64, engine: Engine<D>) -> Result<Self, PartitionError> {
        Self::from_partition(Partition::new_single(id, engine)?)
    }

    pub fn open_single(
        id: u64,
        engine: Engine<D>,
        dir: impl AsRef<std::path::Path>,
    ) -> Result<Self, PartitionError> {
        let mut partition = Partition::open(id, vec![id], engine, dir)?;
        partition.drive()?;
        partition.campaign()?;
        Self::from_partition(partition)
    }

    pub fn new(id: u64, voters: Vec<u64>, engine: Engine<D>) -> Result<Self, PartitionError> {
        Self::from_partition(Partition::new(id, voters, engine)?)
    }

    pub fn open(
        id: u64,
        voters: Vec<u64>,
        engine: Engine<D>,
        dir: impl AsRef<std::path::Path>,
    ) -> Result<Self, PartitionError> {
        let mut partition = Partition::open(id, voters, engine, dir)?;
        partition.drive()?;
        Self::from_partition(partition)
    }

    pub fn id(&self) -> u64 {
        self.partition.id()
    }

    pub fn campaign(&mut self) -> Result<(), PartitionError> {
        self.partition.campaign()
    }

    pub fn step(&mut self, msg: Message) -> Result<(), PartitionError> {
        self.partition.step(msg)
    }

    pub fn take_outgoing(&mut self) -> Vec<Message> {
        self.partition.take_outgoing()
    }

    pub fn add_node(&mut self, node: NodeId) -> Result<(), PartitionError> {
        self.add_node_weighted(node, 1)
    }

    pub fn add_node_weighted(&mut self, node: NodeId, weight: u32) -> Result<(), PartitionError> {
        self.add_node_addressed(node, weight, &[])
    }

    pub fn add_node_addressed(
        &mut self,
        node: NodeId,
        weight: u32,
        addrs: &[SocketAddr],
    ) -> Result<(), PartitionError> {
        self.prepare_authoritative_mutation()?;
        if self.validation_members.is_empty() && self.validation_global_rf > 1 {
            return Err(Self::invariant(format!(
                "initial membership for global RF {} must be installed atomically",
                self.validation_global_rf
            )));
        }
        Self::validate_member(weight, addrs)?;
        let value = encode_member_value(weight, addrs);
        self.partition
            .propose(&Command::Put(node.to_le_bytes().to_vec(), value))
    }

    fn validate_member(weight: u32, addrs: &[SocketAddr]) -> Result<(), PartitionError> {
        if weight == 0 {
            return Err(Self::invariant("member weight must be >= 1"));
        }
        u32::try_from(addrs.len())
            .map_err(|_| Self::invariant("member address list exceeds the wire-format limit"))?;
        Ok(())
    }

    pub fn bootstrap_members(&mut self, members: &AddressedMembers) -> Result<(), PartitionError> {
        self.prepare_authoritative_mutation()?;
        if !self.validation_members.is_empty() {
            if self.validation_members == *members {
                return Ok(());
            }
            return Err(Self::invariant(
                "initial membership can only be installed into an empty catalog",
            ));
        }
        if members.len() < self.validation_global_rf {
            return Err(Self::invariant(format!(
                "global RF {} exceeds bootstrap membership of {}",
                self.validation_global_rf,
                members.len()
            )));
        }
        let mut ids = BTreeSet::new();
        let mut writes = Vec::with_capacity(members.len());
        for (node, weight, addrs) in members {
            if !ids.insert(*node) {
                return Err(Self::invariant(format!(
                    "bootstrap membership contains duplicate node {node}"
                )));
            }
            Self::validate_member(*weight, addrs)?;
            writes.push((
                node.to_le_bytes().to_vec(),
                Some(encode_member_value(*weight, addrs)),
            ));
        }
        self.partition.propose(&Command::ConditionalBatch {
            checks: writes.iter().map(|(key, _)| (key.clone(), None)).collect(),
            writes,
        })
    }

    pub fn remove_node(&mut self, node: NodeId) -> Result<(), PartitionError> {
        self.prepare_authoritative_mutation()?;
        if !self
            .validation_members
            .iter()
            .any(|(member, _, _)| *member == node)
        {
            return Err(Self::invariant(format!("node {node} is not a member")));
        }
        let mut remaining = self.validation_member_ids();
        remaining.remove(&node);
        if remaining.len() < self.validation_global_rf {
            return Err(Self::invariant(format!(
                "cannot remove node {node}: global RF {} would exceed membership of {}",
                self.validation_global_rf,
                remaining.len()
            )));
        }
        for (zone_id, cfg) in self.validation_zones.iter() {
            cfg.validate(&remaining).map_err(|error| {
                Self::invariant(format!(
                    "cannot remove node {node}: zone {zone_id} ({:?}) would be invalid: {error}",
                    cfg.name
                ))
            })?;
        }
        self.partition
            .propose(&Command::Delete(node.to_le_bytes().to_vec()))
    }

    pub fn tick(&mut self) {
        self.partition.tick();
    }

    pub fn drive(&mut self) -> Result<(), PartitionError> {
        let result = self.partition.drive();
        if result.is_ok() {
            self.sync_state_index();
        } else {
            self.state_index = StateIndex::default();
        }
        result
    }

    fn sync_state_index(&mut self) {
        let changes = self.partition.take_state_changes();
        let partition = &self.partition;
        let get = |key: &[u8]| partition.state_get(key).ok().flatten();
        match changes {
            Some(keys) if self.state_index.applied.is_some() => self.state_index.update(keys, &get),
            _ => self.state_index = StateIndex::build(partition.state_keys(), &get),
        }
        self.state_index.applied = Some(partition.applied_index());
    }

    fn with_state_index<R>(&self, read: impl FnOnce(&StateIndex) -> R) -> R {
        if self.state_index.applied == Some(self.partition.applied_index()) {
            return read(&self.state_index);
        }
        read(&StateIndex::build(self.partition.state_keys(), &|key| self.state_value(key)))
    }

    #[cfg(test)]
    pub(super) fn assert_state_index_fresh(&self) {
        let rebuilt = StateIndex::build(self.partition.state_keys(), &|key| self.state_value(key));
        let index = &self.state_index;
        assert_eq!(index.applied, Some(self.partition.applied_index()), "state index is behind");
        assert_eq!(index.control_keys, rebuilt.control_keys);
        assert_eq!(index.zones.keys().collect::<Vec<_>>(), rebuilt.zones.keys().collect::<Vec<_>>());
        for (zone, state) in &index.zones {
            let fresh = &rebuilt.zones[zone];
            assert_eq!((&state.meta, &state.legacy, &state.records), (&fresh.meta, &fresh.legacy, &fresh.records));
            assert_eq!(state.map, fresh.map, "zone {zone} cached range map");
        }
    }

    fn control_keys(&self) -> Vec<Vec<u8>> {
        self.with_state_index(|index| index.control_keys.iter().cloned().collect())
    }

    pub fn epoch(&self) -> u64 {
        self.partition.applied_index()
    }

    pub(super) fn last_index(&self) -> u64 {
        self.partition.last_index()
    }

    pub(super) fn status(&self) -> PdStatus {
        let (voters, learners) = self.pd_voters();
        PdStatus {
            id: self.id(),
            role: self.partition.role_name(),
            term: self.partition.term(),
            leader: self.partition.leader_id(),
            commit: self.partition.commit_index(),
            applied: self.epoch(),
            caught_up: self.is_caught_up_leader(),
            incarnation: self.incarnation(),
            voters,
            learners,
        }
    }

    pub fn pd_voters(&self) -> (Vec<u64>, Vec<u64>) {
        let cs = self.partition.conf_state();
        let (mut voters, mut learners) = (cs.voters, cs.learners);
        voters.sort_unstable();
        learners.sort_unstable();
        (voters, learners)
    }

    pub fn pd_peer_record(&self, id: u64) -> Option<PdPeerRecord> {
        self.state_value(&pd_peer_key(id)).and_then(|value| decode_pd_peer(&value))
    }

    fn propose_pd_change(
        &mut self,
        change: vals_raft::ConfChangeType,
        id: u64,
        record: (u64, PdPeerRecord),
    ) -> Result<(), PartitionError> {
        let context = Command::Put(pd_peer_key(record.0), encode_pd_peer(&record.1));
        self.partition.propose_conf_change(change, id, Some(&context)).map(|_| ())
    }

    fn reserved_learner_of(&self, learner: u64) -> Option<u64> {
        self.control_keys().into_iter().find_map(|key| {
            let id = pd_peer_id_of_key(&key)?;
            (self.pd_peer_record(id) == Some(PdPeerRecord::Retired { replaced_by: learner })).then_some(id)
        })
    }

    fn check_learner_caught_up(&self, learner: u64) -> Result<(), PartitionError> {
        let matched = self.partition.peer_matched(learner).unwrap_or(0);
        let commit = self.partition.commit_index();
        if matched == 0 || matched.saturating_add(LEARNER_CATCH_UP_SLACK) < commit {
            return Err(Self::invariant(format!(
                "learner {learner} has not caught up: it holds the log through index {matched} and \
                 the group has committed {commit}; check that it runs and is reachable, then retry"
            )));
        }
        Ok(())
    }

    fn prepare_pd_membership_change(&mut self) -> Result<(Vec<u64>, Vec<u64>), PartitionError> {
        self.prepare_authoritative_mutation()?;
        let (voters, learners) = self.pd_voters();
        if voters.len() < 2 {
            return Err(Self::invariant(
                "PD membership changes need a multi-PD raft group; a single PD is not grown into one",
            ));
        }
        if self.partition.has_pending_conf() {
            return Err(PartitionError::NotReady);
        }
        Ok((voters, learners))
    }

    pub(super) fn add_pd_learner(&mut self, id: u64, addr: SocketAddr) -> Result<bool, PartitionError> {
        let (voters, learners) = self.prepare_pd_membership_change()?;
        if id == 0 {
            return Err(Self::invariant("PD id 0 is reserved"));
        }
        if voters.contains(&id) {
            return Err(Self::invariant(format!("pd {id} is already a voter")));
        }
        match self.pd_peer_record(id) {
            Some(PdPeerRecord::Retired { .. }) => {
                return Err(Self::invariant(format!(
                    "pd id {id} belonged to a removed replica and is never reused; choose a new id"
                )));
            }
            Some(PdPeerRecord::Member(current)) if learners.contains(&id) => {
                if current == addr {
                    return Ok(false);
                }
                return Err(Self::invariant(format!(
                    "pd {id} is already a learner at {current}; remove it with pd-remove-learner \
                     and add a new id"
                )));
            }
            _ if learners.contains(&id) => {
                return Err(Self::invariant(format!("pd {id} is already a learner")));
            }
            _ => {}
        }
        self.propose_pd_change(
            vals_raft::ConfChangeType::AddLearnerNode,
            id,
            (id, PdPeerRecord::Member(addr)),
        )?;
        Ok(true)
    }

    pub(super) fn remove_pd_learner(&mut self, id: u64) -> Result<bool, PartitionError> {
        let (_, learners) = self.prepare_pd_membership_change()?;
        if !learners.contains(&id) {
            if matches!(self.pd_peer_record(id), Some(PdPeerRecord::Retired { replaced_by: 0 })) {
                return Ok(false);
            }
            return Err(Self::invariant(format!("pd {id} is not a learner")));
        }
        if let Some(old) = self.reserved_learner_of(id) {
            return Err(Self::invariant(format!(
                "learner {id} is reserved to replace pd {old}; finish with pd-replace {old} {id}, or \
                 add another learner and run pd-replace {old} <its id>, then remove {id}"
            )));
        }
        self.propose_pd_change(
            vals_raft::ConfChangeType::RemoveNode,
            id,
            (id, PdPeerRecord::Retired { replaced_by: 0 }),
        )?;
        Ok(true)
    }

    pub(super) fn replace_pd_voter_step(&mut self, old: u64, new: u64) -> Result<bool, PartitionError> {
        let (voters, learners) = self.prepare_pd_membership_change()?;
        if old == new {
            return Err(Self::invariant("a voter is replaced by a different, new id"));
        }
        let old_record = self.pd_peer_record(old);
        if voters.contains(&new) {
            if old_record == Some(PdPeerRecord::Retired { replaced_by: new }) {
                return Ok(false);
            }
            return Err(Self::invariant(format!("pd {new} is already a voter")));
        }
        if !learners.contains(&new) {
            return Err(Self::invariant(format!(
                "pd {new} is not a learner; start it with `join = true` and add it with \
                 pd-add-learner {new} <addr> first"
            )));
        }
        self.check_learner_caught_up(new)?;
        if voters.contains(&old) {
            if old == self.id() {
                return Err(Self::invariant(format!(
                    "pd {old} is the current leader, so it is not lost; replace only a stopped voter"
                )));
            }
            if voters.len() < 3 {
                return Err(Self::invariant(format!(
                    "only {} voters remain; finish the pending replacement before removing another",
                    voters.len()
                )));
            }
            self.propose_pd_change(
                vals_raft::ConfChangeType::RemoveNode,
                old,
                (old, PdPeerRecord::Retired { replaced_by: new }),
            )?;
            return Ok(true);
        }
        match old_record {
            Some(PdPeerRecord::Retired { replaced_by }) if learners.contains(&replaced_by) => {
                self.propose_pd_change(
                    vals_raft::ConfChangeType::AddNode,
                    new,
                    (old, PdPeerRecord::Retired { replaced_by: new }),
                )?;
                Ok(true)
            }
            Some(PdPeerRecord::Retired { replaced_by }) if replaced_by != 0 => Err(Self::invariant(
                format!("pd {old} was already replaced by pd {replaced_by}"),
            )),
            _ => Err(Self::invariant(format!("pd {old} is not a voter"))),
        }
    }

    pub fn incarnation(&self) -> Option<u128> {
        incarnation_of(&|key| self.state_value(key))
    }

    fn state_value(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.partition.state_get(key).ok().flatten()
    }

    pub(super) fn state_entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = self.partition
            .state_keys()
            .into_iter()
            .filter_map(|key| {
                let value = self.state_value(&key)?;
                Some((key, value))
            })
            .collect();
        entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    pub(super) fn ring_rf(&self) -> Option<u32> {
        ring_rf_of(&|key| self.state_value(key))
    }

    pub(super) fn mint_identity(
        &mut self,
        incarnation: Option<u128>,
        ring_rf: Option<u32>,
    ) -> Result<u64, PartitionError> {
        self.prepare_authoritative_mutation()?;
        if incarnation == Some(0) {
            return Err(Self::invariant("cluster incarnation must be non-zero"));
        }
        if ring_rf == Some(0) {
            return Err(Self::invariant("ring rf must be >= 1"));
        }
        let mut writes = Vec::new();
        if let Some(id) = incarnation {
            writes.push((incarnation_key(), Some(encode_incarnation(id))));
        }
        if let Some(rf) = ring_rf {
            writes.push((ring_rf_key(), Some(encode_ring_rf(rf))));
        }
        self.partition.propose_index(&Command::ConditionalBatch {
            checks: writes.iter().map(|(key, _)| (key.clone(), None)).collect(),
            writes,
        })
    }

    pub(super) fn restore_seed_ack(&self, id: u64) -> Option<u32> {
        self.state_value(&super::snapshot::seed_ack_key(id)).and_then(|value| super::snapshot::decode_seed_ack(&value))
    }

    pub(super) fn propose_restore_seed_ack(&mut self, digest: u32) -> Result<(), PartitionError> {
        let (key, value) = super::snapshot::seed_ack_entry(self.id(), digest);
        self.partition.propose(&Command::Put(key, value))
    }

    pub(super) fn state_capacity(&self) -> Option<(usize, usize, usize)> {
        self.partition.state_capacity().map(|capacity| {
            (capacity, self.partition.state_usage(), self.partition.state_reserved())
        })
    }

    pub fn is_leader(&self) -> bool {
        self.partition.is_leader()
    }

    pub(super) fn is_caught_up_leader(&self) -> bool {
        self.partition.is_caught_up_leader()
    }

    pub fn members(&self) -> Vec<NodeId> {
        self.members_weighted()
            .into_iter()
            .map(|(n, _)| n)
            .collect()
    }

    pub fn members_weighted(&self) -> Vec<(NodeId, u32)> {
        self.members_addressed()
            .into_iter()
            .map(|(n, w, _)| (n, w))
            .collect()
    }

    pub fn members_addressed(&self) -> AddressedMembers {
        members_of(&self.control_keys(), &|key| self.state_value(key))
    }

    pub fn refresh_ring(&self, ring: &mut Ring) {
        ring.set_members_weighted(self.members_weighted());
    }

    pub fn zones(&self) -> Catalog {
        zones_of(&self.control_keys(), &|key| self.state_value(key))
    }

    pub(super) fn zone_exists(&self, id: ZoneId) -> bool {
        self.state_value(&zone_key(id)).is_some()
    }

    fn range_map_keys_where(&self, select: impl FnMut(ZoneId) -> bool) -> Vec<Vec<u8>> {
        self.with_state_index(|index| index.range_map_keys_where(select))
    }

    pub(super) fn collect_orphan_range_maps(&mut self) -> Result<bool, PartitionError> {
        let mut seen = HashMap::new();
        let keys = self.range_map_keys_where(|id| {
            *seen.entry(id).or_insert_with(|| !self.zone_exists(id))
        });
        let orphans: BTreeSet<ZoneId> =
            seen.into_iter().filter_map(|(id, orphan)| orphan.then_some(id)).collect();
        if keys.is_empty() {
            return Ok(false);
        }
        self.prepare_authoritative_mutation()?;
        log::info!(
            "pd: collecting {} range-map records of dropped zones {orphans:?}",
            keys.len()
        );
        self.partition.propose(&Command::ConditionalBatch {
            checks: orphans.into_iter().map(|id| (zone_key(id), None)).collect(),
            writes: keys.into_iter().map(|key| (key, None)).collect(),
        })?;
        Ok(true)
    }

    pub fn next_zone_id(&self) -> ZoneId {
        let live_next = self
            .zones()
            .ids()
            .into_iter()
            .max()
            .map_or(DEFAULT_ZONE + 1, |m| m.saturating_add(1));
        let persisted_next = self
            .partition
            .state_get(&next_zone_id_key())
            .ok()
            .flatten()
            .and_then(|value| value.as_slice().try_into().ok())
            .map(u32::from_le_bytes)
            .unwrap_or(DEFAULT_ZONE + 1);
        live_next.max(persisted_next)
    }

    pub fn create_zone(&mut self, cfg: ZoneConfig) -> Result<ZoneId, PartitionError> {
        self.prepare_authoritative_mutation()?;
        let id = self.validation_next_zone_id;
        self.validate_zone(id, &cfg)?;
        let next = id
            .checked_add(1)
            .ok_or_else(|| Self::invariant("zone id space is exhausted"))?;
        self.partition.propose(&Command::ConditionalBatch {
            checks: vec![(zone_key(id), None)],
            writes: vec![
                (next_zone_id_key(), Some(next.to_le_bytes().to_vec())),
                (zone_key(id), Some(encode_zone_config(&cfg))),
            ],
        })?;
        Ok(id)
    }

    fn validate_zone(&self, id: ZoneId, cfg: &ZoneConfig) -> Result<(), PartitionError> {
        cfg.validate(&self.validation_member_ids())
            .map_err(|error| Self::invariant(error.to_string()))?;
        if let Some(existing_id) = self.validation_zones.by_name(&cfg.name) {
            if existing_id != id {
                return Err(Self::invariant(format!(
                    "zone name {:?} is already used by zone {existing_id}",
                    cfg.name
                )));
            }
        }
        Ok(())
    }

    pub fn propose_zone(&mut self, id: ZoneId, cfg: &ZoneConfig) -> Result<(), PartitionError> {
        self.prepare_authoritative_mutation()?;
        let next = id
            .checked_add(1)
            .ok_or_else(|| Self::invariant("zone id space is exhausted"))?;
        self.validate_zone(id, cfg)?;
        let mut writes = Vec::with_capacity(2);
        if self.validation_next_zone_id < next {
            writes.push((next_zone_id_key(), Some(next.to_le_bytes().to_vec())));
        }
        writes.push((zone_key(id), Some(encode_zone_config(cfg))));
        if !self.validation_zones.contains(id) {
            writes.extend(self.range_map_keys_where(|zone| zone == id).into_iter().map(|key| (key, None)));
        }
        self.partition.propose(&Command::ConditionalBatch {
            checks: Vec::new(),
            writes,
        })
    }

    pub fn drop_zone(&mut self, id: ZoneId) -> Result<(), PartitionError> {
        self.prepare_authoritative_mutation()?;
        if !self.validation_zones.contains(id) {
            return Err(Self::invariant(format!("zone {id} does not exist")));
        }
        let mut writes = vec![(zone_key(id), None)];
        writes.extend(self.range_map_keys_where(|zone| zone == id).into_iter().map(|key| (key, None)));
        self.partition.propose(&Command::ConditionalBatch {
            checks: Vec::new(),
            writes,
        })
    }

    pub fn range_maps(&self) -> HashMap<ZoneId, RangeMap> {
        self.with_state_index(StateIndex::range_maps)
    }

    pub fn range_map(&self, zone_id: ZoneId) -> RangeMap {
        self.with_state_index(|index| index.range_map(zone_id))
    }

    pub fn set_range_map(&mut self, zone_id: ZoneId, map: &RangeMap) -> Result<(), PartitionError> {
        let before = self.range_map(zone_id);
        self.set_range_map_checked(zone_id, &before, map, Vec::new()).map(|_| ())
    }

    pub(super) fn set_initial_range_map(
        &mut self,
        zone_id: ZoneId,
        cfg: &ZoneConfig,
        map: &RangeMap,
    ) -> Result<(), PartitionError> {
        let key = zone_key(zone_id);
        let expected = encode_zone_config(cfg);
        if self.partition.state_get(&key)?.as_deref() != Some(expected.as_slice()) {
            return Err(Self::invariant(format!(
                "zone {zone_id} no longer matches initial range map owner"
            )));
        }
        if self.range_maps().contains_key(&zone_id) {
            return Err(Self::invariant(format!(
                "zone {zone_id} initial range map is no longer pristine"
            )));
        }
        let checks = vec![
            (key, Some(expected)),
            (range_map_meta_key(zone_id), None),
            (range_map_key(zone_id), None),
            (range_record_key(zone_id, &[]), None),
        ];
        self.set_range_map_checked(zone_id, &RangeMap::single(), map, checks)
            .map(|_| ())
    }

    fn set_range_map_checked(
        &mut self,
        zone_id: ZoneId,
        before: &RangeMap,
        after: &RangeMap,
        mut checks: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) -> Result<(u64, usize, usize), PartitionError> {
        let zone = zone_key(zone_id);
        let Some(cfg) = self.partition.state_get(&zone)? else {
            return Err(Self::invariant(format!("zone {zone_id} does not exist")));
        };
        checks.push((zone, Some(cfg)));
        if self.range_map(zone_id) != *before {
            return Ok((self.partition.last_index(), 0, 0));
        }
        let meta_key = range_map_meta_key(zone_id);
        let meta = self.partition.state_get(&meta_key)?;
        let legacy_key = range_map_key(zone_id);
        let legacy = self.partition.state_get(&legacy_key)?;
        let generation = meta.as_deref().and_then(decode_range_map_meta).map_or(0, |v| v.0);
        checks.push((meta_key.clone(), meta.clone()));
        checks.push((legacy_key.clone(), legacy.clone()));

        let incremental = meta.is_some();
        let next_meta = encode_range_map_meta(
            generation.wrapping_add(1),
            after.supports_opaque_tokens(),
        );
        let mut writes = vec![(meta_key, Some(next_meta))];
        if legacy.is_some() {
            writes.push((legacy_key, None));
        }
        push_range_record_writes(zone_id, incremental.then_some(before), after, &mut writes);
        let write_count = writes.len();
        let command = Command::ConditionalBatch { checks, writes };
        #[cfg(test)]
        let encoded_len = command.encode().len();
        #[cfg(not(test))]
        let encoded_len = 0;
        self.partition
            .propose_index(&command)
            .map(|index| (index, encoded_len, write_count))
    }

    pub fn split_zone(&mut self, zone_id: ZoneId, at: &[u8]) -> Result<bool, PartitionError> {
        let mut map = self.range_map(zone_id);
        if !map.split(at) {
            return Ok(false);
        }
        self.set_range_map(zone_id, &map)?;
        Ok(true)
    }

    #[cfg(test)]
    pub(super) fn split_zone_measured(
        &mut self,
        zone_id: ZoneId,
        at: &[u8],
    ) -> Result<Option<(u64, usize, usize)>, PartitionError> {
        let before = self.range_map(zone_id);
        let mut after = before.clone();
        if !after.split(at) {
            return Ok(None);
        }
        self.set_range_map_checked(zone_id, &before, &after, Vec::new()).map(Some)
    }

    pub fn split_zone_canonical(
        &mut self,
        zone_id: ZoneId,
        at: &[u8],
    ) -> Result<bool, PartitionError> {
        let mut map = self.range_map(zone_id);
        if map.split_canonical(at) {
            self.set_range_map(zone_id, &map)?;
            return Ok(true);
        }
        if at.is_empty() || map.splits().iter().any(|split| split == at) {
            return Ok(false);
        }
        log::warn!(
            "pd: canonical split on zone {zone_id} range {:016x} would violate token ordering; falling back to inherited placement",
            range_log_id(zone_id, at)
        );
        if !map.split(at) {
            return Ok(false);
        }
        self.set_range_map(zone_id, &map)?;
        Ok(true)
    }

    pub fn split_zone_canonical_to(
        &mut self,
        zone_id: ZoneId,
        at: &[u8],
        token: Vec<u8>,
    ) -> Result<bool, PartitionError> {
        let mut map = self.range_map(zone_id);
        if !map.split_canonical_with_token(at, token) {
            return Ok(false);
        }
        self.set_range_map(zone_id, &map)?;
        Ok(true)
    }

    pub fn merge_zone(&mut self, zone_id: ZoneId, at: &[u8]) -> Result<bool, PartitionError> {
        let mut map = self.range_map(zone_id);
        if !map.merge_at(at) {
            return Ok(false);
        }
        self.set_range_map(zone_id, &map)?;
        Ok(true)
    }

    pub fn merge_zone_all(
        &mut self,
        zone_id: ZoneId,
        boundaries: &[Vec<u8>],
    ) -> Result<bool, PartitionError> {
        let mut map = self.range_map(zone_id);
        let merged = boundaries.iter().filter(|at| map.merge_at(at)).count();
        if merged == 0 {
            return Ok(false);
        }
        self.set_range_map(zone_id, &map)?;
        Ok(true)
    }

    pub fn prepare_rehome(&mut self, zone_id: ZoneId, lower: &[u8]) -> Result<bool, PartitionError> {
        self.prepare_rehome_to(zone_id, lower, lower)
    }

    pub fn prepare_rehome_to(
        &mut self,
        zone_id: ZoneId,
        lower: &[u8],
        target: &[u8],
    ) -> Result<bool, PartitionError> {
        let mut map = self.range_map(zone_id);
        let index = map.range_index(lower);
        if map.range_bounds(index).0.unwrap_or_default() != lower
            || !map.set_pending(index, target.to_vec())
        {
            return Ok(false);
        }
        self.set_range_map(zone_id, &map)?;
        Ok(true)
    }

    pub(super) fn balance_intent(&self) -> Result<Option<u64>, PartitionError> {
        Ok(self.partition.state_get(&balance_lock_key())?
            .and_then(|value| value.as_slice().try_into().ok())
            .map(u64::from_le_bytes))
    }

    pub(super) fn acquire_balance_intent(&mut self, identity: u64) -> Result<u64, PartitionError> {
        let key = balance_lock_key();
        self.partition.propose_index(&Command::ConditionalWrite {
            checks: vec![(key.clone(), None)],
            key,
            value: Some(identity.to_le_bytes().to_vec()),
        })
    }

    pub(super) fn prepare_balance_map(
        &mut self,
        zone_id: ZoneId,
        lower: &[u8],
        target: &[u8],
        expected: &RangeMap,
        identity: u64,
    ) -> Result<Option<u64>, PartitionError> {
        let mut next = expected.clone();
        let index = next.range_index(lower);
        if next.range_bounds(index).0.unwrap_or_default() != lower
            || !next.set_pending(index, target.to_vec())
        {
            return Ok(None);
        }
        if self.range_map(zone_id) != *expected {
            return Ok(None);
        }
        self.set_range_map_checked(
            zone_id,
            expected,
            &next,
            vec![(balance_lock_key(), Some(identity.to_le_bytes().to_vec()))],
        ).map(|(index, _, _)| Some(index))
    }

    pub(super) fn clear_balance_intent(&mut self, identity: u64) -> Result<u64, PartitionError> {
        let key = balance_lock_key();
        self.partition.propose_index(&Command::ConditionalWrite {
            checks: vec![(key.clone(), Some(identity.to_le_bytes().to_vec()))],
            key,
            value: None,
        })
    }

    pub(super) fn clear_orphan_balance_lock(&mut self) -> Result<bool, PartitionError> {
        let Some(identity) = self.balance_intent()? else { return Ok(false) };
        self.clear_balance_intent(identity)?;
        Ok(true)
    }

    fn set_rehome_result(
        &mut self,
        zone_id: ZoneId,
        _before: &RangeMap,
        after: &RangeMap,
    ) -> Result<(), PartitionError> {
        self.set_range_map(zone_id, after)
    }

    pub fn abort_rehome(&mut self, zone_id: ZoneId, lower: &[u8]) -> Result<bool, PartitionError> {
        let before = self.range_map(zone_id);
        let mut map = before.clone();
        let index = map.range_index(lower);
        if map.range_bounds(index).0.unwrap_or_default() != lower || !map.clear_pending(index) {
            return Ok(false);
        }
        self.set_rehome_result(zone_id, &before, &map)?;
        Ok(true)
    }

    pub fn commit_rehome(&mut self, zone_id: ZoneId, lower: &[u8]) -> Result<bool, PartitionError> {
        let before = self.range_map(zone_id);
        let mut map = before.clone();
        let index = map.range_index(lower);
        if map.range_bounds(index).0.unwrap_or_default() != lower || !map.commit_pending(index) {
            return Ok(false);
        }
        self.set_rehome_result(zone_id, &before, &map)?;
        Ok(true)
    }
}

impl PlacementDriver<vals_spdk::MemBlockDevice> {
    pub fn new_single_bounded(id: u64, capacity: usize) -> Result<Self, PartitionError> {
        Self::from_partition(Partition::new_single_bounded(id, capacity)?)
    }

    pub fn open_single_bounded(
        id: u64,
        capacity: usize,
        dir: impl AsRef<std::path::Path>,
    ) -> Result<Self, PartitionError> {
        Self::from_partition(Partition::open_single_bounded(id, capacity, dir)?)
    }

    pub fn new_bounded(
        id: u64,
        voters: Vec<u64>,
        capacity: usize,
    ) -> Result<Self, PartitionError> {
        Self::from_partition(Partition::new_bounded(id, voters, capacity)?)
    }

    pub fn open_bounded(
        id: u64,
        voters: Vec<u64>,
        capacity: usize,
        dir: impl AsRef<std::path::Path>,
    ) -> Result<Self, PartitionError> {
        let mut partition = Partition::open_bounded(id, voters, capacity, dir)?;
        partition.drive()?;
        Self::from_partition(partition)
    }

    pub fn open_bounded_joining(
        id: u64,
        capacity: usize,
        dir: impl AsRef<std::path::Path>,
    ) -> Result<Self, PartitionError> {
        let mut partition = Partition::open_bounded_joining(id, capacity, dir)?;
        partition.drive()?;
        Self::from_partition(partition)
    }
}

type StateGet<'a> = &'a dyn Fn(&[u8]) -> Option<Vec<u8>>;

pub(super) fn incarnation_of(get: StateGet) -> Option<u128> {
    decode_incarnation(&get(&incarnation_key())?)
}

fn ring_rf_of(get: StateGet) -> Option<u32> {
    decode_ring_rf(&get(&ring_rf_key())?)
}

pub(super) fn members_of(keys: &[Vec<u8>], get: StateGet) -> AddressedMembers {
    let mut members: AddressedMembers = keys
        .iter()
        .filter_map(|k| {
            let node = <[u8; 4]>::try_from(k.as_slice()).ok().map(u32::from_le_bytes)?;
            let (weight, addrs) = get(k)
                .map(|v| decode_member_value(&v))
                .unwrap_or((1, Vec::new()));
            Some((node, weight, addrs))
        })
        .collect();
    members.sort_unstable_by_key(|&(n, _, _)| n);
    members
}

pub(super) fn zones_of(keys: &[Vec<u8>], get: StateGet) -> Catalog {
    let mut cat = Catalog::new();
    for key in keys {
        if let Some(id) = zone_id_of_key(key) {
            if let Some(cfg) = get(key).and_then(|v| decode_zone_config(&v)) {
                cat.insert(id, cfg);
            }
        }
    }
    cat
}

pub(super) fn range_maps_of(keys: &[Vec<u8>], get: StateGet) -> HashMap<ZoneId, RangeMap> {
    StateIndex::build(keys.to_vec(), get).range_maps()
}

type RangeRecord = (Vec<u8>, Option<Vec<u8>>, bool);

#[derive(Default)]
struct StateIndex {
    applied: Option<u64>,
    control_keys: BTreeSet<Vec<u8>>,
    zones: BTreeMap<ZoneId, ZoneMapState>,
}

#[derive(Default)]
struct ZoneMapState {
    meta: Option<Vec<u8>>,
    legacy: Option<Vec<u8>>,
    records: BTreeMap<Vec<u8>, Option<RangeRecord>>,
    map: RangeMap,
}

impl StateIndex {
    fn build(keys: Vec<Vec<u8>>, get: StateGet) -> Self {
        let mut index = StateIndex::default();
        index.update(keys, get);
        index
    }

    fn update(&mut self, keys: Vec<Vec<u8>>, get: StateGet) {
        let keys: BTreeSet<Vec<u8>> = keys.into_iter().collect();
        let mut dirty = BTreeSet::new();
        for key in keys {
            let value = get(&key);
            if let Some((zone, lower)) = range_record_of_key(&key) {
                let records = &mut self.zones.entry(zone).or_default().records;
                match value {
                    Some(value) => records.insert(lower, decode_range_record(&value)),
                    None => records.remove(&lower),
                };
                dirty.insert(zone);
                continue;
            }
            if let Some(zone) = range_map_meta_id_of_key(&key) {
                self.zones.entry(zone).or_default().meta = value.clone();
                dirty.insert(zone);
            } else if let Some(zone) = range_map_id_of_key(&key) {
                self.zones.entry(zone).or_default().legacy = value.clone();
                dirty.insert(zone);
            }
            if value.is_some() {
                self.control_keys.insert(key);
            } else {
                self.control_keys.remove(&key);
            }
        }
        for zone in dirty {
            let Some(state) = self.zones.get_mut(&zone) else { continue };
            if state.meta.is_none() && state.legacy.is_none() && state.records.is_empty() {
                self.zones.remove(&zone);
            } else {
                state.map = state.assemble();
            }
        }
    }

    fn range_map(&self, zone_id: ZoneId) -> RangeMap {
        self.zones.get(&zone_id).map_or_else(RangeMap::single, |state| state.map.clone())
    }

    fn range_maps(&self) -> HashMap<ZoneId, RangeMap> {
        self.zones
            .iter()
            .filter(|(&id, state)| {
                (state.meta.is_some() || state.legacy.is_some())
                    && self.control_keys.contains(&zone_key(id))
            })
            .map(|(&id, state)| (id, state.map.clone()))
            .collect()
    }

    fn range_map_keys_where(&self, mut select: impl FnMut(ZoneId) -> bool) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        for (&id, state) in &self.zones {
            if !select(id) {
                continue;
            }
            if state.meta.is_some() {
                keys.push(range_map_meta_key(id));
            }
            if state.legacy.is_some() {
                keys.push(range_map_key(id));
            }
            keys.extend(state.records.keys().map(|lower| range_record_key(id, lower)));
        }
        keys
    }
}

impl ZoneMapState {
    fn assemble(&self) -> RangeMap {
        self.assemble_records().unwrap_or_else(|| {
            self.legacy.as_deref().and_then(RangeMap::decode).unwrap_or_else(RangeMap::single)
        })
    }

    fn assemble_records(&self) -> Option<RangeMap> {
        let (_, opaque) = decode_range_map_meta(self.meta.as_deref()?)?;
        if !self.records.first_key_value()?.0.is_empty() {
            return None;
        }
        let count = self.records.len();
        let mut splits = Vec::with_capacity(count - 1);
        let mut tokens = Vec::with_capacity(count);
        let mut pending = Vec::with_capacity(count);
        let mut debt = Vec::with_capacity(count);
        for (lower, record) in &self.records {
            let (token, target, owes) = record.as_ref()?;
            if !lower.is_empty() {
                splits.push(lower.clone());
            }
            tokens.push(token.clone());
            pending.push(target.clone());
            debt.push(*owes);
        }
        if opaque {
            RangeMap::from_opaque_parts(splits, tokens, pending, debt).ok()
        } else {
            RangeMap::from_splits_tokens_and_pending(splits, tokens, pending).ok()
        }
    }
}

const LEARNER_CATCH_UP_SLACK: u64 = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PdPeerRecord {
    Member(SocketAddr),
    Retired { replaced_by: u64 },
}

const PD_PEER_VALUE_VERSION: u8 = 1;

pub(super) fn pd_peer_key(id: u64) -> Vec<u8> {
    let mut key = vec![b'V'];
    key.extend_from_slice(&id.to_le_bytes());
    key
}

pub(super) fn pd_peer_id_of_key(key: &[u8]) -> Option<u64> {
    if key.len() == 9 && key[0] == b'V' {
        Some(u64::from_le_bytes(key[1..].try_into().ok()?))
    } else {
        None
    }
}

pub(super) fn encode_pd_peer(record: &PdPeerRecord) -> Vec<u8> {
    let mut value = vec![PD_PEER_VALUE_VERSION];
    match record {
        PdPeerRecord::Member(addr) => {
            value.push(1);
            wire::put_sock_addrs(&mut value, &[*addr]);
        }
        PdPeerRecord::Retired { replaced_by } => {
            value.push(2);
            value.extend_from_slice(&replaced_by.to_le_bytes());
        }
    }
    value
}

fn decode_pd_peer(value: &[u8]) -> Option<PdPeerRecord> {
    match value {
        [PD_PEER_VALUE_VERSION, 1, ..] => {
            let mut pos = 2;
            let addrs = wire::read_sock_addrs(value, &mut pos)?;
            (pos == value.len() && addrs.len() == 1).then(|| PdPeerRecord::Member(addrs[0]))
        }
        [PD_PEER_VALUE_VERSION, 2, rest @ ..] => Some(PdPeerRecord::Retired {
            replaced_by: u64::from_le_bytes(rest.try_into().ok()?),
        }),
        _ => None,
    }
}

pub(super) fn balance_lock_key() -> Vec<u8> {
    vec![b'B']
}

pub(super) fn incarnation_key() -> Vec<u8> {
    vec![b'I']
}

const INCARNATION_VALUE_VERSION: u8 = 1;

pub(super) fn encode_incarnation(id: u128) -> Vec<u8> {
    let mut value = Vec::with_capacity(17);
    value.push(INCARNATION_VALUE_VERSION);
    value.extend_from_slice(&id.to_le_bytes());
    value
}

fn decode_incarnation(value: &[u8]) -> Option<u128> {
    if value.len() != 17 || value[0] != INCARNATION_VALUE_VERSION {
        return None;
    }
    Some(u128::from_le_bytes(value[1..17].try_into().ok()?)).filter(|&id| id != 0)
}

pub(super) fn ring_rf_key() -> Vec<u8> {
    vec![b'G']
}

const RING_RF_VALUE_VERSION: u8 = 1;

pub(super) fn encode_ring_rf(rf: u32) -> Vec<u8> {
    let mut value = Vec::with_capacity(5);
    value.push(RING_RF_VALUE_VERSION);
    value.extend_from_slice(&rf.to_le_bytes());
    value
}

fn decode_ring_rf(value: &[u8]) -> Option<u32> {
    if value.len() != 5 || value[0] != RING_RF_VALUE_VERSION {
        return None;
    }
    Some(u32::from_le_bytes(value[1..5].try_into().ok()?)).filter(|&rf| rf != 0)
}

pub(super) fn fresh_incarnation() -> io::Result<u128> {
    let mut f = std::fs::File::open("/dev/urandom")?;
    loop {
        let mut bytes = [0u8; 16];
        f.read_exact(&mut bytes)?;
        let id = u128::from_le_bytes(bytes);
        if id != 0 {
            return Ok(id);
        }
    }
}

fn next_zone_id_key() -> Vec<u8> {
    vec![b'N']
}

pub(super) fn range_map_key(id: ZoneId) -> Vec<u8> {
    let mut k = Vec::with_capacity(5);
    k.push(b'R');
    k.extend_from_slice(&id.to_le_bytes());
    k
}

fn range_map_meta_key(id: ZoneId) -> Vec<u8> {
    let mut key = Vec::with_capacity(5);
    key.push(b'P');
    key.extend_from_slice(&id.to_le_bytes());
    key
}

fn range_map_meta_id_of_key(key: &[u8]) -> Option<ZoneId> {
    if key.len() == 5 && key[0] == b'P' {
        Some(u32::from_le_bytes(key[1..5].try_into().ok()?))
    } else {
        None
    }
}

fn encode_range_map_meta(generation: u64, opaque: bool) -> Vec<u8> {
    let mut value = Vec::with_capacity(10);
    value.push(1);
    value.push(u8::from(opaque));
    value.extend_from_slice(&generation.to_le_bytes());
    value
}

fn decode_range_map_meta(value: &[u8]) -> Option<(u64, bool)> {
    if value.len() != 10 || value[0] != 1 || value[1] > 1 {
        return None;
    }
    Some((u64::from_le_bytes(value[2..10].try_into().ok()?), value[1] == 1))
}

fn range_record_key(id: ZoneId, lower: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(5 + lower.len());
    key.push(b'Q');
    key.extend_from_slice(&id.to_le_bytes());
    key.extend_from_slice(lower);
    key
}

pub(super) fn range_record_of_key(key: &[u8]) -> Option<(ZoneId, Vec<u8>)> {
    if key.len() < 5 || key[0] != b'Q' {
        return None;
    }
    Some((u32::from_le_bytes(key[1..5].try_into().ok()?), key[5..].to_vec()))
}

fn push_range_record_writes(
    zone_id: ZoneId,
    before: Option<&RangeMap>,
    after: &RangeMap,
    writes: &mut Vec<(Vec<u8>, Option<Vec<u8>>)>,
) {
    fn lower(map: &RangeMap, index: usize) -> &[u8] {
        map.range_bounds(index).0.unwrap_or_default()
    }
    fn record(map: &RangeMap, index: usize) -> (&[u8], Option<&[u8]>, bool) {
        (map.token_of(index), map.pending_of(index), map.placement_debt_of(index))
    }
    let mut deletes = Vec::new();
    let mut old = 0;
    for new in 0..after.num_ranges() {
        let at = lower(after, new);
        let mut unchanged = false;
        if let Some(before) = before {
            while old < before.num_ranges() && lower(before, old) < at {
                deletes.push((range_record_key(zone_id, lower(before, old)), None));
                old += 1;
            }
            if old < before.num_ranges() && lower(before, old) == at {
                unchanged = record(before, old) == record(after, new);
                old += 1;
            }
        }
        if !unchanged {
            let (token, pending, debt) = record(after, new);
            writes.push((range_record_key(zone_id, at), Some(encode_range_record(token, pending, debt))));
        }
    }
    if let Some(before) = before {
        while old < before.num_ranges() {
            deletes.push((range_record_key(zone_id, lower(before, old)), None));
            old += 1;
        }
    }
    writes.append(&mut deletes);
}

pub(super) fn encode_range_record(token: &[u8], pending: Option<&[u8]>, debt: bool) -> Vec<u8> {
    let mut value = Vec::with_capacity(10 + token.len() + pending.map_or(0, <[u8]>::len));
    value.push(1);
    value.extend_from_slice(&(token.len() as u32).to_le_bytes());
    value.extend_from_slice(token);
    value.push(u8::from(pending.is_some()));
    if let Some(pending) = pending {
        value.extend_from_slice(&(pending.len() as u32).to_le_bytes());
        value.extend_from_slice(pending);
    }
    value.push(u8::from(debt));
    value
}

pub(super) fn decode_range_record(value: &[u8]) -> Option<(Vec<u8>, Option<Vec<u8>>, bool)> {
    if value.first() != Some(&1) {
        return None;
    }
    let mut rest = &value[1..];
    let take = |rest: &mut &[u8]| -> Option<Vec<u8>> {
        let (length, tail) = rest.split_at_checked(4)?;
        let length = u32::from_le_bytes(length.try_into().ok()?) as usize;
        let (field, tail) = tail.split_at_checked(length)?;
        *rest = tail;
        Some(field.to_vec())
    };
    let token = take(&mut rest)?;
    let (&pending, tail) = rest.split_first()?;
    rest = tail;
    let pending = match pending {
        0 => None,
        1 => Some(take(&mut rest)?),
        _ => return None,
    };
    let (&debt, tail) = rest.split_first()?;
    if !tail.is_empty() || debt > 1 {
        return None;
    }
    Some((token, pending, debt == 1))
}

#[cfg(test)]
pub(super) fn range_map_zone_of_key(key: &[u8]) -> Option<ZoneId> {
    range_map_meta_id_of_key(key)
        .or_else(|| range_map_id_of_key(key))
        .or_else(|| range_record_of_key(key).map(|(id, _)| id))
}

pub(super) fn range_map_id_of_key(key: &[u8]) -> Option<ZoneId> {
    if key.len() == 5 && key[0] == b'R' {
        Some(u32::from_le_bytes(key[1..5].try_into().ok()?))
    } else {
        None
    }
}

pub(super) fn zone_key(id: ZoneId) -> Vec<u8> {
    let mut k = Vec::with_capacity(5);
    k.push(b'Z');
    k.extend_from_slice(&id.to_le_bytes());
    k
}

fn zone_id_of_key(key: &[u8]) -> Option<ZoneId> {
    if key.len() == 5 && key[0] == b'Z' {
        Some(u32::from_le_bytes(key[1..5].try_into().ok()?))
    } else {
        None
    }
}

const MEMBER_VALUE_VERSION: u8 = 1;

pub(super) fn encode_member_value(weight: u32, addrs: &[SocketAddr]) -> Vec<u8> {
    let mut b = vec![MEMBER_VALUE_VERSION];
    b.extend_from_slice(&weight.to_le_bytes());
    wire::put_sock_addrs(&mut b, addrs);
    b
}

fn decode_member_value(v: &[u8]) -> (u32, Vec<SocketAddr>) {
    if v.first() != Some(&MEMBER_VALUE_VERSION) || v.len() < 5 {
        return (1, Vec::new());
    }
    let weight = u32::from_le_bytes(v[1..5].try_into().unwrap()).max(1);
    let mut pos = 5;
    let addrs = wire::read_sock_addrs(v, &mut pos).unwrap_or_default();
    (weight, addrs)
}

pub(super) fn encode_zone_config(cfg: &ZoneConfig) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&(cfg.rf as u32).to_le_bytes());
    b.push(ordering_to_u8(cfg.ordering));
    match &cfg.scope {
        None => b.push(0),
        Some(s) => {
            b.push(1);
            b.extend_from_slice(&(s.len() as u32).to_le_bytes());
            for &n in s {
                b.extend_from_slice(&n.to_le_bytes());
            }
        }
    }
    b.extend_from_slice(&(cfg.name.len() as u32).to_le_bytes());
    b.extend_from_slice(cfg.name.as_bytes());
    b
}

fn decode_zone_config(buf: &[u8]) -> Option<ZoneConfig> {
    let mut pos = 0usize;
    let take = |buf: &[u8], pos: &mut usize, n: usize| -> Option<Vec<u8>> {
        let end = pos.checked_add(n)?;
        if end > buf.len() {
            return None;
        }
        let s = buf[*pos..end].to_vec();
        *pos = end;
        Some(s)
    };
    let rf = u32::from_le_bytes(take(buf, &mut pos, 4)?.try_into().ok()?) as usize;
    let ordering = ordering_from_u8(take(buf, &mut pos, 1)?[0])?;
    let scope = match take(buf, &mut pos, 1)?[0] {
        0 => None,
        1 => {
            let n = u32::from_le_bytes(take(buf, &mut pos, 4)?.try_into().ok()?) as usize;
            let mut set = BTreeSet::new();
            for _ in 0..n {
                set.insert(u32::from_le_bytes(take(buf, &mut pos, 4)?.try_into().ok()?));
            }
            Some(set)
        }
        _ => return None,
    };
    let name_len = u32::from_le_bytes(take(buf, &mut pos, 4)?.try_into().ok()?) as usize;
    let name = String::from_utf8(take(buf, &mut pos, name_len)?).ok()?;
    Some(ZoneConfig {
        name,
        rf,
        ordering,
        scope,
    })
}

pub(super) fn sorted_zones(cat: &Catalog) -> Vec<(ZoneId, ZoneConfig)> {
    let mut zones: Vec<(ZoneId, ZoneConfig)> =
        cat.iter().map(|(id, cfg)| (id, cfg.clone())).collect();
    zones.sort_unstable_by_key(|(id, _)| *id);
    zones
}

