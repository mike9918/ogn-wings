//! Node-local FIFO admission policy. No power actions or async runtime here.
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Queued,
    Starting,
    Attention,
    Running,
    Failed,
    Cancelled,
}
impl Phase {
    pub fn name(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Starting => "starting",
            Self::Attention => "attention_required",
            Self::Running => "running",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
    fn holds_slot(self) -> bool {
        matches!(self, Self::Starting | Self::Attention)
    }
    fn live(self) -> bool {
        self == Self::Queued || self.holds_slot()
    }
}
#[derive(Clone, Debug)]
pub struct Ticket {
    pub priority: u8,
    pub generation: u64,
    pub queued_at: u64,
    pub started_at: Option<u64>,
    pub finished_at: Option<u64>,
    pub phase: Phase,
}
#[derive(Debug)]
pub struct Queue {
    limit: usize,
    generation: u64,
    entries: BTreeMap<u128, Ticket>,
    waiting: VecDeque<u128>,
    external: BTreeSet<u128>,
}
impl Queue {
    pub fn new(limit: usize) -> Self {
        Self {
            limit: limit.max(1),
            generation: 0,
            entries: BTreeMap::new(),
            waiting: VecDeque::new(),
            external: BTreeSet::new(),
        }
    }
    pub fn set_limit(&mut self, limit: usize) {
        self.limit = limit.clamp(1, 64);
    }
    pub fn limit(&self) -> usize {
        self.limit
    }
    pub fn set_external(&mut self, ids: impl IntoIterator<Item = u128>) {
        self.external = ids.into_iter().collect();
    }
    pub fn active(&self) -> usize {
        self.entries
            .values()
            .filter(|e| e.phase.holds_slot())
            .count()
            + self
                .external
                .iter()
                .filter(|id| !self.entries.get(id).is_some_and(|e| e.phase.holds_slot()))
                .count()
    }
    #[cfg(test)]
    pub fn reserve(&mut self, id: u128, now: u64) -> Result<u64, &'static str> {
        self.reserve_priority(id, now, 0)
    }
    pub fn reserve_priority(
        &mut self,
        id: u128,
        now: u64,
        priority: u8,
    ) -> Result<u64, &'static str> {
        if self.entries.get(&id).is_some_and(|e| e.phase.live()) {
            return Err("Server already has a pending start");
        }
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or("Queue sequence overflow")?;
        self.entries.insert(
            id,
            Ticket {
                priority: priority.min(10),
                generation: self.generation,
                queued_at: now,
                started_at: None,
                finished_at: None,
                phase: Phase::Queued,
            },
        );
        self.waiting.push_back(id);
        // Bounded recent history; pending/active tickets are never evicted.
        if self.entries.len() > 2048 {
            let oldest = self
                .entries
                .iter()
                .filter(|(_, e)| !e.phase.live())
                .min_by_key(|(_, e)| e.queued_at)
                .map(|(id, _)| *id);
            if let Some(oldest) = oldest {
                self.entries.remove(&oldest);
            }
        }
        Ok(self.generation)
    }
    pub fn admit(&mut self, id: u128, generation: u64, now: u64) -> Result<bool, &'static str> {
        let entry = self
            .entries
            .get(&id)
            .ok_or("Start request no longer exists")?;
        if entry.generation != generation || entry.phase != Phase::Queued {
            return Err("Start request was cancelled or replaced");
        }
        if self.ordered_waiting(now).first() != Some(&id) || self.active() >= self.limit {
            return Ok(false);
        }
        self.waiting.retain(|candidate| *candidate != id);
        let entry = self.entries.get_mut(&id).unwrap();
        entry.phase = Phase::Starting;
        entry.started_at = Some(now);
        Ok(true)
    }
    pub fn cancel(&mut self, id: u128, now: u64) -> bool {
        if let Some(entry) = self.entries.get_mut(&id)
            && entry.phase == Phase::Queued
        {
            entry.phase = Phase::Cancelled;
            entry.finished_at = Some(now);
            self.waiting.retain(|x| *x != id);
            return true;
        }
        false
    }
    pub fn attention(&mut self, id: u128, generation: u64) {
        if let Some(e) = self.entries.get_mut(&id)
            && e.generation == generation
            && e.phase == Phase::Starting
        {
            e.phase = Phase::Attention;
        }
    }
    pub fn finish(&mut self, id: u128, generation: u64, now: u64, running: bool) {
        if let Some(e) = self.entries.get_mut(&id)
            && e.generation == generation
            && e.phase.live()
        {
            e.phase = if running {
                Phase::Running
            } else {
                Phase::Failed
            };
            e.finished_at = Some(now);
            self.waiting.retain(|x| *x != id);
        }
    }
    pub fn ticket(&self, id: u128) -> Option<&Ticket> {
        self.entries.get(&id)
    }
    pub fn effective_priority(ticket: &Ticket, now: u64) -> u64 {
        u64::from(ticket.priority) + now.saturating_sub(ticket.queued_at) / 300_000
    }
    fn ordered_waiting(&self, now: u64) -> Vec<u128> {
        let mut ids: Vec<_> = self.waiting.iter().copied().collect();
        ids.sort_by(|a, b| {
            let a = &self.entries[a];
            let b = &self.entries[b];
            Self::effective_priority(b, now)
                .cmp(&Self::effective_priority(a, now))
                .then(a.generation.cmp(&b.generation))
        });
        ids
    }
    pub fn position_at(&self, id: u128, now: u64) -> usize {
        self.ordered_waiting(now)
            .iter()
            .position(|x| *x == id)
            .map_or(0, |n| n + 1)
    }
    #[cfg(test)]
    pub fn position(&self, id: u128) -> usize {
        self.position_at(id, 0)
    }
    pub fn entries(&self) -> impl Iterator<Item = (&u128, &Ticket)> {
        self.entries.iter()
    }
    pub fn external(&self) -> impl Iterator<Item = &u128> {
        self.external.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn priority_orders_waiters_without_preempting_active_and_fifo_breaks_ties() {
        let mut q = Queue::new(1);
        let a = q.reserve_priority(1, 0, 0).unwrap();
        q.admit(1, a, 1).unwrap();
        let b = q.reserve_priority(2, 2, 0).unwrap();
        let c = q.reserve_priority(3, 3, 10).unwrap();
        let d = q.reserve_priority(4, 4, 10).unwrap();
        assert!(!q.admit(3, c, 5).unwrap());
        assert_eq!(q.position_at(3, 5), 1);
        assert_eq!(q.position_at(4, 5), 2);
        assert_eq!(q.position_at(2, 5), 3);
        q.finish(1, a, 6, true);
        assert!(q.admit(3, c, 7).unwrap());
        assert!(!q.admit(4, d, 8).unwrap());
        q.finish(3, c, 9, true);
        assert!(q.admit(4, d, 10).unwrap());
        q.finish(4, d, 11, true);
        assert!(q.admit(2, b, 12).unwrap());
    }
    #[test]
    fn aging_prevents_indefinite_starvation_and_position_matches_admission() {
        let mut q = Queue::new(1);
        let old = q.reserve_priority(1, 0, 0).unwrap();
        let new = q.reserve_priority(2, 3_300_000, 10).unwrap();
        assert_eq!(q.position_at(1, 3_300_000), 1);
        assert_eq!(q.position_at(2, 3_300_000), 2);
        assert!(!q.admit(2, new, 3_300_000).unwrap());
        assert!(q.admit(1, old, 3_300_000).unwrap());
    }
    #[test]
    fn fifo_four_slots_and_no_fifth_until_ready() {
        let mut q = Queue::new(4);
        let g: Vec<_> = (1..=6).map(|id| q.reserve(id, 0).unwrap()).collect();
        assert!(!q.admit(2, g[1], 1).unwrap());
        for id in 1..=4 {
            assert!(q.admit(id, g[id as usize - 1], 10).unwrap());
        }
        assert_eq!(q.active(), 4);
        assert_eq!(q.position(5), 1);
        assert!(!q.admit(5, g[4], 20).unwrap());
        q.finish(1, g[0], 100, true);
        assert!(q.admit(5, g[4], 101).unwrap());
        assert_eq!(q.active(), 4);
    }
    #[test]
    fn duplicate_cancel_and_stale_completion_do_not_replay() {
        let mut q = Queue::new(1);
        let old = q.reserve(7, 0).unwrap();
        assert!(q.reserve(7, 1).is_err());
        assert!(q.cancel(7, 2));
        let new = q.reserve(7, 3).unwrap();
        q.finish(7, old, 4, false);
        assert!(q.admit(7, old, 5).is_err());
        assert!(q.admit(7, new, 5).unwrap());
        assert!(!q.cancel(7, 6));
        q.finish(7, new, 7, true);
        assert_eq!(q.ticket(7).unwrap().phase, Phase::Running);
    }
    #[test]
    fn timeout_keeps_slot_and_limit_decrease_never_overadmits() {
        let mut q = Queue::new(2);
        let a = q.reserve(1, 0).unwrap();
        let b = q.reserve(2, 0).unwrap();
        let c = q.reserve(3, 0).unwrap();
        q.admit(1, a, 1).unwrap();
        q.admit(2, b, 1).unwrap();
        q.attention(1, a);
        assert_eq!(q.active(), 2);
        q.set_limit(1);
        assert!(!q.admit(3, c, 300000).unwrap());
        q.finish(2, b, 300001, false);
        assert!(!q.admit(3, c, 300002).unwrap());
        q.finish(1, a, 300003, false);
        assert!(q.admit(3, c, 300004).unwrap());
    }
    #[test]
    fn queue_wait_and_startup_have_distinct_boundaries() {
        let mut q = Queue::new(1);
        let g = q.reserve(1, 100).unwrap();
        q.admit(1, g, 1100).unwrap();
        q.finish(1, g, 4100, true);
        let e = q.ticket(1).unwrap();
        assert_eq!(e.started_at.unwrap() - e.queued_at, 1000);
        assert_eq!(e.finished_at.unwrap() - e.started_at.unwrap(), 3000);
    }
    #[test]
    fn restored_starting_servers_consume_slots_without_double_counting() {
        let mut q = Queue::new(1);
        q.set_external([9]);
        let g = q.reserve(1, 0).unwrap();
        assert!(!q.admit(1, g, 1).unwrap());
        q.set_external([]);
        assert!(q.admit(1, g, 2).unwrap());
        q.set_external([1]);
        assert_eq!(q.active(), 1);
    }
}
