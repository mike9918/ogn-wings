//! Atomic journal of waiting requests, outside server-controlled storage.
//! Admission and cancellation remove their intent durably before acknowledging.
use super::startup_queue_core::{Phase, Queue, Ticket};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    io::{Read, Write},
    path::{Path, PathBuf},
};

const MAX_BYTES: u64 = 16 * 1024 * 1024;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pending {
    server: String,
    generation: u64,
    priority: u8,
    queued_at: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    version: u32,
    generation: u64,
    waiting: Vec<Pending>,
}
pub struct Journal {
    path: PathBuf,
}
impl Journal {
    pub fn new(root: &Path) -> Self {
        Self {
            path: root.join(".ogn-startup-queue.json"),
        }
    }
    pub fn load(&self, limit: usize) -> anyhow::Result<Queue> {
        let file = match std::fs::File::open(&self.path) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Queue::new(limit)),
            Err(e) => return Err(e.into()),
        };
        let mut bytes = Vec::new();
        file.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
        anyhow::ensure!(
            bytes.len() as u64 <= MAX_BYTES,
            "Queue journal exceeds supported size"
        );
        let doc: Document = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(
            doc.version == 1 && doc.waiting.len() <= 50_000,
            "Unsupported queue journal"
        );
        let mut ids = HashSet::new();
        let mut generations = HashSet::new();
        let mut entries = Vec::new();
        for pending in doc.waiting {
            let id = uuid::Uuid::parse_str(&pending.server)?.as_u128();
            anyhow::ensure!(
                ids.insert(id)
                    && pending.generation > 0
                    && pending.generation <= doc.generation
                    && generations.insert(pending.generation)
                    && pending.priority <= 10,
                "Invalid queue journal ticket"
            );
            entries.push((
                id,
                Ticket {
                    priority: pending.priority,
                    generation: pending.generation,
                    queued_at: pending.queued_at,
                    started_at: None,
                    finished_at: None,
                    phase: Phase::Queued,
                },
            ));
        }
        let mut queue = Queue::new(limit);
        queue.restore_pending(entries, doc.generation);
        Ok(queue)
    }
    pub fn save(&self, queue: &Queue) -> anyhow::Result<()> {
        let waiting: Vec<_> = queue
            .entries()
            .filter(|(_, e)| e.phase == Phase::Queued)
            .map(|(id, e)| Pending {
                server: uuid::Uuid::from_u128(*id).to_string(),
                generation: e.generation,
                priority: e.priority,
                queued_at: e.queued_at,
            })
            .collect();
        anyhow::ensure!(
            waiting.len() <= 50_000,
            "Queue journal exceeds supported ticket count"
        );
        let bytes = serde_json::to_vec(&Document {
            version: 1,
            generation: queue.generation(),
            waiting,
        })?;
        anyhow::ensure!(
            bytes.len() as u64 <= MAX_BYTES,
            "Queue journal exceeds supported size"
        );
        let parent = self
            .path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("Missing queue journal parent"))?;
        std::fs::create_dir_all(parent)?;
        let mut staged = tempfile::NamedTempFile::new_in(parent)?;
        staged.write_all(&bytes)?;
        staged.as_file().sync_all()?;
        staged.persist(&self.path)?;
        #[cfg(unix)]
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn round_trip_preserves_large_uuid_priority_age_ties_and_generation() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal::new(dir.path());
        let id = uuid::Uuid::parse_str("ffffffff-ffff-ffff-ffff-fffffffffffe")
            .unwrap()
            .as_u128();
        let mut q = Queue::new(1);
        q.reserve_priority(id, 100, 0).unwrap();
        q.reserve_priority(2, 200, 7).unwrap();
        q.reserve_priority(3, 201, 7).unwrap();
        journal.save(&q).unwrap();
        assert_eq!(journal.load(100).unwrap().limit(), 64);
        assert_eq!(journal.load(0).unwrap().limit(), 1);
        let mut restored = journal.load(1).unwrap();
        assert_eq!(restored.ticket(id).unwrap().queued_at, 100);
        assert_eq!(restored.position_at(2, 202), 1);
        assert_eq!(restored.position_at(3, 202), 2);
        assert_eq!(restored.position_at(id, 202), 3);
        assert_eq!(
            Queue::effective_priority(restored.ticket(id).unwrap(), 2_500_100),
            8
        );
        assert_eq!(restored.reserve_priority(4, 300, 0).unwrap(), 4);
    }
    #[test]
    fn admitted_and_cancelled_intents_are_not_replayed() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal::new(dir.path());
        let mut q = Queue::new(1);
        let a = q.reserve_priority(1, 1, 0).unwrap();
        q.reserve_priority(2, 2, 0).unwrap();
        q.reserve_priority(3, 3, 0).unwrap();
        q.admit(1, a, 4).unwrap();
        q.cancel(2, 5);
        journal.save(&q).unwrap();
        let restored = journal.load(1).unwrap();
        assert!(restored.ticket(1).is_none());
        assert!(restored.ticket(2).is_none());
        assert_eq!(restored.position_at(3, 6), 1);
    }
    #[test]
    fn malformed_or_duplicate_journal_is_not_silently_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal::new(dir.path());
        for bytes in [
            "{broken",
            r#"{"version":2,"generation":0,"waiting":[]}"#,
            r#"{"version":1,"generation":1,"waiting":[{"server":"00000000-0000-0000-0000-000000000001","generation":1,"priority":0,"queued_at":1},{"server":"00000000-0000-0000-0000-000000000001","generation":1,"priority":0,"queued_at":1}]}"#,
        ] {
            std::fs::write(&journal.path, bytes).unwrap();
            assert!(journal.load(4).is_err());
            assert_eq!(std::fs::read_to_string(&journal.path).unwrap(), bytes);
        }
    }
    #[test]
    fn atomic_replace_and_failed_save_preserve_prior_document() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal::new(dir.path());
        let mut q = Queue::new(1);
        q.reserve_priority(1, 1, 0).unwrap();
        journal.save(&q).unwrap();
        q.reserve_priority(2, 2, 7).unwrap();
        journal.save(&q).unwrap();
        assert_eq!(journal.load(1).unwrap().position_at(2, 3), 1);
        let blocker = dir.path().join("blocked");
        std::fs::create_dir(&blocker).unwrap();
        assert!(Journal { path: blocker }.save(&q).is_err());
        assert_eq!(journal.load(1).unwrap().ticket(1).unwrap().queued_at, 1);
    }
}
