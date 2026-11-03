use super::startup_queue_core::Queue;
use serde::Serialize;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::Notify;

struct Shared {
    core: Mutex<Queue>,
    changed: Notify,
}
fn shared() -> Arc<Shared> {
    static INSTANCE: OnceLock<Arc<Shared>> = OnceLock::new();
    INSTANCE
        .get_or_init(|| {
            Arc::new(Shared {
                core: Mutex::new(Queue::new(4)),
                changed: Notify::new(),
            })
        })
        .clone()
}
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
pub struct Reservation {
    shared: Arc<Shared>,
    id: u128,
    generation: u64,
    finished: bool,
}
impl Reservation {
    pub async fn acquire(&self, server: &super::Server) -> Result<(), anyhow::Error> {
        loop {
            let notified = self.shared.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let external = server
                .app_state
                .server_manager
                .get_servers()
                .await
                .iter()
                .filter(|s| s.state.get_state() == super::state::ServerState::Starting)
                .map(|s| s.uuid.as_u128())
                .collect::<Vec<_>>();
            let admitted = {
                let mut core = self.shared.core.lock().unwrap_or_else(|p| p.into_inner());
                core.set_external(external);
                core.admit(self.id, self.generation, now())
                    .map_err(anyhow::Error::msg)?
            };
            if admitted {
                self.shared.changed.notify_waiters();
                return Ok(());
            }
            tokio::select! {_=notified=>{},_=tokio::time::sleep(std::time::Duration::from_secs(1))=>{}}
        }
    }
    pub fn attention(&self) {
        self.shared
            .core
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .attention(self.id, self.generation);
    }
    pub fn finish(&mut self, running: bool) {
        self.shared
            .core
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .finish(self.id, self.generation, now(), running);
        self.finished = true;
        self.shared.changed.notify_waiters();
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.finished {
            self.finish(false);
        }
    }
}
#[cfg(test)]
pub fn reserve(id: uuid::Uuid, limit: usize) -> Result<Reservation, anyhow::Error> {
    reserve_priority(id, limit, 0)
}
pub fn reserve_priority(
    id: uuid::Uuid,
    limit: usize,
    priority: u8,
) -> Result<Reservation, anyhow::Error> {
    let shared = shared();
    let generation = {
        let mut core = shared.core.lock().unwrap_or_else(|p| p.into_inner());
        core.set_limit(limit);
        core.reserve_priority(id.as_u128(), now(), priority)
            .map_err(anyhow::Error::msg)?
    };
    shared.changed.notify_waiters();
    Ok(Reservation {
        shared,
        id: id.as_u128(),
        generation,
        finished: false,
    })
}
pub fn cancel(id: uuid::Uuid) -> bool {
    cancel_ticket(id, None)
}
pub fn cancel_ticket(id: uuid::Uuid, queued_at: Option<u64>) -> bool {
    let s = shared();
    let cancelled = {
        let mut q = s.core.lock().unwrap_or_else(|p| p.into_inner());
        if queued_at.is_some_and(|time| q.ticket(id.as_u128()).is_none_or(|e| e.queued_at != time)) {
            false
        } else { q.cancel(id.as_u128(), now()) }
    };
    if cancelled {
        s.changed.notify_waiters();
    }
    cancelled
}
pub async fn refresh(server: &super::Server) {
    observe_servers(
        server
            .app_state
            .server_manager
            .get_servers()
            .await
            .as_slice(),
    );
}
pub fn observe_servers(servers: &[super::Server]) {
    let ids = servers
        .iter()
        .filter(|s| s.state.get_state() == super::state::ServerState::Starting)
        .map(|s| s.uuid.as_u128())
        .collect::<Vec<_>>();
    shared()
        .core
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .set_external(ids);
}
#[derive(Serialize, utoipa::ToSchema)]
pub struct Snapshot {
    pub priority: u8,
    pub effective_priority: u64,
    pub phase: &'static str,
    pub position: usize,
    pub limit: usize,
    pub active: usize,
    pub queued_at: u64,
    pub started_at: Option<u64>,
    pub finished_at: Option<u64>,
    pub server_time: u64,
}
pub fn snapshot(id: uuid::Uuid) -> Option<Snapshot> {
    let s = shared();
    let q = s.core.lock().unwrap_or_else(|p| p.into_inner());
    let e = q.ticket(id.as_u128())?;
    Some(snapshot_locked(&q, id.as_u128(), e, now()))
}
fn snapshot_locked(
    q: &Queue,
    id: u128,
    e: &super::startup_queue_core::Ticket,
    time: u64,
) -> Snapshot {
    Snapshot {
        priority: e.priority,
        effective_priority: Queue::effective_priority(
            e,
            if e.phase == super::startup_queue_core::Phase::Queued {
                time
            } else {
                e.started_at.unwrap_or(e.queued_at)
            },
        ),
        phase: e.phase.name(),
        position: q.position_at(id, time),
        limit: q.limit(),
        active: q.active(),
        queued_at: e.queued_at,
        started_at: e.started_at,
        finished_at: e.finished_at,
        server_time: time,
    }
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct NodeEntry {
    pub server_uuid: String,
    pub recovered: bool,
    #[serde(flatten)]
    pub status: Snapshot,
}
#[derive(Serialize, utoipa::ToSchema)]
pub struct NodeSnapshot {
    pub available: bool,
    pub priority_supported: bool,
    pub limit: usize,
    pub active: usize,
    pub queued: usize,
    pub server_time: u64,
    pub entries: Vec<NodeEntry>,
}
pub fn node_snapshot(limit: usize) -> NodeSnapshot {
    let shared = shared();
    let mut q = shared.core.lock().unwrap_or_else(|p| p.into_inner());
    q.set_limit(limit);
    let time = now();
    let mut entries: Vec<_> = q
        .entries()
        .map(|(id, e)| NodeEntry {
            server_uuid: uuid::Uuid::from_u128(*id).to_string(),
            recovered: false,
            status: snapshot_locked(&q, *id, e, time),
        })
        .collect();
    for id in q.external() {
        if !q.ticket(*id).is_some_and(|e| {
            matches!(
                e.phase,
                super::startup_queue_core::Phase::Starting
                    | super::startup_queue_core::Phase::Attention
            )
        }) {
            entries.retain(|e| e.server_uuid != uuid::Uuid::from_u128(*id).to_string());
            entries.push(NodeEntry {
                server_uuid: uuid::Uuid::from_u128(*id).to_string(),
                recovered: true,
                status: Snapshot {
                    priority: 0,
                    effective_priority: 0,
                    phase: "starting",
                    position: 0,
                    limit: q.limit(),
                    active: q.active(),
                    queued_at: time,
                    started_at: None,
                    finished_at: None,
                    server_time: time,
                },
            });
        }
    }
    let queued = entries
        .iter()
        .filter(|e| e.status.phase == "queued")
        .count();
    entries.sort_by_key(|e| match e.status.phase {
        "starting" | "attention_required" => (0, e.status.started_at.unwrap_or(0)),
        "queued" => (1, e.status.position as u64),
        _ => (2, u64::MAX - e.status.finished_at.unwrap_or(0)),
    });
    // Current work is complete; recent history is bounded for polling payloads.
    let mut history = 0;
    entries.retain(|e| {
        if matches!(e.status.phase, "queued" | "starting" | "attention_required") {
            true
        } else {
            history += 1;
            history <= 50
        }
    });
    NodeSnapshot {
        available: true,
        priority_supported: true,
        limit: q.limit(),
        active: q.active(),
        queued,
        server_time: time,
        entries,
    }
}
