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
pub fn reserve(id: uuid::Uuid, limit: usize) -> Result<Reservation, anyhow::Error> {
    let shared = shared();
    let generation = {
        let mut core = shared.core.lock().unwrap_or_else(|p| p.into_inner());
        core.set_limit(limit);
        core.reserve(id.as_u128(), now())
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
    let s = shared();
    let cancelled = s
        .core
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .cancel(id.as_u128(), now());
    if cancelled {
        s.changed.notify_waiters();
    }
    cancelled
}
pub async fn refresh(server: &super::Server) {
    let ids = server
        .app_state
        .server_manager
        .get_servers()
        .await
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
    Some(Snapshot {
        phase: e.phase.name(),
        position: q.position(id.as_u128()),
        limit: q.limit(),
        active: q.active(),
        queued_at: e.queued_at,
        started_at: e.started_at,
        finished_at: e.finished_at,
        server_time: now(),
    })
}
