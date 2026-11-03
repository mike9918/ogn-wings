//! Compile the real queue adapter against the narrow server interface it uses.
pub mod state {
    #[derive(Clone, Copy, PartialEq, Eq)]
    pub enum ServerState {
        Offline,
        Starting,
        Running,
    }
    pub struct State(std::sync::atomic::AtomicU8);
    impl State {
        pub fn new() -> Self {
            Self(std::sync::atomic::AtomicU8::new(0))
        }
        pub fn get_state(&self) -> ServerState {
            match self.0.load(std::sync::atomic::Ordering::SeqCst) {
                1 => ServerState::Starting,
                2 => ServerState::Running,
                _ => ServerState::Offline,
            }
        }
        pub fn set(&self, value: u8) {
            self.0.store(value, std::sync::atomic::Ordering::SeqCst);
        }
    }
    impl Default for State {
        fn default() -> Self {
            Self::new()
        }
    }
}
#[derive(Clone)]
pub struct Server {
    uuid: uuid::Uuid,
    state: std::sync::Arc<state::State>,
    app_state: std::sync::Arc<App>,
}
struct App {
    server_manager: Manager,
}
struct Manager {
    servers: tokio::sync::RwLock<Vec<Server>>,
}
impl Manager {
    async fn get_servers(&self) -> tokio::sync::RwLockReadGuard<'_, Vec<Server>> {
        self.servers.read().await
    }
}
#[path = "../../../application/src/server/startup_queue.rs"]
pub mod startup_queue;
#[path = "../../../application/src/server/startup_queue_core.rs"]
pub mod startup_queue_core;

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn admission_waits_cancellation_wakes_and_dropped_reservations_release() {
        let app = std::sync::Arc::new(App {
            server_manager: Manager {
                servers: tokio::sync::RwLock::new(vec![]),
            },
        });
        let servers: Vec<_> = (1..=6)
            .map(|id| Server {
                uuid: uuid::Uuid::from_u128(id),
                state: std::sync::Arc::new(state::State::new()),
                app_state: app.clone(),
            })
            .collect();
        *app.server_manager.servers.write().await = servers.clone();
        let mut admitted = vec![];
        for server in &servers[..4] {
            let r = startup_queue::reserve(server.uuid, 4).unwrap();
            r.acquire(server).await.unwrap();
            server.state.set(1);
            admitted.push(r);
        }
        let server = servers[4].clone();
        let pending = startup_queue::reserve(server.uuid, 4).unwrap();
        let task = tokio::spawn(async move { pending.acquire(&server).await });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!task.is_finished());
        assert_eq!(
            startup_queue::snapshot(servers[4].uuid).unwrap().position,
            1
        );
        assert!(startup_queue::cancel(servers[4].uuid));
        assert!(task.await.unwrap().is_err());
        assert_eq!(
            startup_queue::snapshot(servers[4].uuid).unwrap().phase,
            "cancelled"
        );
        servers[0].state.set(2);
        admitted.remove(0).finish(true);
        let r = startup_queue::reserve(servers[5].uuid, 4).unwrap();
        r.acquire(&servers[5]).await.unwrap();
        drop(r);
        assert_eq!(
            startup_queue::snapshot(servers[5].uuid).unwrap().phase,
            "failed"
        );
        for server in &servers[1..4] {
            server.state.set(2);
        }
        for mut r in admitted {
            r.finish(true);
        }
        startup_queue::refresh(&servers[5]).await;
        assert_eq!(startup_queue::snapshot(servers[5].uuid).unwrap().active, 0);
    }
}
