use super::State;
use crate::{
    response::{ApiResponse, ApiResponseResult},
    routes::GetState,
};
use utoipa_axum::{router::OpenApiRouter, routes};
#[utoipa::path(get, path="/", responses((status=OK, body=crate::server::startup_queue::NodeSnapshot)))]
async fn get(state: GetState) -> ApiResponseResult {
    crate::server::startup_queue::observe_servers(
        state.server_manager.get_servers().await.as_slice(),
    );
    ApiResponse::new_serialized(crate::server::startup_queue::node_snapshot(
        state.config.load().limits.server_concurrent_starts,
    ))
    .ok()
}
pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(get))
        .with_state(state.clone())
}
