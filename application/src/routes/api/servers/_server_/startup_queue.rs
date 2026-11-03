use super::State;
use crate::{
    response::{ApiResponse, ApiResponseResult},
    routes::api::servers::_server_::GetServer,
};
use utoipa_axum::{router::OpenApiRouter, routes};
#[utoipa::path(get,path="/",responses((status=OK,body=serde_json::Value)))]
async fn get(server: GetServer) -> ApiResponseResult {
    crate::server::startup_queue::refresh(&server).await;
    ApiResponse::new_serialized(serde_json::json!({"available":true,"error":crate::server::startup_queue::storage_error(),"queue":crate::server::startup_queue::snapshot(server.uuid)})).ok()
}
#[derive(serde::Deserialize)]
struct CancelParams { queued_at: Option<u64> }
#[utoipa::path(delete,path="/",responses((status=OK,body=serde_json::Value)))]
async fn cancel(server: GetServer, axum::extract::Query(params): axum::extract::Query<CancelParams>) -> ApiResponseResult {
    if !crate::server::startup_queue::try_cancel_ticket(server.uuid, params.queued_at)? {
        return ApiResponse::error("This start has already left the queue")
            .with_status(axum::http::StatusCode::CONFLICT)
            .ok();
    }
    ApiResponse::new_serialized(serde_json::json!({"cancelled":true})).ok()
}
pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(get, cancel))
        .with_state(state.clone())
}
