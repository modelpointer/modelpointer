use axum::{Json, http::StatusCode, response::{IntoResponse, Response}};

pub fn create_error(status: StatusCode, code: &str, message: &str) -> Response {
    (status, Json(serde_json::json!({ "error": { "code": code, "message": message } }))).into_response()
}
