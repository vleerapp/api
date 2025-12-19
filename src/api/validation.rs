use axum::{
    Json,
    extract::{FromRequest, Request},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::de::DeserializeOwned;
use validator::Validate;

#[derive(Debug, Clone, Copy, Default)]
pub struct ValidatedJson<T>(pub T);

impl<T, S> FromRequest<S> for ValidatedJson<T>
where
    T: DeserializeOwned + Validate,
    S: Send + Sync,
{
    type Rejection = ValidationError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let Json(value) = Json::<T>::from_request(req, state)
            .await
            .map_err(|e| ValidationError::JsonDataError(e.to_string()))?;

        value
            .validate()
            .map_err(|e| ValidationError::ValidationError(e.to_string()))?;

        Ok(ValidatedJson(value))
    }
}

pub enum ValidationError {
    JsonDataError(String),
    ValidationError(String),
}

impl IntoResponse for ValidationError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ValidationError::JsonDataError(msg) => {
                (StatusCode::BAD_REQUEST, format!("Invalid JSON: {}", msg))
            }
            ValidationError::ValidationError(msg) => (
                StatusCode::BAD_REQUEST,
                format!("Validation Failed: {}", msg),
            ),
        };
        (status, message).into_response()
    }
}
