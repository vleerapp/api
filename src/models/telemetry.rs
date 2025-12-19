use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use time::OffsetDateTime;
use uuid::Uuid;
use validator::{Validate, ValidationError};

static SEMVER_REGEX: OnceLock<Regex> = OnceLock::new();

fn validate_semver(version: &str) -> Result<(), ValidationError> {
    let regex = SEMVER_REGEX.get_or_init(|| Regex::new(r"^\d+\.\d+\.\d+$").unwrap());

    if regex.is_match(version) {
        Ok(())
    } else {
        Err(ValidationError::new("invalid_semver_format"))
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy)]
pub enum Os {
    Linux,
    #[serde(rename = "macOS")]
    MacOS,
    Windows,
}

impl Os {
    pub fn as_str(&self) -> &'static str {
        match self {
            Os::Linux => "Linux",
            Os::MacOS => "macOS",
            Os::Windows => "Windows",
        }
    }
}

#[derive(Deserialize, Validate)]
pub struct TelemetrySubmission {
    pub user_id: Uuid,

    #[validate(custom(function = "validate_semver"))]
    pub app_version: String,

    pub os: Os,

    #[validate(range(min = 0))]
    pub song_count: i64,
}

#[derive(Serialize)]
pub struct TelemetryStat {
    #[serde(with = "time::serde::rfc3339")]
    pub bucket: OffsetDateTime,
    pub os: String,
    pub avg_songs: f64,
    pub user_count: i64,
}
