use axum::body::Body;
use governor::middleware::NoOpMiddleware;
use tower_governor::{
    GovernorLayer, governor::GovernorConfigBuilder, key_extractor::SmartIpKeyExtractor,
};

pub type QuotaLayer = GovernorLayer<SmartIpKeyExtractor, NoOpMiddleware, Body>;

pub fn rate_limit(requests: u32, duration_ms: u64) -> QuotaLayer {
    let period = if requests > 0 {
        duration_ms / (requests as u64)
    } else {
        duration_ms
    };

    let config = GovernorConfigBuilder::default()
        .per_millisecond(period)
        .burst_size(requests)
        .key_extractor(SmartIpKeyExtractor)
        .finish()
        .expect("Failed to create rate limit config");

    GovernorLayer::new(config)
}
