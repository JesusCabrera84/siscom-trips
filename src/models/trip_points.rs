use chrono::NaiveDateTime;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, FromRow)]
pub struct TripPoint {
    pub point_id: i64, // bigserial
    pub trip_id: Uuid,
    pub device_id: String,
    pub timestamp: NaiveDateTime,
    pub lat: f64,
    pub lng: f64, // DDL says lng
    pub speed: Option<f64>,
    pub heading: Option<f64>,
    pub correlation_id: Uuid,
}
