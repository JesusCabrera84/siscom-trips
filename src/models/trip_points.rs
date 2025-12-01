use sqlx::FromRow;
use chrono::NaiveDateTime;
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
    pub ignition_on: Option<bool>,
    pub correlation_id: Uuid,
}
