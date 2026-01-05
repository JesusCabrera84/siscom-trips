use chrono::NaiveDateTime;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, FromRow)]
pub struct Trip {
    pub trip_id: Uuid,
    pub device_id: Uuid, // DDL says uuid
    pub start_time: NaiveDateTime,
    pub start_lat: Option<f64>, // DDL says float8 NULL
    pub start_lng: Option<f64>, // DDL says float8 NULL
    pub end_time: Option<NaiveDateTime>,
    pub end_lat: Option<f64>,
    pub end_lng: Option<f64>,
    pub distance_meters: Option<f64>,
    pub start_odometer_meters: Option<i32>,
    pub end_odometer_meters: Option<i32>,
}
