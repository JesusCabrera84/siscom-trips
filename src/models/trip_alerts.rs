use chrono::NaiveDateTime;
use sqlx::FromRow;
use uuid::Uuid;

use serde_json::Value;
use sqlx::types::Json;

#[derive(Debug, FromRow)]
#[allow(dead_code)]
pub struct TripAlert {
    pub alert_id: Uuid,
    pub trip_id: Uuid, // DDL says NOT NULL
    pub timestamp: NaiveDateTime,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub alert_type: String,    // Enum in DB, map to String
    pub raw_code: Option<i32>, // DDL says int4
    pub severity: Option<i16>, // DDL says int2
    pub device_id: String,
    pub correlation_id: Option<Uuid>,
    pub metadata: Option<Json<Value>>,
}
