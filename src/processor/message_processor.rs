use crate::models::message::MqttMessage;
use crate::db::queries;
use sqlx::{Postgres, Row};
use uuid::Uuid;
use chrono::NaiveDateTime;
use tracing::{info, warn};

pub async fn process_message(pool: &sqlx::Pool<Postgres>, payload: &[u8]) -> anyhow::Result<()> {
    // 1. Parse JSON
    let message: MqttMessage = match serde_json::from_slice(payload) {
        Ok(m) => m,
        Err(e) => {
            warn!("Failed to parse message: {}", e);
            return Ok(());
        }
    };

    // 2. Extract Data
    let device_id_str = match message.get_device_id() {
        Some(id) => id.clone(),
        None => {
            warn!("Message missing device_id, skipping");
            return Ok(());
        }
    };

    info!("Processing message for device: {} {:?}\n", device_id_str, message);
    

    let gps_datetime_str = message.data.gps_datetime.as_deref().unwrap_or("");
    let timestamp = match NaiveDateTime::parse_from_str(gps_datetime_str, "%Y-%m-%d %H:%M:%S") {
        Ok(t) => t,
        Err(_) => {
             match NaiveDateTime::parse_from_str(gps_datetime_str, "%Y-%m-%dT%H:%M:%S") {
                 Ok(t) => t,
                 Err(_) => {
                     warn!("Invalid GPS_DATETIME: '{}'", gps_datetime_str);
                     return Ok(());
                 }
             }
        }
    };

    let lat = message.data.latitude.unwrap_or(0.0);
    let lon = message.data.longitude.unwrap_or(0.0);
    let speed = message.data.speed.unwrap_or(0.0);
    let heading = message.data.heading.unwrap_or(0.0);

    let alert_type = message.data.alert.as_deref();
    let alert_type_upper = alert_type.map(|s| s.to_uppercase());
    let is_engine_on = alert_type_upper.as_deref() == Some("ENGINE ON");
    let is_engine_off = alert_type_upper.as_deref() == Some("ENGINE OFF");
    // let msg_class = message.data.msg_class.as_deref(); // Not strictly needed if we focus on alert_type and points

    // 3. Start Transaction
    let mut tx = pool.begin().await?;

    // 4. Get Active Trip State (FOR UPDATE)
    let active_trip_row = sqlx::query(queries::SELECT_ACTIVE_TRIP_ID)
        .bind(&device_id_str)
        .fetch_optional(&mut *tx)
        .await?;

    let (last_trip_id, current_ignition_status): (Option<Uuid>, Option<bool>) = match active_trip_row {
        Some(row) => (
            row.try_get("current_trip_id").ok(),
            row.try_get("ignition_on").ok(),
        ),
        None => (None, None),
    };

    let is_trip_active = current_ignition_status.unwrap_or(false);

    // 5. Evaluate Rules

    // Rule 1: Engine ON -> Start Trip
    if is_engine_on {
        // Only start if no active trip or if we want to enforce a restart (User says: "Cuando llega una alerta... Crear un nuevo trip")
        // But also "Para evitar: Doble creación de trips".
        // If active_trip_id exists, we should probably check if it's stale or just log it.
        // User says: "Si llegan dos mensajes casi al mismo tiempo... Para evitar doble creación".
        // Since we locked the row, we are safe from race conditions.
        // If is_trip_active is true, we ignore the Engine ON to avoid double creation.
        
        if !is_trip_active {
            // Create new trip
            let trip_id = Uuid::parse_str(&message.uuid).unwrap_or_else(|_| Uuid::new_v4());
            info!("Started new trip {} for device {}", trip_id, device_id_str);

            sqlx::query(queries::INSERT_TRIP)
                .bind(trip_id)
                .bind(&device_id_str)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .execute(&mut *tx)
                .await?;

            // Update trip_current_state
            let message_uuid = Uuid::parse_str(&message.uuid).unwrap_or_default();
            sqlx::query(queries::UPDATE_CURRENT_STATE_NEW_TRIP)
                .bind(&device_id_str)
                .bind(trip_id)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .bind(message_uuid)
                .execute(&mut *tx)
                .await?;

            // Insert Alert
            let alert_id = Uuid::new_v4();
            // let correlation_id = ... // Removed as per user request
            let message_uuid = Uuid::parse_str(&message.uuid).unwrap_or_default();

            sqlx::query(queries::INSERT_TRIP_ALERT)
                .bind(alert_id)
                .bind(trip_id)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .bind("ignition_on")
                .bind(message.data.raw_code.as_deref().and_then(|s| s.parse::<i32>().ok()))
                .bind(1i16)
                .bind(&device_id_str)
                .bind(message_uuid)
                .execute(&mut *tx)
                .await?;

            // Insert Point (First point of trip)
            sqlx::query(queries::INSERT_TRIP_POINT)
                .bind(trip_id)
                .bind(&device_id_str)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .bind(speed)
                .bind(heading)
                .bind(true) // Ignition On
                .bind(message_uuid)
                .execute(&mut *tx)
                .await?;
        } else {
            info!("Ignored Engine ON for active trip: {}", device_id_str);
        }
    }
    // Rule 3: Engine Off -> End Trip
    else if is_engine_off {
        if is_trip_active {
            let trip_id = last_trip_id.expect("Active trip must have an ID");
            sqlx::query(queries::UPDATE_TRIP_END)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .bind(trip_id)
                .execute(&mut *tx)
                .await?;

            // Update trip_current_state
            let message_uuid = Uuid::parse_str(&message.uuid).unwrap_or_default();
            sqlx::query(queries::UPDATE_CURRENT_STATE_END_TRIP)
                .bind(&device_id_str)
                .bind(message_uuid)
                .bind(trip_id)
                .execute(&mut *tx)
                .await?;

            info!("Ended trip {} for device {}", trip_id, device_id_str);

            // Insert Alert
            let alert_id = Uuid::new_v4();
            let message_uuid = Uuid::parse_str(&message.uuid).unwrap_or_default();

            sqlx::query(queries::INSERT_TRIP_ALERT)
                .bind(alert_id)
                .bind(trip_id)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .bind("ignition_off")
                .bind(message.data.raw_code.as_deref().and_then(|s| s.parse::<i32>().ok()))
                .bind(1i16)
                .bind(&device_id_str)
                .bind(message_uuid)
                .execute(&mut *tx)
                .await?;
            
            // Insert Point (Last point of trip)
            sqlx::query(queries::INSERT_TRIP_POINT)
                .bind(trip_id)
                .bind(&device_id_str)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .bind(speed)
                .bind(heading)
                .bind(false) // Ignition Off
                .bind(message_uuid)
                .execute(&mut *tx)
                .await?;
        } else {
             info!("Ignored Engine OFF for inactive trip: {}", device_id_str);
        }
    }
    // Rule 2: Points and Other Alerts (During Active Trip)
    else {
        // If we have an active trip, we process points and other alerts
        if is_trip_active {
            let trip_id = last_trip_id.expect("Active trip must have an ID");
            let message_uuid = Uuid::parse_str(&message.uuid).unwrap_or_default();

            // Always insert point if valid (User says: "Siempre que llegue un punto válido")
            // Assuming all messages here are valid points if they have lat/lon/timestamp
            // We'll insert a point for every message that falls through here if it's part of a trip
            
            // Check if it's an alert
            if let Some(alert_name) = alert_type {
                 let mapped_alert = match alert_name {
                    "Power Cut" => Some("power_cut"),
                    "Jamming" => Some("jamming"),
                    "Low Battery" => Some("low_backup_battery"),
                    // Add other mappings as needed
                    _ => None,
                };

                if let Some(valid_alert) = mapped_alert {
                    let alert_id = Uuid::new_v4();
                    sqlx::query(queries::INSERT_TRIP_ALERT)
                        .bind(alert_id)
                        .bind(trip_id)
                        .bind(timestamp)
                        .bind(lat)
                        .bind(lon)
                        .bind(valid_alert)
                        .bind(message.data.raw_code.as_deref().and_then(|s| s.parse::<i32>().ok()))
                        .bind(1i16)
                        .bind(&device_id_str)
                        .bind(message_uuid)
                        .execute(&mut *tx)
                        .await?;
                }
            }

            // Insert Point
            // We assume ignition is ON because we are in an active trip (unless engine status says otherwise, but usually yes)
            let is_ignition_on = current_ignition_status.unwrap_or(true);

            sqlx::query(queries::INSERT_TRIP_POINT)
                .bind(trip_id)
                .bind(&device_id_str)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .bind(speed)
                .bind(heading)
                .bind(is_ignition_on)
                .bind(message_uuid)
                .execute(&mut *tx)
                .await?;

            // Update trip_current_state
            let message_uuid = Uuid::parse_str(&message.uuid).unwrap_or_default();
            sqlx::query(queries::UPDATE_CURRENT_STATE_POINT)
                .bind(&device_id_str)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .bind(speed)
                .bind(message_uuid)
                // .bind(heading) // Removed heading as it is not in trip_current_state DDL
                .execute(&mut *tx)
                .await?;
        }
        // If no active trip, we do nothing (User says: "Los puntos GPS no abren ni cierran trips")
    }

    tx.commit().await?;

    Ok(())
}
