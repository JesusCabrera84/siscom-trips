use crate::models::message::MqttMessage;
use crate::db::queries;
use sqlx::{Postgres, Row};
use uuid::Uuid;
use chrono::NaiveDateTime;
use tracing::{info, warn, error};

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

    info!("Processing message for device: {} uuid: {}\n", device_id_str, message.uuid);

    let message_uuid = Uuid::parse_str(&message.uuid).unwrap_or_else(|_| Uuid::new_v4());

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
    // let heading = message.data.heading.unwrap_or(0.0); // Not used in current logic

    let alert_type = message.data.alert.as_deref();
    let alert_type_upper = alert_type.map(|s| s.to_uppercase());
    let is_engine_on = alert_type_upper.as_deref() == Some("ENGINE ON");
    let is_engine_off = alert_type_upper.as_deref() == Some("ENGINE OFF");

    // 3. Start Transaction
    let mut tx = pool.begin().await?;

    // 4. Get Active Trip State (FOR UPDATE)
    let active_trip_row = sqlx::query(queries::SELECT_ACTIVE_TRIP_ID)
        .bind(&device_id_str)
        .fetch_optional(&mut *tx)
        .await?;

    let (mut last_trip_id, current_ignition_status): (Option<Uuid>, Option<bool>) = match active_trip_row {
        Some(row) => (
            row.try_get("current_trip_id").ok(),
            row.try_get("ignition_on").ok(),
        ),
        None => (None, None),
    };

    // Rule: ignition_on = true cuando hay viaje activo
    let is_trip_active = current_ignition_status.unwrap_or(false);

    // If trip is active but we don't have the ID (because it's NULL in current state), fetch it from trips table
    if is_trip_active && last_trip_id.is_none() {
        let open_trip_row = sqlx::query(queries::SELECT_LATEST_OPEN_TRIP)
            .bind(&device_id_str)
            .fetch_optional(&mut *tx)
            .await?;
        
        if let Some(row) = open_trip_row {
            last_trip_id = row.try_get("trip_id").ok();
        }
    }

    // 5. Update Current State (Always update last point info)
    // "Siempre actualizar last_point_at, last_lat, last_lng, last_speed"
    // Note: We use UPDATE_CURRENT_STATE_POINT for this generic update.
    // However, if we are creating a NEW trip, we use UPDATE_CURRENT_STATE_NEW_TRIP which also sets these.
    // If we are ENDING a trip, we use UPDATE_CURRENT_STATE_END_TRIP.
    // So we will do specific updates inside the rules, OR a generic one if no state change?
    // The prompt says "Detectar ENGINE_ON -> ... -> actualizar current_state".
    // So we should probably do it as part of the specific actions to avoid double updates.
    // BUT, for "Si es punto válido -> trip_points + actualizar current_state".
    
    // Let's handle the logic flow:

    if is_engine_on {
        // Detectar ENGINE_ON -> crear trip -> alert -> actualizar current_state
        if !is_trip_active {
            let trip_id = message_uuid; // Use message UUID as trip ID
            info!("Started new trip {} for device {}", trip_id, device_id_str);

            // Create Trip
            sqlx::query(queries::INSERT_TRIP)
                .bind(trip_id)
                .bind(&device_id_str)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .execute(&mut *tx)
                .await?;

            // Update Current State (New Trip)
            // Sets ignition_on = true, current_trip_id = NULL
            sqlx::query(queries::UPDATE_CURRENT_STATE_NEW_TRIP)
                .bind(&device_id_str)
                .bind(trip_id) // This param is used for last_point logic if needed, but query sets current_trip_id=NULL
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .bind(message_uuid)
                .execute(&mut *tx)
                .await?;

            // Insert Alert (ignition_on)
            let alert_id = Uuid::new_v4();
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
        } else {
            info!("Ignored Engine ON for active trip: {}", device_id_str);
            // Even if ignored, we should probably update last_point info?
            // User says "Siempre actualizar...".
            sqlx::query(queries::UPDATE_CURRENT_STATE_POINT)
                .bind(&device_id_str)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .bind(speed)
                .bind(message_uuid)
                .execute(&mut *tx)
                .await?;
        }
    } else if is_engine_off {
        // Detectar ENGINE_OFF -> cerrar trip -> alert -> limpiar current_state
        if is_trip_active {
            if let Some(trip_id) = last_trip_id {
                info!("Ended trip {} for device {}", trip_id, device_id_str);

                // Close Trip
                sqlx::query(queries::UPDATE_TRIP_END)
                    .bind(timestamp)
                    .bind(lat)
                    .bind(lon)
                    .bind(trip_id)
                    .execute(&mut *tx)
                    .await?;

                // Update Current State (End Trip)
                // Sets ignition_on = false, current_trip_id = NULL
                sqlx::query(queries::UPDATE_CURRENT_STATE_END_TRIP)
                    .bind(&device_id_str)
                    .bind(message_uuid)
                    .bind(timestamp)
                    .bind(lat)
                    .bind(lon)
                    .bind(speed)
                    .execute(&mut *tx)
                    .await?;

                // Insert Alert (ignition_off)
                let alert_id = Uuid::new_v4();
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
            } else {
                error!("Active trip detected but no trip_id found for device {}", device_id_str);
            }
        } else {
             info!("Ignored Engine OFF for inactive trip: {}", device_id_str);
             // Update last point info
             sqlx::query(queries::UPDATE_CURRENT_STATE_POINT)
                .bind(&device_id_str)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .bind(speed)
                .bind(message_uuid)
                .execute(&mut *tx)
                .await?;
        }
    } else if let Some(alert_name) = alert_type {
        // Detectar otras alertas -> solo trip_alerts
        // "no mapes las alartas, en su lugar, usa el valor de alert_type"
        // "Si este viene vacio, considera k no es valida y no proceses la alerta"

        if !alert_name.trim().is_empty() {
            // We need a trip_id to insert alert.
            // If active, use last_trip_id.
            // If inactive, we cannot insert into trip_alerts (NOT NULL constraint).
            if let Some(trip_id) = last_trip_id {
                let alert_id = Uuid::new_v4();
                sqlx::query(queries::INSERT_TRIP_ALERT)
                    .bind(alert_id)
                    .bind(trip_id)
                    .bind(timestamp)
                    .bind(lat)
                    .bind(lon)
                    .bind(alert_name) // Use raw alert name
                    .bind(message.data.raw_code.as_deref().and_then(|s| s.parse::<i32>().ok()))
                    .bind(1i16)
                    .bind(&device_id_str)
                    .bind(message_uuid)
                    .execute(&mut *tx)
                    .await?;
            } else {
                warn!("Cannot insert alert '{}' because no active trip found for device {}", alert_name, device_id_str);
            }
        } else {
             warn!("Empty alert type received for device {}", device_id_str);
        }

        // Update last point info (Siempre actualizar)
        sqlx::query(queries::UPDATE_CURRENT_STATE_POINT)
            .bind(&device_id_str)
            .bind(timestamp)
            .bind(lat)
            .bind(lon)
            .bind(speed)
            .bind(message_uuid)
            .execute(&mut *tx)
            .await?;

    } else {
        // Si es punto válido -> trip_points + actualizar current_state
        // "SOLO insertar en trip_points si: Es un mensaje sin alert... Existe un viaje activo"
        
        if is_trip_active {
             if let Some(trip_id) = last_trip_id {
                // Insert Point
                // "trip_points nunca debe tener un campo de ignición" -> Pass None
                sqlx::query(queries::INSERT_TRIP_POINT)
                    .bind(trip_id)
                    .bind(&device_id_str)
                    .bind(timestamp)
                    .bind(lat)
                    .bind(lon)
                    .bind(speed)
                    .bind(message.data.heading.unwrap_or(0.0))
                    .bind(message_uuid)
                    .execute(&mut *tx)
                    .await?;
             }
        }

        // Update last point info (Siempre actualizar)
        sqlx::query(queries::UPDATE_CURRENT_STATE_POINT)
            .bind(&device_id_str)
            .bind(timestamp)
            .bind(lat)
            .bind(lon)
            .bind(speed)
            .bind(message_uuid)
            .execute(&mut *tx)
            .await?;
    }

    tx.commit().await?;

    Ok(())
}
