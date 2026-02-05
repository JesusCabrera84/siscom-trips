use crate::db::queries;
use crate::models::siscom::v1::KafkaMessage;
use chrono::{TimeZone, Utc};
use prost::Message;
use sqlx::{Postgres, Row};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// ... (is_ignition_on, is_ignition_off, determine_destination, MessageDestination remains)

/// Detecta si el mensaje es un evento de encendido (ignition on)
/// Soporta múltiples formatos de diferentes fabricantes:
/// - "ENGINE ON" (formato genérico)
/// - "TURN ON" (Queclink)
pub fn is_ignition_on(alert: Option<&str>) -> bool {
    match alert.map(|s| s.to_uppercase()) {
        Some(ref s) => matches!(s.as_str(), "ENGINE ON" | "TURN ON"),
        None => false,
    }
}

/// Detecta si el mensaje es un evento de apagado (ignition off)
/// Soporta múltiples formatos de diferentes fabricantes:
/// - "ENGINE OFF" (formato genérico)
/// - "TURN OFF" (Queclink)
pub fn is_ignition_off(alert: Option<&str>) -> bool {
    match alert.map(|s| s.to_uppercase()) {
        Some(ref s) => matches!(s.as_str(), "ENGINE OFF" | "TURN OFF"),
        None => false,
    }
}

/// Determina el destino de un mensaje basado en el estado del viaje y el tipo de alerta
#[derive(Debug, Clone, PartialEq)]
pub enum MessageDestination {
    /// Crear nuevo viaje (ignition on sin viaje activo)
    NewTrip,
    /// Cerrar viaje existente (ignition off con viaje activo)
    EndTrip,
    /// Agregar punto al viaje activo
    TripPoint,
    /// Agregar alerta al viaje activo
    TripAlert,
    /// Guardar en actividad idle (sin viaje activo y no es ignition)
    IdleActivity,
    /// Ignition on ignorado (ya hay viaje activo)
    IgnoredIgnitionOn,
    /// Ignition off ignorado (no hay viaje activo)
    IgnoredIgnitionOff,
}

/// Determina a dónde debe ir un mensaje basado en el estado actual
pub fn determine_destination(alert: Option<&str>, is_trip_active: bool) -> MessageDestination {
    let engine_on = is_ignition_on(alert);
    let engine_off = is_ignition_off(alert);

    if engine_on {
        if !is_trip_active {
            MessageDestination::NewTrip
        } else {
            MessageDestination::IgnoredIgnitionOn
        }
    } else if engine_off {
        if is_trip_active {
            MessageDestination::EndTrip
        } else {
            MessageDestination::IgnoredIgnitionOff
        }
    } else if is_trip_active {
        match alert {
            Some(a) if !a.trim().is_empty() => MessageDestination::TripAlert,
            _ => MessageDestination::TripPoint,
        }
    } else {
        MessageDestination::IdleActivity
    }
}

pub async fn process_message(pool: &sqlx::Pool<Postgres>, payload: &[u8]) -> anyhow::Result<()> {
    // 1. Parse Protobuf
    let message = match KafkaMessage::decode(payload) {
        Ok(m) => m,
        Err(e) => {
            warn!("Failed to decode Protobuf KafkaMessage: {}", e);
            return Ok(());
        }
    };

    // 2. Extract Data
    let device_id_str = message.data.get("DEVICE_ID").cloned().unwrap_or_default();
    if device_id_str.is_empty() {
        warn!("Message missing DEVICE_ID in data map, skipping");
        return Ok(());
    }

    info!(
        "Processing Protobuf message for device: {} uuid: {}\n",
        device_id_str, message.uuid
    );

    let message_uuid = Uuid::parse_str(&message.uuid).unwrap_or_else(|_| Uuid::new_v4());

    // Use GPS_EPOCH if available, otherwise fallback to decoded_epoch or current time
    let timestamp = if let Some(epoch_str) = message.data.get("GPS_EPOCH") {
        if let Ok(epoch) = epoch_str.parse::<i64>() {
            Utc.timestamp_opt(epoch, 0).single().map(|t| t.naive_utc())
        } else {
            None
        }
    } else {
        None
    }
    .unwrap_or_else(|| {
        if let Some(metadata) = message.metadata.as_ref() {
            if metadata.decoded_epoch > 0 {
                return Utc
                    .timestamp_millis_opt(metadata.decoded_epoch as i64)
                    .single()
                    .map(|t| t.naive_utc())
                    .unwrap_or_else(|| Utc::now().naive_utc());
            }
        }
        Utc::now().naive_utc()
    });

    let lat = message
        .data
        .get("LATITUD")
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let lon = message
        .data
        .get("LONGITUD")
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let speed = message
        .data
        .get("SPEED")
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let odometer_meters = message
        .data
        .get("ODOMETER")
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let heading = message
        .data
        .get("COURSE")
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);

    let alert_type = message.data.get("ALERT").map(|s| s.as_str());

    // 3. Start Transaction
    let mut tx = pool.begin().await?;

    // 4. Get Active Trip State (FOR UPDATE)
    let active_trip_row = sqlx::query(queries::SELECT_ACTIVE_TRIP_ID)
        .bind(&device_id_str)
        .fetch_optional(&mut *tx)
        .await?;

    let (mut last_trip_id, current_ignition_status): (Option<Uuid>, Option<bool>) =
        match active_trip_row {
            Some(row) => (
                row.try_get("current_trip_id").ok(),
                row.try_get("ignition_on").ok(),
            ),
            None => (None, None),
        };

    // Rule: ignition_on = true cuando hay viaje activo
    let is_trip_active = current_ignition_status.unwrap_or(false);

    // If trip is active but we don't have the ID, fetch it
    if is_trip_active && last_trip_id.is_none() {
        let open_trip_row = sqlx::query(queries::SELECT_LATEST_OPEN_TRIP)
            .bind(&device_id_str)
            .fetch_optional(&mut *tx)
            .await?;

        if let Some(row) = open_trip_row {
            last_trip_id = row.try_get("trip_id").ok();
        }
    }

    // 5. Determine Destination and Process
    let destination = determine_destination(alert_type, is_trip_active);
    debug!(
        "Message destination for {}: {:?}",
        device_id_str, destination
    );

    match destination {
        MessageDestination::NewTrip => {
            let trip_id = message_uuid;
            info!("Started new trip {} for device {}", trip_id, device_id_str);

            sqlx::query(queries::INSERT_TRIP)
                .bind(trip_id)
                .bind(&device_id_str)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .bind(odometer_meters)
                .execute(&mut *tx)
                .await?;

            sqlx::query(queries::UPDATE_CURRENT_STATE_NEW_TRIP)
                .bind(&device_id_str)
                .bind(trip_id)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .bind(message_uuid)
                .bind(odometer_meters)
                .execute(&mut *tx)
                .await?;

            let alert_id = Uuid::new_v4();
            sqlx::query(queries::INSERT_TRIP_ALERT)
                .bind(alert_id)
                .bind(trip_id)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .bind("ignition_on")
                .bind(
                    message
                        .data
                        .get("RAW_CODE")
                        .and_then(|s| s.parse::<i32>().ok()),
                )
                .bind(1i16)
                .bind(&device_id_str)
                .bind(message_uuid)
                .execute(&mut *tx)
                .await?;
        }
        MessageDestination::EndTrip => {
            if let Some(trip_id) = last_trip_id {
                info!("Ended trip {} for device {}", trip_id, device_id_str);

                sqlx::query(queries::UPDATE_TRIP_END)
                    .bind(timestamp)
                    .bind(lat)
                    .bind(lon)
                    .bind(odometer_meters)
                    .bind(trip_id)
                    .execute(&mut *tx)
                    .await?;

                sqlx::query(queries::UPDATE_CURRENT_STATE_END_TRIP)
                    .bind(&device_id_str)
                    .bind(message_uuid)
                    .bind(timestamp)
                    .bind(lat)
                    .bind(lon)
                    .bind(speed)
                    .execute(&mut *tx)
                    .await?;

                let alert_id = Uuid::new_v4();
                sqlx::query(queries::INSERT_TRIP_ALERT)
                    .bind(alert_id)
                    .bind(trip_id)
                    .bind(timestamp)
                    .bind(lat)
                    .bind(lon)
                    .bind("ignition_off")
                    .bind(
                        message
                            .data
                            .get("RAW_CODE")
                            .and_then(|s| s.parse::<i32>().ok()),
                    )
                    .bind(1i16)
                    .bind(&device_id_str)
                    .bind(message_uuid)
                    .execute(&mut *tx)
                    .await?;
            } else {
                error!(
                    "Active trip state without trip_id for end trip: {}",
                    device_id_str
                );
            }
        }
        MessageDestination::TripAlert => {
            if let Some(trip_id) = last_trip_id {
                let alert_id = Uuid::new_v4();
                sqlx::query(queries::INSERT_TRIP_ALERT)
                    .bind(alert_id)
                    .bind(trip_id)
                    .bind(timestamp)
                    .bind(lat)
                    .bind(lon)
                    .bind(alert_type.unwrap_or(""))
                    .bind(
                        message
                            .data
                            .get("RAW_CODE")
                            .and_then(|s| s.parse::<i32>().ok()),
                    )
                    .bind(1i16)
                    .bind(&device_id_str)
                    .bind(message_uuid)
                    .execute(&mut *tx)
                    .await?;
            }

            sqlx::query(queries::UPDATE_CURRENT_STATE_POINT)
                .bind(&device_id_str)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .bind(speed)
                .bind(message_uuid)
                .bind(odometer_meters)
                .execute(&mut *tx)
                .await?;
        }
        MessageDestination::TripPoint => {
            if let Some(trip_id) = last_trip_id {
                sqlx::query(queries::INSERT_TRIP_POINT)
                    .bind(trip_id)
                    .bind(&device_id_str)
                    .bind(timestamp)
                    .bind(lat)
                    .bind(lon)
                    .bind(lon)
                    .bind(speed)
                    .bind(heading)
                    .bind(odometer_meters)
                    .bind(message_uuid)
                    .execute(&mut *tx)
                    .await?;
            }

            sqlx::query(queries::UPDATE_CURRENT_STATE_POINT)
                .bind(&device_id_str)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .bind(speed)
                .bind(message_uuid)
                .bind(odometer_meters)
                .execute(&mut *tx)
                .await?;
        }
        MessageDestination::IdleActivity => {
            let idle_id = Uuid::new_v4();
            let activity_type = alert_type.unwrap_or("gps_idle_point");

            let metadata_json = if let Some(m) = message.metadata {
                serde_json::json!({
                    "worker_id": m.worker_id,
                    "received_epoch": m.received_epoch,
                    "decoded_epoch": m.decoded_epoch,
                    "bytes": m.bytes,
                    "client_ip": m.client_ip,
                    "client_port": m.client_port
                })
            } else {
                serde_json::Value::Null
            };

            sqlx::query(queries::INSERT_DEVICE_IDLE_ACTIVITY)
                .bind(idle_id)
                .bind(&device_id_str)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .bind(activity_type)
                .bind(
                    message
                        .data
                        .get("RAW_CODE")
                        .and_then(|s| s.parse::<i32>().ok()),
                )
                .bind(1i16)
                .bind(metadata_json)
                .bind(message_uuid)
                .execute(&mut *tx)
                .await?;

            sqlx::query(queries::UPDATE_CURRENT_STATE_POINT)
                .bind(&device_id_str)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .bind(speed)
                .bind(message_uuid)
                .bind(odometer_meters)
                .execute(&mut *tx)
                .await?;
        }
        MessageDestination::IgnoredIgnitionOn | MessageDestination::IgnoredIgnitionOff => {
            info!(
                "Ignored ignition event ({:?}) for device {}",
                destination, device_id_str
            );
            sqlx::query(queries::UPDATE_CURRENT_STATE_POINT)
                .bind(&device_id_str)
                .bind(timestamp)
                .bind(lat)
                .bind(lon)
                .bind(speed)
                .bind(message_uuid)
                .bind(odometer_meters)
                .execute(&mut *tx)
                .await?;
        }
    }

    tx.commit().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==================== Tests de detección de ignition ====================

    #[test]
    fn test_is_ignition_on_engine_on() {
        assert!(is_ignition_on(Some("ENGINE ON")));
        assert!(is_ignition_on(Some("engine on")));
        assert!(is_ignition_on(Some("Engine On")));
    }

    #[test]
    fn test_is_ignition_on_turn_on_queclink() {
        assert!(is_ignition_on(Some("TURN ON")));
        assert!(is_ignition_on(Some("turn on")));
        assert!(is_ignition_on(Some("Turn On")));
    }

    #[test]
    fn test_is_ignition_on_negative_cases() {
        assert!(!is_ignition_on(None));
        assert!(!is_ignition_on(Some("")));
        assert!(!is_ignition_on(Some("ENGINE OFF")));
        assert!(!is_ignition_on(Some("TURN OFF")));
        assert!(!is_ignition_on(Some("SPEEDING")));
        assert!(!is_ignition_on(Some("LOW BATTERY")));
    }

    #[test]
    fn test_is_ignition_off_engine_off() {
        assert!(is_ignition_off(Some("ENGINE OFF")));
        assert!(is_ignition_off(Some("engine off")));
        assert!(is_ignition_off(Some("Engine Off")));
    }

    #[test]
    fn test_is_ignition_off_turn_off_queclink() {
        assert!(is_ignition_off(Some("TURN OFF")));
        assert!(is_ignition_off(Some("turn off")));
        assert!(is_ignition_off(Some("Turn Off")));
    }

    #[test]
    fn test_is_ignition_off_negative_cases() {
        assert!(!is_ignition_off(None));
        assert!(!is_ignition_off(Some("")));
        assert!(!is_ignition_off(Some("ENGINE ON")));
        assert!(!is_ignition_off(Some("TURN ON")));
        assert!(!is_ignition_off(Some("SPEEDING")));
    }

    // ==================== Tests de destino de mensajes ====================

    #[test]
    fn test_destination_queclink_turn_on_no_active_trip() {
        // Queclink "Turn On" sin viaje activo -> debe crear nuevo trip
        let dest = determine_destination(Some("Turn On"), false);
        assert_eq!(dest, MessageDestination::NewTrip);
    }

    #[test]
    fn test_destination_queclink_turn_on_with_active_trip() {
        // Queclink "Turn On" con viaje activo -> ignorar
        let dest = determine_destination(Some("Turn On"), true);
        assert_eq!(dest, MessageDestination::IgnoredIgnitionOn);
    }

    #[test]
    fn test_destination_queclink_turn_off_with_active_trip() {
        // Queclink "Turn Off" con viaje activo -> cerrar trip
        let dest = determine_destination(Some("Turn Off"), true);
        assert_eq!(dest, MessageDestination::EndTrip);
    }

    #[test]
    fn test_destination_queclink_turn_off_no_active_trip() {
        // Queclink "Turn Off" sin viaje activo -> ignorar
        let dest = determine_destination(Some("Turn Off"), false);
        assert_eq!(dest, MessageDestination::IgnoredIgnitionOff);
    }

    #[test]
    fn test_destination_engine_on_no_active_trip() {
        // ENGINE ON sin viaje activo -> crear nuevo trip
        let dest = determine_destination(Some("ENGINE ON"), false);
        assert_eq!(dest, MessageDestination::NewTrip);
    }

    #[test]
    fn test_destination_engine_off_with_active_trip() {
        // ENGINE OFF con viaje activo -> cerrar trip
        let dest = determine_destination(Some("ENGINE OFF"), true);
        assert_eq!(dest, MessageDestination::EndTrip);
    }

    #[test]
    fn test_destination_alert_with_active_trip() {
        // Alerta (ej: SPEEDING) con viaje activo -> agregar como alerta al trip
        let dest = determine_destination(Some("SPEEDING"), true);
        assert_eq!(dest, MessageDestination::TripAlert);

        let dest = determine_destination(Some("LOW BATTERY"), true);
        assert_eq!(dest, MessageDestination::TripAlert);
    }

    #[test]
    fn test_destination_no_alert_with_active_trip() {
        // Sin alerta con viaje activo -> agregar punto al trip
        let dest = determine_destination(None, true);
        assert_eq!(dest, MessageDestination::TripPoint);

        let dest = determine_destination(Some(""), true);
        assert_eq!(dest, MessageDestination::TripPoint);

        let dest = determine_destination(Some("   "), true);
        assert_eq!(dest, MessageDestination::TripPoint);
    }

    #[test]
    fn test_destination_no_alert_no_active_trip() {
        // Sin alerta y sin viaje activo -> idle activity
        let dest = determine_destination(None, false);
        assert_eq!(dest, MessageDestination::IdleActivity);
    }

    #[test]
    fn test_destination_other_alert_no_active_trip() {
        // Otra alerta sin viaje activo -> idle activity
        let dest = determine_destination(Some("LOW BATTERY"), false);
        assert_eq!(dest, MessageDestination::IdleActivity);

        let dest = determine_destination(Some("SPEEDING"), false);
        assert_eq!(dest, MessageDestination::IdleActivity);
    }

    // ==================== Test del mensaje específico de Queclink ====================

    #[test]
    fn test_queclink_gtvgn_message_should_create_trip() {
        // Este es el mensaje real de Queclink que estaba fallando
        let alert = Some("Turn On");
        let is_trip_active = false;

        let dest = determine_destination(alert, is_trip_active);

        assert_eq!(
            dest,
            MessageDestination::NewTrip,
            "El mensaje GTVGN de Queclink con 'Turn On' debe crear un nuevo trip"
        );
    }
}
