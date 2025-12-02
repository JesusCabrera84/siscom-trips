pub const SELECT_ACTIVE_TRIP_ID: &str = r#"
SELECT current_trip_id, ignition_on FROM trip_current_state WHERE device_id = $1 FOR UPDATE;
"#;

pub const SELECT_LATEST_OPEN_TRIP: &str = r#"
SELECT trip_id FROM trips WHERE device_id = $1 AND end_time IS NULL ORDER BY start_time DESC LIMIT 1;
"#;

pub const INSERT_TRIP: &str = r#"
INSERT INTO trips (trip_id, device_id, start_time, start_lat, start_lng)
VALUES ($1, $2, $3, $4, $5);
"#;

pub const UPDATE_TRIP_END: &str = r#"
UPDATE trips
SET end_time = $1,
    end_lat = $2,
    end_lng = $3
WHERE trip_id = $4;
"#;

pub const UPDATE_CURRENT_STATE_NEW_TRIP: &str = r#"
INSERT INTO trip_current_state (device_id, current_trip_id, ignition_on, last_updated_at, last_point_at, last_lat, last_lng, last_correlation_id)
VALUES ($1, $2, true, NOW(), $3, $4, $5, $6)
ON CONFLICT (device_id) DO UPDATE
SET current_trip_id = $2,
    ignition_on = true,
    last_updated_at = NOW(),
    last_point_at = $3,
    last_lat = $4,
    last_lng = $5,
    last_correlation_id = $6;
"#;

pub const UPDATE_CURRENT_STATE_END_TRIP: &str = r#"
UPDATE trip_current_state
SET current_trip_id = NULL,
    ignition_on = false,
    last_updated_at = NOW(),
    last_point_at = $3,
    last_lat = $4,
    last_lng = $5,
    last_speed = $6,
    last_correlation_id = $2
WHERE device_id = $1;
"#;

pub const UPDATE_CURRENT_STATE_POINT: &str = r#"
UPDATE trip_current_state
SET last_point_at = $2,
    last_lat = $3,
    last_lng = $4,
    last_speed = $5,
    last_updated_at = NOW(),
    last_correlation_id = $6
WHERE device_id = $1;
"#;

pub const INSERT_TRIP_POINT: &str = r#"
INSERT INTO trip_points (trip_id, device_id, timestamp, lat, lng, speed, heading, correlation_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
"#;

pub const INSERT_TRIP_ALERT: &str = r#"
INSERT INTO trip_alerts (
    alert_id, trip_id, timestamp, lat, lon, alert_type, raw_code, severity, device_id, correlation_id
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);
"#;

pub const INSERT_DEVICE_IDLE_ACTIVITY: &str = r#"
INSERT INTO device_idle_activity (
    idle_id,
    device_id,
    timestamp,
    lat,
    lon,
    activity_type,
    raw_code,
    severity,
    metadata,
    correlation_id
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10);
"#;
