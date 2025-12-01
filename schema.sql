-- public.trip definition
CREATE TABLE IF NOT EXISTS trip (
    trip_id uuid NOT NULL,
    device_id uuid NOT NULL,
    start_time timestamptz NOT NULL,
    end_time timestamptz NULL,
    start_lat float8 NULL,
    start_lng float8 NULL,
    end_lat float8 NULL,
    end_lng float8 NULL,
    distance_m float8 DEFAULT 0 NULL,
    created_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT trip_pkey PRIMARY KEY (trip_id)
);
CREATE INDEX IF NOT EXISTS idx_trip_device ON public.trip USING btree (device_id);
CREATE INDEX IF NOT EXISTS idx_trip_start ON public.trip USING btree (start_time);

-- public.trip_alerts definition
CREATE TYPE alert_type_enum AS ENUM ('power_cut', 'jamming', 'ignition_on', 'ignition_off', 'low_backup_battery');

CREATE TABLE IF NOT EXISTS trip_alerts (
    alert_id uuid NOT NULL,
    trip_id uuid NOT NULL,
    "timestamp" timestamptz NOT NULL,
    lat float8 NULL,
    lon float8 NULL,
    alert_type public.alert_type_enum NOT NULL,
    raw_code int4 NULL,
    severity int2 DEFAULT 1 NULL,
    metadata jsonb NULL,
    created_at timestamptz DEFAULT now() NULL,
    device_id varchar NOT NULL,
    correlation_id uuid NULL
) PARTITION BY RANGE ("timestamp");
CREATE INDEX IF NOT EXISTS idx_trip_alert_device ON ONLY public.trip_alerts USING btree (device_id);
CREATE INDEX IF NOT EXISTS idx_trip_alert_trip ON ONLY public.trip_alerts USING btree (trip_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_trip_alerts_corr_unique ON ONLY public.trip_alerts USING btree (device_id, correlation_id, "timestamp");
CREATE INDEX IF NOT EXISTS idx_trip_alerts_device_time ON ONLY public.trip_alerts USING btree (device_id, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_trip_alerts_type ON ONLY public.trip_alerts USING btree (alert_type);

-- public.trip_current_state definition
CREATE TABLE IF NOT EXISTS trip_current_state (
    device_id varchar NOT NULL,
    current_trip_id uuid NULL,
    ignition_on bool DEFAULT false NOT NULL,
    last_point_at timestamptz NULL,
    last_lat float8 NULL,
    last_lng float8 NULL,
    last_speed float8 NULL,
    last_correlation_id uuid NULL,
    last_updated_at timestamptz DEFAULT now() NOT NULL,
    CONSTRAINT trip_current_state_pkey PRIMARY KEY (device_id)
);

-- public.trip_points definition
CREATE TABLE IF NOT EXISTS trip_points (
    point_id bigserial NOT NULL,
    trip_id uuid NOT NULL,
    device_id varchar NOT NULL,
    "timestamp" timestamptz NOT NULL,
    lat float8 NOT NULL,
    lng float8 NOT NULL,
    speed float8 NULL,
    heading float8 NULL,
    ignition_on bool NULL,
    correlation_id uuid NOT NULL,
    CONSTRAINT trip_points_pkey PRIMARY KEY (device_id, "timestamp", correlation_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_trip_points_corr_unique ON public.trip_points USING btree (device_id, correlation_id, "timestamp");
CREATE INDEX IF NOT EXISTS idx_trip_points_device_time ON public.trip_points USING btree (device_id, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_trip_points_time ON public.trip_points USING btree ("timestamp" DESC);
CREATE INDEX IF NOT EXISTS trip_points_timestamp_idx ON public.trip_points USING btree ("timestamp" DESC);
