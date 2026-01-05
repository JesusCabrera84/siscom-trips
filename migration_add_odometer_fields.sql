-- Migration to add odometer fields to trip tables
-- Run this after the ALTER TABLE statements mentioned by the user

-- Add last_odometer_meters to trip_current_state
ALTER TABLE trip_current_state
ADD COLUMN last_odometer_meters int4;

-- Note: The user already ran these ALTER TABLE statements:
-- ALTER TABLE trip_points ADD COLUMN odometer_meters int4;
-- ALTER TABLE trips ADD COLUMN start_odometer_meters int4, ADD COLUMN end_odometer_meters int4;
