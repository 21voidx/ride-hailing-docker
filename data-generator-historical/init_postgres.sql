CREATE TABLE IF NOT EXISTS rider_account (
    rider_id BIGSERIAL PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    full_name VARCHAR(200),
    email VARCHAR(200),
    phone_number VARCHAR(50),
    account_status VARCHAR(30) NOT NULL DEFAULT 'ACTIVE',
    city_code VARCHAR(10) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS driver_profile (
    driver_id BIGSERIAL PRIMARY KEY,
    driver_name VARCHAR(200) NOT NULL,
    phone_number VARCHAR(50),
    city_code VARCHAR(10) NOT NULL,
    driver_status VARCHAR(30) NOT NULL DEFAULT 'AVAILABLE',
    verification_status VARCHAR(30) NOT NULL DEFAULT 'VERIFIED',
    rating_avg NUMERIC(4,2) NOT NULL DEFAULT 5.00,
    rating_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS vehicle (
    vehicle_id BIGSERIAL PRIMARY KEY,
    driver_id BIGINT REFERENCES driver_profile(driver_id),
    license_plate VARCHAR(30) UNIQUE NOT NULL,
    vehicle_type VARCHAR(20) NOT NULL,
    vehicle_make VARCHAR(50),
    vehicle_model VARCHAR(50),
    vehicle_year INTEGER,
    vehicle_status VARCHAR(30) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS driver_vehicle_assignment (
    assignment_id BIGSERIAL PRIMARY KEY,
    driver_id BIGINT NOT NULL REFERENCES driver_profile(driver_id),
    vehicle_id BIGINT NOT NULL REFERENCES vehicle(vehicle_id),
    assigned_from TIMESTAMPTZ NOT NULL,
    assigned_to TIMESTAMPTZ,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS driver_shift (
    shift_id BIGSERIAL PRIMARY KEY,
    driver_id BIGINT NOT NULL REFERENCES driver_profile(driver_id),
    shift_status VARCHAR(30) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS ride (
    ride_id BIGSERIAL PRIMARY KEY,
    rider_id BIGINT NOT NULL REFERENCES rider_account(rider_id),
    driver_id BIGINT REFERENCES driver_profile(driver_id),
    vehicle_id BIGINT REFERENCES vehicle(vehicle_id),
    ride_status VARCHAR(40) NOT NULL,
    service_type VARCHAR(20) NOT NULL,
    city_code VARCHAR(10) NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    accepted_at TIMESTAMPTZ,
    arrived_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    cancelled_by_type VARCHAR(30),
    cancel_reason_code VARCHAR(100),
    estimated_distance_km NUMERIC(10,2),
    estimated_duration_min NUMERIC(10,2),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ride_status_history (
    ride_status_history_id BIGSERIAL PRIMARY KEY,
    ride_id BIGINT NOT NULL REFERENCES ride(ride_id),
    old_status VARCHAR(40),
    new_status VARCHAR(40) NOT NULL,
    changed_by_type VARCHAR(30) NOT NULL,
    changed_by_id BIGINT,
    reason_code VARCHAR(100),
    reason_note TEXT,
    changed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS ride_location (
    ride_location_id BIGSERIAL PRIMARY KEY,
    ride_id BIGINT NOT NULL REFERENCES ride(ride_id),
    location_type VARCHAR(40) NOT NULL,
    latitude NUMERIC(10,6) NOT NULL,
    longitude NUMERIC(10,6) NOT NULL,
    address_text TEXT,
    captured_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS ride_tracking_point (
    tracking_point_id BIGSERIAL PRIMARY KEY,
    ride_id BIGINT NOT NULL REFERENCES ride(ride_id),
    driver_id BIGINT NOT NULL REFERENCES driver_profile(driver_id),
    latitude NUMERIC(10,6) NOT NULL,
    longitude NUMERIC(10,6) NOT NULL,
    speed_kmh NUMERIC(10,2),
    recorded_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS ride_fare (
    fare_id BIGSERIAL PRIMARY KEY,
    ride_id BIGINT NOT NULL REFERENCES ride(ride_id),
    fare_type VARCHAR(30) NOT NULL DEFAULT 'FINAL',
    fare_version INTEGER NOT NULL DEFAULT 1,
    currency_code VARCHAR(10) NOT NULL DEFAULT 'IDR',
    distance_km NUMERIC(10,2),
    duration_min NUMERIC(10,2),
    base_fare NUMERIC(18,2),
    distance_fare NUMERIC(18,2),
    time_fare NUMERIC(18,2),
    surge_multiplier NUMERIC(6,2),
    surge_amount NUMERIC(18,2),
    discount_amount NUMERIC(18,2),
    tax_amount NUMERIC(18,2),
    platform_fee NUMERIC(18,2),
    driver_earning NUMERIC(18,2),
    total_fare NUMERIC(18,2),
    fare_rule_code VARCHAR(100),
    is_corrected BOOLEAN NOT NULL DEFAULT FALSE,
    calculated_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ride_updated_at ON ride(updated_at);
CREATE INDEX IF NOT EXISTS idx_ride_status_history_changed_at ON ride_status_history(changed_at);
CREATE INDEX IF NOT EXISTS idx_ride_fare_updated_at ON ride_fare(updated_at);
CREATE INDEX IF NOT EXISTS idx_driver_profile_updated_at ON driver_profile(updated_at);
