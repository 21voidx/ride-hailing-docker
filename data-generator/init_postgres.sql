CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Create a role for Debezium with appropriate permissions for logical decoding and replication.
------------------------------------------------------------------
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';

GRANT CONNECT ON DATABASE ride_ops_pg TO debezium;

GRANT USAGE ON SCHEMA public TO debezium;

GRANT SELECT ON TABLE
  public.ride,
  public.driver_profile,
  public.payment_transaction
TO debezium;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO debezium;
------------------------------------------------------------------

-- Drop in dependency order so repeated local resets are easy.
DROP TABLE IF EXISTS promo_usage, review, payment_refund, payment_transaction, user_payment_method,
    payment_method_type, ride_fare_component, ride_fare, ride_tracking_point, ride_location,
    ride_status_history, ride, driver_vehicle_assignment, vehicle, driver_document,
    driver_profile, user_role, role, promotion, user_account CASCADE;

CREATE TABLE user_account (
    user_id BIGSERIAL PRIMARY KEY,
    username VARCHAR(80) NOT NULL UNIQUE,
    email VARCHAR(160) UNIQUE,
    phone_number VARCHAR(30) UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    account_status VARCHAR(30) NOT NULL CHECK (account_status IN ('ACTIVE','SUSPENDED','DELETED')),
    email_verified_at TIMESTAMPTZ,
    phone_verified_at TIMESTAMPTZ,
    last_login_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ
);

CREATE TABLE role (
    role_id SMALLSERIAL PRIMARY KEY,
    role_code VARCHAR(40) NOT NULL UNIQUE,
    role_name VARCHAR(80) NOT NULL,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE user_role (
    user_id BIGINT NOT NULL REFERENCES user_account(user_id),
    role_id SMALLINT NOT NULL REFERENCES role(role_id),
    assigned_at TIMESTAMPTZ NOT NULL,
    assigned_by BIGINT REFERENCES user_account(user_id),
    is_active BOOLEAN NOT NULL DEFAULT true,
    PRIMARY KEY (user_id, role_id)
);

CREATE TABLE driver_profile (
    driver_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL UNIQUE REFERENCES user_account(user_id),
    license_number VARCHAR(80) NOT NULL UNIQUE,
    license_expiry DATE NOT NULL,
    driver_status VARCHAR(30) NOT NULL CHECK (driver_status IN ('AVAILABLE','ON_RIDE','OFFLINE','SUSPENDED')),
    verification_status VARCHAR(30) NOT NULL CHECK (verification_status IN ('PENDING','VERIFIED','REJECTED')),
    verified_at TIMESTAMPTZ,
    suspended_at TIMESTAMPTZ,
    rating_avg NUMERIC(3,2) NOT NULL DEFAULT 5.00 CHECK (rating_avg BETWEEN 1.00 AND 5.00),
    rating_count INT NOT NULL DEFAULT 0 CHECK (rating_count >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE driver_document (
    document_id BIGSERIAL PRIMARY KEY,
    driver_id BIGINT NOT NULL REFERENCES driver_profile(driver_id),
    document_type VARCHAR(40) NOT NULL CHECK (document_type IN ('KTP','SIM','STNK','SKCK')),
    document_number VARCHAR(100) NOT NULL,
    document_file_url VARCHAR(400),
    verification_status VARCHAR(30) NOT NULL CHECK (verification_status IN ('PENDING','VERIFIED','REJECTED')),
    submitted_at TIMESTAMPTZ NOT NULL,
    verified_at TIMESTAMPTZ,
    verified_by BIGINT REFERENCES user_account(user_id),
    expires_at DATE,
    rejection_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE vehicle (
    vehicle_id BIGSERIAL PRIMARY KEY,
    license_plate VARCHAR(20) NOT NULL UNIQUE,
    vehicle_make VARCHAR(80) NOT NULL,
    vehicle_model VARCHAR(80) NOT NULL,
    vehicle_year INT NOT NULL CHECK (vehicle_year BETWEEN 2000 AND 2035),
    vehicle_capacity INT NOT NULL CHECK (vehicle_capacity BETWEEN 1 AND 12),
    vehicle_color VARCHAR(40),
    vehicle_type VARCHAR(30) NOT NULL CHECK (vehicle_type IN ('BIKE','CAR','XL')),
    vehicle_status VARCHAR(30) NOT NULL CHECK (vehicle_status IN ('ACTIVE','INACTIVE','SUSPENDED')),
    verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ
);

CREATE TABLE driver_vehicle_assignment (
    assignment_id BIGSERIAL PRIMARY KEY,
    driver_id BIGINT NOT NULL REFERENCES driver_profile(driver_id),
    vehicle_id BIGINT NOT NULL REFERENCES vehicle(vehicle_id),
    assigned_from TIMESTAMPTZ NOT NULL,
    assigned_to TIMESTAMPTZ,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE ride (
    ride_id BIGSERIAL PRIMARY KEY,
    rider_id BIGINT NOT NULL REFERENCES user_account(user_id),
    driver_id BIGINT REFERENCES driver_profile(driver_id),
    vehicle_id BIGINT REFERENCES vehicle(vehicle_id),
    ride_status VARCHAR(30) NOT NULL CHECK (ride_status IN ('REQUESTED','ACCEPTED','ARRIVED','IN_PROGRESS','COMPLETED','CANCELLED','PAYMENT_FAILED')),
    service_type VARCHAR(30) NOT NULL CHECK (service_type IN ('BIKE','CAR','XL')),
    city_code VARCHAR(20) NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    accepted_at TIMESTAMPTZ,
    arrived_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    cancelled_by_user_id BIGINT REFERENCES user_account(user_id),
    cancel_reason_code VARCHAR(50),
    cancel_reason_note TEXT,
    estimated_distance_km NUMERIC(8,2) NOT NULL CHECK (estimated_distance_km > 0),
    estimated_duration_min NUMERIC(8,2) NOT NULL CHECK (estimated_duration_min > 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE ride_status_history (
    ride_status_history_id BIGSERIAL PRIMARY KEY,
    ride_id BIGINT NOT NULL REFERENCES ride(ride_id),
    old_status VARCHAR(30),
    new_status VARCHAR(30) NOT NULL,
    changed_by_user_id BIGINT REFERENCES user_account(user_id),
    reason_code VARCHAR(50),
    reason_note TEXT,
    changed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE ride_location (
    ride_location_id BIGSERIAL PRIMARY KEY,
    ride_id BIGINT NOT NULL REFERENCES ride(ride_id),
    location_type VARCHAR(40) NOT NULL CHECK (location_type IN ('PICKUP_REQUESTED','DROPOFF_REQUESTED','PICKUP_ACTUAL','DROPOFF_ACTUAL')),
    latitude NUMERIC(9,6) NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude NUMERIC(9,6) NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    address_text TEXT,
    place_id VARCHAR(120),
    captured_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE ride_tracking_point (
    tracking_point_id BIGSERIAL PRIMARY KEY,
    ride_id BIGINT NOT NULL REFERENCES ride(ride_id),
    driver_id BIGINT NOT NULL REFERENCES driver_profile(driver_id),
    latitude NUMERIC(9,6) NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude NUMERIC(9,6) NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    speed_kmh NUMERIC(6,2) CHECK (speed_kmh >= 0),
    heading_degree NUMERIC(6,2) CHECK (heading_degree >= 0 AND heading_degree < 360),
    accuracy_meter NUMERIC(6,2) CHECK (accuracy_meter >= 0),
    recorded_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE ride_fare (
    fare_id BIGSERIAL PRIMARY KEY,
    ride_id BIGINT NOT NULL REFERENCES ride(ride_id),
    fare_type VARCHAR(30) NOT NULL CHECK (fare_type IN ('ESTIMATED','FINAL','ADJUSTED')),
    fare_version INT NOT NULL DEFAULT 1 CHECK (fare_version > 0),
    currency_code CHAR(3) NOT NULL DEFAULT 'IDR',
    distance_km NUMERIC(8,2) NOT NULL CHECK (distance_km > 0),
    duration_min NUMERIC(8,2) NOT NULL CHECK (duration_min > 0),
    base_fare NUMERIC(12,2) NOT NULL CHECK (base_fare >= 0),
    distance_fare NUMERIC(12,2) NOT NULL CHECK (distance_fare >= 0),
    time_fare NUMERIC(12,2) NOT NULL CHECK (time_fare >= 0),
    surge_multiplier NUMERIC(4,2) NOT NULL CHECK (surge_multiplier >= 1),
    surge_amount NUMERIC(12,2) NOT NULL CHECK (surge_amount >= 0),
    discount_amount NUMERIC(12,2) NOT NULL CHECK (discount_amount >= 0),
    tax_amount NUMERIC(12,2) NOT NULL CHECK (tax_amount >= 0),
    platform_fee NUMERIC(12,2) NOT NULL CHECK (platform_fee >= 0),
    driver_earning NUMERIC(12,2) NOT NULL CHECK (driver_earning >= 0),
    total_fare NUMERIC(12,2) NOT NULL CHECK (total_fare >= 0),
    fare_rule_code VARCHAR(80) NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE ride_fare_component (
    fare_component_id BIGSERIAL PRIMARY KEY,
    fare_id BIGINT NOT NULL REFERENCES ride_fare(fare_id),
    component_code VARCHAR(50) NOT NULL,
    component_name VARCHAR(100) NOT NULL,
    component_amount NUMERIC(12,2) NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE payment_method_type (
    payment_method_type_id SMALLSERIAL PRIMARY KEY,
    method_code VARCHAR(40) NOT NULL UNIQUE,
    method_name VARCHAR(100) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE user_payment_method (
    user_payment_method_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES user_account(user_id),
    payment_method_type_id SMALLINT NOT NULL REFERENCES payment_method_type(payment_method_type_id),
    provider_name VARCHAR(80),
    provider_customer_id VARCHAR(120),
    provider_payment_token VARCHAR(180),
    masked_account VARCHAR(40),
    expiry_month SMALLINT CHECK (expiry_month BETWEEN 1 AND 12),
    expiry_year SMALLINT CHECK (expiry_year BETWEEN 2024 AND 2045),
    is_default BOOLEAN NOT NULL DEFAULT false,
    payment_method_status VARCHAR(30) NOT NULL CHECK (payment_method_status IN ('ACTIVE','EXPIRED','DISABLED')),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ
);

CREATE TABLE payment_transaction (
    transaction_id BIGSERIAL PRIMARY KEY,
    ride_id BIGINT NOT NULL REFERENCES ride(ride_id),
    user_payment_method_id BIGINT NOT NULL REFERENCES user_payment_method(user_payment_method_id),
    provider_name VARCHAR(80) NOT NULL,
    provider_transaction_id VARCHAR(120) UNIQUE,
    idempotency_key VARCHAR(160) NOT NULL UNIQUE,
    amount NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
    method_fee NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (method_fee >= 0),
    currency_code CHAR(3) NOT NULL DEFAULT 'IDR',
    payment_status VARCHAR(30) NOT NULL CHECK (payment_status IN ('AUTHORIZED','CAPTURED','PAID','FAILED','REFUNDED')),
    failure_code VARCHAR(80),
    failure_message TEXT,
    authorized_at TIMESTAMPTZ,
    captured_at TIMESTAMPTZ,
    paid_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE payment_refund (
    refund_id BIGSERIAL PRIMARY KEY,
    transaction_id BIGINT NOT NULL REFERENCES payment_transaction(transaction_id),
    provider_refund_id VARCHAR(120) UNIQUE,
    refund_amount NUMERIC(12,2) NOT NULL CHECK (refund_amount >= 0),
    currency_code CHAR(3) NOT NULL DEFAULT 'IDR',
    refund_status VARCHAR(30) NOT NULL CHECK (refund_status IN ('REQUESTED','COMPLETED','FAILED')),
    refund_reason_code VARCHAR(80),
    refund_reason_note TEXT,
    requested_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE review (
    review_id BIGSERIAL PRIMARY KEY,
    ride_id BIGINT NOT NULL REFERENCES ride(ride_id),
    reviewer_id BIGINT NOT NULL REFERENCES user_account(user_id),
    reviewee_id BIGINT NOT NULL REFERENCES user_account(user_id),
    review_type VARCHAR(30) NOT NULL CHECK (review_type IN ('RIDER_TO_DRIVER','DRIVER_TO_RIDER')),
    rating_score SMALLINT NOT NULL CHECK (rating_score BETWEEN 1 AND 5),
    comments TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ,
    UNIQUE (ride_id, reviewer_id, review_type)
);

CREATE TABLE promotion (
    promotion_id BIGSERIAL PRIMARY KEY,
    promo_code VARCHAR(60) NOT NULL UNIQUE,
    promo_description TEXT,
    discount_type VARCHAR(30) NOT NULL CHECK (discount_type IN ('PERCENT','FIXED')),
    discount_pct NUMERIC(5,2) CHECK (discount_pct BETWEEN 0 AND 100),
    discount_amount NUMERIC(12,2) CHECK (discount_amount >= 0),
    max_discount_amount NUMERIC(12,2) CHECK (max_discount_amount >= 0),
    min_fare_amount NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (min_fare_amount >= 0),
    usage_limit_total INT CHECK (usage_limit_total > 0),
    usage_limit_per_user INT CHECK (usage_limit_per_user > 0),
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ NOT NULL,
    promotion_status VARCHAR(30) NOT NULL CHECK (promotion_status IN ('ACTIVE','INACTIVE','EXPIRED')),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CHECK (valid_to > valid_from)
);

CREATE TABLE promo_usage (
    promo_usage_id BIGSERIAL PRIMARY KEY,
    promotion_id BIGINT NOT NULL REFERENCES promotion(promotion_id),
    ride_id BIGINT NOT NULL REFERENCES ride(ride_id),
    rider_id BIGINT NOT NULL REFERENCES user_account(user_id),
    discount_amount_applied NUMERIC(12,2) NOT NULL CHECK (discount_amount_applied >= 0),
    used_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (ride_id)
);

CREATE INDEX idx_ride_rider_time ON ride (rider_id, requested_at DESC);
CREATE INDEX idx_ride_driver_time ON ride (driver_id, requested_at DESC);
CREATE INDEX idx_ride_status_time ON ride (ride_status, requested_at DESC);
CREATE INDEX idx_ride_created_at ON ride (created_at DESC);
CREATE INDEX idx_ride_status_history_ride_time ON ride_status_history (ride_id, changed_at);
CREATE INDEX idx_location_ride_type ON ride_location (ride_id, location_type);
CREATE INDEX idx_tracking_ride_time ON ride_tracking_point (ride_id, recorded_at);
CREATE INDEX idx_payment_ride ON payment_transaction (ride_id);
CREATE INDEX idx_payment_status_time ON payment_transaction (payment_status, created_at DESC);
CREATE INDEX idx_review_ride ON review (ride_id);
CREATE INDEX idx_promo_usage_rider ON promo_usage (rider_id, used_at DESC);

INSERT INTO role (role_code, role_name, description, is_active, created_at, updated_at)
VALUES
('RIDER','Rider','Customer who requests rides',true, now(), now()),
('DRIVER','Driver','Driver partner',true, now(), now()),
('ADMIN','Admin','Back-office user',true, now(), now());

INSERT INTO payment_method_type (method_code, method_name, is_active, created_at, updated_at)
VALUES
('CASH','Cash',true, now(), now()),
('CARD','Card',true, now(), now()),
('EWALLET','E-Wallet',true, now(), now()),
('BANK_TRANSFER','Bank Transfer',true, now(), now());


-- Create publication for Debezium logical decoding
CREATE PUBLICATION debezium_pub_ride_core
FOR TABLE
  public.ride,
  public.driver_profile,
  public.payment_transaction;