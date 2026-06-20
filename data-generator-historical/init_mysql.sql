CREATE TABLE IF NOT EXISTS payment_method (
    payment_method_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    rider_id BIGINT NOT NULL,
    method_code VARCHAR(50) NOT NULL,
    provider_name VARCHAR(100) NOT NULL,
    masked_account VARCHAR(50),
    payment_method_status VARCHAR(30) NOT NULL DEFAULT 'ACTIVE',
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    deleted_at DATETIME(6)
);

CREATE TABLE IF NOT EXISTS payment_transaction (
    payment_transaction_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ride_id BIGINT NOT NULL,
    rider_id BIGINT NOT NULL,
    payment_method_id BIGINT,
    provider_name VARCHAR(100) NOT NULL,
    provider_transaction_id VARCHAR(200),
    idempotency_key VARCHAR(200) NOT NULL,
    amount DECIMAL(18,2) NOT NULL,
    method_fee DECIMAL(18,2) NOT NULL DEFAULT 0,
    currency_code VARCHAR(10) NOT NULL DEFAULT 'IDR',
    payment_status VARCHAR(40) NOT NULL,
    failure_code VARCHAR(100),
    failure_message TEXT,
    authorized_at DATETIME(6),
    captured_at DATETIME(6),
    paid_at DATETIME(6),
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    deleted_at DATETIME(6),
    UNIQUE KEY uk_idempotency_key (idempotency_key),
    INDEX idx_payment_updated_at (updated_at),
    INDEX idx_payment_ride_id (ride_id)
);

CREATE TABLE IF NOT EXISTS payment_refund (
    refund_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    payment_transaction_id BIGINT NOT NULL,
    ride_id BIGINT NOT NULL,
    refund_amount DECIMAL(18,2) NOT NULL,
    refund_reason_code VARCHAR(100),
    refund_status VARCHAR(40) NOT NULL,
    requested_at DATETIME(6) NOT NULL,
    processed_at DATETIME(6),
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    INDEX idx_refund_updated_at (updated_at)
);

CREATE TABLE IF NOT EXISTS promotion (
    promotion_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    promo_code VARCHAR(100) NOT NULL UNIQUE,
    promo_description TEXT,
    discount_type VARCHAR(30) NOT NULL,
    discount_pct DECIMAL(6,2),
    discount_amount DECIMAL(18,2),
    max_discount_amount DECIMAL(18,2),
    min_fare_amount DECIMAL(18,2),
    valid_from DATETIME(6) NOT NULL,
    valid_to DATETIME(6) NOT NULL,
    promotion_status VARCHAR(30) NOT NULL DEFAULT 'ACTIVE',
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    deleted_at DATETIME(6)
);

CREATE TABLE IF NOT EXISTS promo_usage (
    promo_usage_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    promotion_id BIGINT NOT NULL,
    ride_id BIGINT NOT NULL,
    rider_id BIGINT NOT NULL,
    discount_amount_applied DECIMAL(18,2) NOT NULL,
    used_at DATETIME(6) NOT NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    UNIQUE KEY uk_promo_ride (ride_id),
    INDEX idx_promo_usage_updated_at (updated_at)
);

CREATE TABLE IF NOT EXISTS review (
    review_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ride_id BIGINT NOT NULL,
    reviewer_type VARCHAR(30) NOT NULL,
    reviewer_id BIGINT NOT NULL,
    reviewee_type VARCHAR(30) NOT NULL,
    reviewee_id BIGINT NOT NULL,
    rating_score INT NOT NULL,
    comments TEXT,
    review_status VARCHAR(30) NOT NULL DEFAULT 'PUBLISHED',
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    deleted_at DATETIME(6),
    INDEX idx_review_updated_at (updated_at)
);

CREATE TABLE IF NOT EXISTS support_ticket (
    ticket_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ride_id BIGINT,
    rider_id BIGINT,
    driver_id BIGINT,
    ticket_category VARCHAR(100) NOT NULL,
    ticket_status VARCHAR(40) NOT NULL,
    priority VARCHAR(30) NOT NULL,
    opened_at DATETIME(6) NOT NULL,
    resolved_at DATETIME(6),
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    deleted_at DATETIME(6),
    INDEX idx_support_updated_at (updated_at)
);
