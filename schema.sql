CREATE DATABASE IF NOT EXISTS traffic_db;
USE traffic_db;

DROP TABLE IF EXISTS traffic_violations;

CREATE TABLE traffic_violations (
    seq_id VARCHAR(50) NOT NULL,
    violation_type VARCHAR(50) NOT NULL,

    stop_datetime DATETIME,
    agency VARCHAR(50),
    subagency VARCHAR(100),
    description TEXT,

    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),

    accident BOOLEAN,
    personal_injury BOOLEAN,
    fatal BOOLEAN,

    vehicle_type VARCHAR(50),
    race VARCHAR(50),
    gender VARCHAR(10),

    PRIMARY KEY (seq_id, violation_type),
    INDEX idx_stop_datetime (stop_datetime),
    INDEX idx_location (latitude, longitude),
    INDEX idx_demographics (race, gender)
);
