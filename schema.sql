CREATE DATABASE IF NOT EXISTS traffic_db;
USE traffic_db;

DROP TABLE IF EXISTS traffic_violations;

CREATE TABLE traffic_violations (
    seq_id VARCHAR(50) NOT NULL,
    charge VARCHAR(50) NOT NULL,

    violation_type VARCHAR(50),
    stop_datetime DATETIME,

    agency VARCHAR(50),
    subagency VARCHAR(100),
    location TEXT,
    description TEXT,

    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),

    accident BOOLEAN,
    property_damage BOOLEAN,
    alcohol BOOLEAN,
    work_zone BOOLEAN,

    search_conducted BOOLEAN,
    search_disposition VARCHAR(100),
    search_outcome VARCHAR(100),
    search_reason VARCHAR(100),

    vehicle_type VARCHAR(50),
    make VARCHAR(50),
    model VARCHAR(50),
    color VARCHAR(30),

    race VARCHAR(50),
    gender VARCHAR(10),
    state VARCHAR(10),
    dl_state VARCHAR(10),

    PRIMARY KEY (seq_id, charge),

    INDEX idx_stop_datetime (stop_datetime),
    INDEX idx_location (latitude, longitude),
    INDEX idx_vehicle (vehicle_type, make),
    INDEX idx_demographics (race, gender),
    INDEX idx_search (search_conducted)
);

