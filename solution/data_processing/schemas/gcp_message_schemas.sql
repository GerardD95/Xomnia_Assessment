-- CREATE TABLE IF NOT EXISTS devices (
--     device_id VARCHAR PRIMARY KEY,
--     address_ip VARCHAR,
--     address_port INTEGER
-- );

-- CREATE TABLE IF NOT EXISTS messages (
--     original_message_id VARCHAR PRIMARY KEY,
--     datetime TIMESTAMP,
--     device_id VARCHAR,
--     FOREIGN KEY (device_id) REFERENCES devices(device_id)
-- );

-- CREATE TABLE IF NOT EXISTS location (
--     original_message_id VARCHAR PRIMARY KEY,
--     lat DOUBLE,
--     long DOUBLE,
--     lat_dir VARCHAR,
--     long_dir VARCHAR,
--     FOREIGN KEY (original_message_id) REFERENCES messages(original_message_id)
-- );

-- CREATE TABLE IF NOT EXISTS navigation (
--     original_message_id VARCHAR PRIMARY KEY,
--     spd_over_grnd DOUBLE,
--     true_course DOUBLE,
--     mag_variaton DOUBLE,
--     mag_var_dir VARCHAR,
--     FOREIGN KEY (original_message_id) REFERENCES messages(original_message_id)
-- );

CREATE TABLE IF NOT EXISTS raw_messages (
    device_id VARCHAR NOT NULL,
    datetime TIMESTAMP NOT NULL,
    address_ip VARCHAR NOT NULL,
    address_port INTEGER NOT NULL,
    original_message_id VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lat_dir VARCHAR NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    lon_dir VARCHAR NOT NULL,
    spd_over_grnd DOUBLE PRECISION NOT NULL,
    true_course DOUBLE PRECISION,
    datestamp INTEGER,
    mag_variation DOUBLE PRECISION,
    mag_var_dir VARCHAR,
    PRIMARY KEY (device_id, original_message_id)
);

