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
    device_id VARCHAR,
    datetime TIMESTAMP,
    address_ip VARCHAR,
    address_port INTEGER,
    original_message_id VARCHAR,
    status VARCHAR,
    lat DOUBLE,
    lat_dir VARCHAR,
    lon DOUBLE,
    lon_dir VARCHAR,
    spd_over_grnd DOUBLE,
    true_course DOUBLE,
    datestamp INTEGER,
    mag_variation DOUBLE,
    mag_var_dir VARCHAR,
    PRIMARY KEY (device_id, original_message_id)
)