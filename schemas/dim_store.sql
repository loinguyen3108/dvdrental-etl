CREATE TABLE IF NOT EXISTS dim_store (
    store_id BIGINT,
    manager_first_name VARCHAR(60),
    manager_last_name VARCHAR(60),
    address VARCHAR(60),
    address2 VARCHAR(60),
    district VARCHAR(60),
    city VARCHAR(60),
    country VARCHAR(60)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
