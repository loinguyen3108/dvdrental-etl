CREATE TABLE IF NOT EXISTS dvd_rental.staff (
    staff_id BIGINT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email STRING,
    address VARCHAR(60),
    address2 VARCHAR(60),
    district VARCHAR(60),
    city VARCHAR(60),
    country VARCHAR(60),
    active BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
