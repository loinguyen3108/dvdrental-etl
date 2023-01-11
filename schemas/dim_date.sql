CREATE TABLE IF NOT EXISTS dvd_rental.dim_date (
    date_id INT,
    year INT,
    month INT,
    day INT,
    quarter INT,
    day_of_week INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
