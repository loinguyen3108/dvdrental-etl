CREATE TABLE IF NOT EXISTS dvd_rental.sale (
    sale_id
    amount INT,
    rental_date_id INT,
    rental_year INT,
    rental_month INT,
    rental_day INT,
    return_date_id INT,
    payment_date_id INT,
    movie_id BIGINT,
    customer_id BIGINT,
    store_id BIGINT,
    staff_id BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
