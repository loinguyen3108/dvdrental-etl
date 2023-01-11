CREATE TABLE IF NOT EXISTS dvd_rental.sale (
    amount INT,
    rental_date_id INT,
    return_date_id INT,
    payment_date_id INT,
    movie_id BIGINT,
    customer_id BIGINT,
    store_id BIGINT,
    staff_id BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
