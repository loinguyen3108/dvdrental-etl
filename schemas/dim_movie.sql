CREATE TABLE IF NOT EXISTS dvd_rental.dim_movie (
    movie_id BIGINT,
    title STRING,
    description STRING,
    release_year INT,
    language VARCHAR(20),
    rental_duration INT,
    rental_rate FLOAT,
    length INT,
    replacement_cost FLOAT,
    rating FLOAT,
    last_update DATE,
    special_features STRING,
    fulltext STRING
)
PARTITIONED BY (last_update DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
