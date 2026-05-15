create table if not exists raw.yellow_taxi_trips (
    vendorid integer,
    tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count numeric,
    trip_distance numeric,
    ratecodeid numeric,
    store_and_fwd_flag text,
    pulocationid integer,
    dolocationid integer,
    payment_type integer,
    fare_amount numeric,
    extra numeric,
    mta_tax numeric,
    tip_amount numeric,
    tolls_amount numeric,
    improvement_surcharge numeric,
    total_amount numeric,
    congestion_surcharge numeric,
    airport_fee numeric,
    source_file text,
    load_batch_id text,
    ingested_at timestamp default current_timestamp
);

create table if not exists raw.ingestion_log (
    batch_id text primary key,
    dataset_name text,
    source_file text,
    year_month text,
    row_count integer,
    status text,
    loaded_at timestamp default current_timestamp
);