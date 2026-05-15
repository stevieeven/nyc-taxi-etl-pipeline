with source as (
    select * from raw.yellow_taxi_trips
),

cleaned as (
    select
        vendorid,
        tpep_pickup_datetime as pickup_ts,
        tpep_dropoff_datetime as dropoff_ts,
        passenger_count,
        trip_distance,
        ratecodeid,
        store_and_fwd_flag,
        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        source_file,
        load_batch_id,
        ingested_at,
        extract(epoch from (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0 as trip_duration_minutes
    from source
    where tpep_pickup_datetime is not null
      and tpep_dropoff_datetime is not null
      and trip_distance >= 0
      and tpep_dropoff_datetime >= tpep_pickup_datetime
)

select * from cleaned