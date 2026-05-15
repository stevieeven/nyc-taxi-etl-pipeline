
  
    

  create  table "nyc_taxi"."analytics"."mart_daily_revenue__dbt_tmp"
  
  
    as
  
  (
    select
    cast(pickup_ts as date) as trip_date,
    count(*) as trip_count,
    sum(total_amount) as total_revenue,
    avg(fare_amount) as avg_fare_amount,
    avg(trip_distance) as avg_trip_distance,
    sum(tip_amount) as total_tip_amount
from "nyc_taxi"."staging"."stg_yellow_taxi_trips"
group by 1
order by 1
  );
  