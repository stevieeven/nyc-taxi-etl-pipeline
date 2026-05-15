
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pickup_ts
from "nyc_taxi"."staging"."stg_yellow_taxi_trips"
where pickup_ts is null



  
  
      
    ) dbt_internal_test