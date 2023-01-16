{{ config(
    materialized='view'
)
}}

WITH fhv_data AS 
(
  SELECT *
    -- ,row_number() OVER(PARTITION BY dispatching_base_num, pickup_datetime) AS rn
  FROM {{ source('staging','FHVHV_ALL_Data') }}
  WHERE dispatching_base_num is not null
)
SELECT
    -- identifiers
        -- Unique TripID
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} AS TripID,
    CAST(TRIM(NULLIF(dispatching_base_num,""),'B') AS INTEGER) AS dispatching_base_num,
    CAST(COALESCE(TRIM(NULLIF(originating_base_num,""),'B'),TRIM(dispatching_base_num,'B')) AS INTEGER) AS originating_base_num,
    CAST(PULocationID AS INTEGER) AS PickUp_LocationID,
    CAST(DOLocationID AS INTEGER) AS DropOff_LocationID,
    
    -- TIMESTAMPs
    CAST(request_datetime AS TIMESTAMP) AS  request_datetime,
    CAST(on_scene_datetime AS TIMESTAMP) AS on_scene_datetime,
    CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    
    -- trip info
    --store_and_fwd_flag,
    CAST(trip_miles AS NUMERIC) AS trip_miles,
    CAST(trip_time AS INTEGER) AS trip_time,
    --flags
    COALESCE(NULLIF(shared_request_flag,""), "N") AS shared_request_flag,
    COALESCE(NULLIF(shared_match_flag,""), "N") AS shared_match_flag,
    COALESCE(NULLIF(access_a_ride_flag,""), "N") AS access_a_ride_flag,
    COALESCE(NULLIF(wav_request_flag,""), "N") AS wav_request_flag,
    --can not assume car is not WAV
    COALESCE(NULLIF(wav_match_flag,""), "NA") AS wav_match_flag,
    
    -- payment info
    CAST(base_passenger_fare AS NUMERIC) AS base_passenger_fare,
    CAST(tolls AS NUMERIC) AS tolls,
    CAST(bcf AS NUMERIC) AS black_car_fund,
    CAST(sales_tax AS NUMERIC) AS sales_tax,
    CAST(congestion_surcharge AS NUMERIC) AS congestion_surcharge,
    COALESCE(CAST(airport_fee AS NUMERIC), 0) AS airport_fee,
    CAST(tips AS NUMERIC) AS tips,
    CAST(driver_pay AS NUMERIC) AS driver_pay 

FROM fhv_data
-- WHERE rn = 1 

-- ease in tryouts during dbt creations.
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}
LIMIT 100
{% endif %}
