{{ config(materialized='table') }}

WITH fhv_data AS (
    SELECT *
    FROM {{ ref('staging_For-hire-vehicles') }}
),

dimension_zones AS (
    SELECT * FROM {{ ref('dimension_zones') }}
    --WHERE borough != 'Unknown'
)
SELECT
    fhv_data.TripID,
    fhv_data.dispatching_base_num,
    fhv_data.originating_base_num,
    
    fhv_data.request_datetime,
    fhv_data.pickup_datetime,
    fhv_data.on_scene_datetime,
    pickup_zone.borough AS PickUp_Borough, 
    pickup_zone.zone AS PickUp_Zone,
    fhv_data.dropoff_datetime,
    dropoff_zone.borough AS DropOff_Borough, 
    dropoff_zone.zone AS DropOff_Zone,
    
    fhv_data.trip_miles,
    fhv_data.trip_time,
    fhv_data.shared_request_flag,
    fhv_data.shared_match_flag,
    fhv_data.access_a_ride_flag,
    fhv_data.wav_request_flag,
    fhv_data.wav_match_flag,
    fhv_data.base_passenger_fare,
    fhv_data.tolls,
    fhv_data.black_car_fund,
    fhv_data.sales_tax,
    fhv_data.congestion_surcharge,
    fhv_data.airport_fee,
    fhv_data.tips,
    fhv_data.driver_pay

FROM fhv_data
left join dimension_zones AS pickup_zone
ON fhv_data.PickUp_LocationID = pickup_zone.locationid
left join dimension_zones AS dropoff_zone
ON fhv_data.DropOff_LocationID = dropoff_zone.locationid