{{ config(materialized='table') }}

SELECT 
    locationid, 
    borough, 
    zone, 
    --can be used in farther analytics regarding impact on taxis from FHV (unnecessary column in for-hire-vehicles)
    replace(service_zone,'Boro','Green') AS service_zone
FROM {{ ref('taxi_zone_lookup') }}