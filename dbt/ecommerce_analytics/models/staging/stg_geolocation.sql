{{ config(
    materialized='view'
) }}

select distinct
    cast(geolocation_zip_code_prefix as integer)
        as geolocation_zip_code_prefix,

    cast(geolocation_lat as numeric(10,6))
        as geolocation_lat,

    cast(geolocation_lng as numeric(10,6))
        as geolocation_lng,

    trim(lower(geolocation_city))
        as geolocation_city,

    upper(trim(geolocation_state))
        as geolocation_state

from {{ source('raw', 'geolocation') }}