{{ config(
    materialized='view'
) }}

select
    trim(seller_id) as seller_id,

    cast(seller_zip_code_prefix as integer)
        as seller_zip_code_prefix,

    trim(lower(seller_city))
        as seller_city,

    upper(trim(seller_state))
        as seller_state

from {{ source('raw', 'sellers') }}

where seller_id is not null