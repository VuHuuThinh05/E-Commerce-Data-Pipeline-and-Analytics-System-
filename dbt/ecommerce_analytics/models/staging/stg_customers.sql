{{ config(
    materialized='view'
) }}

select
    trim(customer_id) as customer_id,
    trim(customer_unique_id) as customer_unique_id,
    cast(customer_zip_code_prefix as integer) as customer_zip_code_prefix,
    trim(lower(customer_city)) as customer_city,
    upper(trim(customer_state)) as customer_state
from {{ source('raw', 'customers') }}
where customer_id is not null