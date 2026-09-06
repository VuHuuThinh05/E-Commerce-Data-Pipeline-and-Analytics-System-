{{ config(
    materialized='view'
) }}

select
    trim(order_id) as order_id,
    cast(order_item_id as integer) as order_item_id,
    trim(product_id) as product_id,
    trim(seller_id) as seller_id,

    cast(shipping_limit_date as timestamp)
        as shipping_limit_date,

    cast(price as numeric(12,2))
        as price,

    cast(freight_value as numeric(12,2))
        as freight_value

from {{ source('raw', 'order_items') }}

where order_id is not null