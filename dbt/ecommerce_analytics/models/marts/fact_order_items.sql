{{ config(
    materialized='table'
) }}

select

    md5(
        concat(
            oi.order_id,
            '||',
            oi.order_item_id::text
        )
    ) as order_item_key,

    oi.order_id,

    oi.order_item_id,

    md5(oi.product_id)
        as product_key,

    md5(oi.seller_id)
        as seller_key,

    oi.product_id,

    oi.seller_id,

    oi.shipping_limit_date,

    oi.price,

    oi.freight_value,

    oi.price + oi.freight_value
        as item_total_value

from {{ ref('stg_order_items') }} oi