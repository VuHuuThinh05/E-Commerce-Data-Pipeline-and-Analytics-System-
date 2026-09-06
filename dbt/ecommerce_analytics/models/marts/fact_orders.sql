{{ config(
    materialized='table'
) }}

with orders as (

    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date

    from {{ ref('stg_orders') }}

),

customer_mapping as (

    select
        customer_id,
        customer_unique_id

    from {{ ref('stg_customers') }}

),

item_summary as (

    select

        order_id,

        count(*) as total_items,

        count(distinct product_id)
            as num_products,

        sum(price)
            as total_price,

        sum(freight_value)
            as total_freight,

        sum(price + freight_value)
            as total_order_value

    from {{ ref('stg_order_items') }}

    group by order_id

),

payment_summary as (

    select

        order_id,

        sum(payment_value)
            as total_payment,

        count(*)
            as payment_count

    from {{ ref('stg_payments') }}

    group by order_id

)

select

    md5(o.order_id)
        as order_key,

    o.order_id,

    c.customer_unique_id,

    md5(c.customer_unique_id)
        as customer_key,

    o.customer_id,

    o.order_status,

    cast(
        o.order_purchase_timestamp as date
    ) as order_date,

    o.order_purchase_timestamp,

    o.order_approved_at,

    o.order_delivered_carrier_date,

    o.order_delivered_customer_date,

    o.order_estimated_delivery_date,

    extract(
        year from o.order_purchase_timestamp
    )::integer as order_year,

    extract(
        month from o.order_purchase_timestamp
    )::integer as order_month,

    extract(
        quarter from o.order_purchase_timestamp
    )::integer as order_quarter,

    coalesce(
        i.total_items,
        0
    ) as total_items,

    coalesce(
        i.num_products,
        0
    ) as num_products,

    coalesce(
        i.total_price,
        0
    ) as total_price,

    coalesce(
        i.total_freight,
        0
    ) as total_freight,

    coalesce(
        i.total_order_value,
        0
    ) as total_order_value,

    coalesce(
        p.total_payment,
        0
    ) as total_payment,

    coalesce(
        p.payment_count,
        0
    ) as payment_count,

    case
        when o.order_delivered_customer_date is not null
         and o.order_purchase_timestamp is not null

        then extract(
            epoch from (
                o.order_delivered_customer_date
                - o.order_purchase_timestamp
            )
        ) / 86400.0

        else null

    end as delivery_days,

    case
        when o.order_delivered_customer_date is not null
         and o.order_estimated_delivery_date is not null

        then extract(
            epoch from (
                o.order_delivered_customer_date
                - o.order_estimated_delivery_date
            )
        ) / 86400.0

        else null

    end as delivery_delay_days

from orders o

left join customer_mapping c
    on o.customer_id = c.customer_id

left join item_summary i
    on o.order_id = i.order_id

left join payment_summary p
    on o.order_id = p.order_id