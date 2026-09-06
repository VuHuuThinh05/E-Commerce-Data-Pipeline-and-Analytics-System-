{{ config(
    materialized='table'
) }}

with customer_base as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,

        row_number() over (
            partition by customer_unique_id
            order by customer_id
        ) as rn

    from {{ ref('stg_customers') }}

),

customer_orders as (

    select
        c.customer_unique_id,

        count(distinct o.order_id)
            as total_orders,

        min(o.order_purchase_timestamp)
            as first_order_date,

        max(o.order_purchase_timestamp)
            as last_order_date

    from {{ ref('stg_customers') }} c

    left join {{ ref('stg_orders') }} o
        on c.customer_id = o.customer_id

    group by
        c.customer_unique_id

)

select

    md5(cb.customer_unique_id)
        as customer_key,

    cb.customer_unique_id,

    cb.customer_id,

    cb.customer_zip_code_prefix,

    cb.customer_city,

    cb.customer_state,

    coalesce(co.total_orders, 0)
        as total_orders,

    co.first_order_date,

    co.last_order_date

from customer_base cb

left join customer_orders co
    on cb.customer_unique_id = co.customer_unique_id

where cb.rn = 1