{{ config(
    materialized='table'
) }}

select

    md5(
        concat(
            p.order_id,
            '||',
            p.payment_sequential::text
        )
    ) as payment_key,

    p.order_id,

    p.payment_sequential,

    p.payment_type,

    p.payment_installments,

    p.payment_value

from {{ ref('stg_payments') }} p