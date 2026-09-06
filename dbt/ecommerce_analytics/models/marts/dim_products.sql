{{ config(
    materialized='table'
) }}

select

    md5(p.product_id)
        as product_key,

    p.product_id,

    p.product_category_name,

    coalesce(
        ct.product_category_name_english,
        p.product_category_name
    ) as product_category_name_english,

    p.product_name_length,

    p.product_description_length,

    p.product_photos_qty,

    p.product_weight_g,

    p.product_length_cm,

    p.product_height_cm,

    p.product_width_cm

from {{ ref('stg_products') }} p

left join {{ ref('stg_category_translation') }} ct
    on p.product_category_name =
       ct.product_category_name