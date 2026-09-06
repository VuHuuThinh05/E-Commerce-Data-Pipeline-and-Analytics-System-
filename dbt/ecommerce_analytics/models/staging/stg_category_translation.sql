{{ config(
    materialized='view'
) }}

select
    trim(product_category_name)
        as product_category_name,

    trim(product_category_name_english)
        as product_category_name_english

from {{ source('raw', 'category_translation') }}

where product_category_name is not null