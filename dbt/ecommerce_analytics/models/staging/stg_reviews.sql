{{ config(
    materialized='view'
) }}

select
    trim(review_id) as review_id,
    trim(order_id) as order_id,

    cast(review_score as integer)
        as review_score,

    nullif(trim(review_comment_title), '')
        as review_comment_title,

    nullif(trim(review_comment_message), '')
        as review_comment_message,

    cast(review_creation_date as timestamp)
        as review_creation_date,

    cast(review_answer_timestamp as timestamp)
        as review_answer_timestamp

from {{ source('raw', 'reviews') }}

where review_id is not null
  and order_id is not null