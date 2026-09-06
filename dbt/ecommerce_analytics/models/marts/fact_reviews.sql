{{ config(
    materialized='table'
) }}

select

    md5(
        concat_ws(
            '||',
            r.review_id,
            r.order_id,
            coalesce(
                r.review_creation_date::text,
                ''
            )
        )
    ) as review_key,

    r.review_id,

    r.order_id,

    r.review_score,

    r.review_comment_title,

    r.review_comment_message,

    r.review_creation_date,

    r.review_answer_timestamp,

    case
        when r.review_answer_timestamp is not null
         and r.review_creation_date is not null
        then
            extract(
                epoch from (
                    r.review_answer_timestamp
                    - r.review_creation_date
                )
            ) / 86400.0
        else null
    end as review_response_days

from {{ ref('stg_reviews') }} r