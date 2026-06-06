with orders as (

    select * from {{ ref('stg_orders') }}

),

payments_agg as (

    select
        order_id,
        sum(payment_value)                                as total_revenue,
        count(distinct payment_seq)                       as payment_count,
        max(payment_installments)                         as max_installments,
        max(case when payment_seq = 1
                 then payment_type end)                   as primary_payment_type,
        bool_or(is_split_payment)                         as has_split_payment
    from {{ ref('stg_order_payments') }}
    group by 1

),

items_agg as (

    select
        order_id,
        count(*)                                          as item_count,
        sum(price)                                        as items_subtotal,
        sum(freight_value)                                as freight_total,
        sum(item_total)                                   as gross_order_value,
        count(distinct product_id)                        as distinct_product_count,
        count(distinct seller_id)                         as distinct_seller_count
    from {{ ref('stg_order_items') }}
    group by 1

),

reviews_agg as (

    select
        order_id,
        max(review_score)                                 as review_score,
        max(review_sentiment)                             as review_sentiment,
        max(has_comment::int)::boolean                    as has_review_comment,
        min(review_created_date)                          as review_created_date
    from {{ ref('stg_order_reviews') }}
    group by 1

)

select
    o.*,
    coalesce(p.total_revenue,          0) as total_revenue,
    coalesce(p.payment_count,          0) as payment_count,
    coalesce(p.max_installments,       1) as max_installments,
    p.primary_payment_type,
    coalesce(p.has_split_payment,  false) as has_split_payment,
    coalesce(i.item_count,             0) as item_count,
    coalesce(i.items_subtotal,         0) as items_subtotal,
    coalesce(i.freight_total,          0) as freight_total,
    coalesce(i.gross_order_value,      0) as gross_order_value,
    coalesce(i.distinct_product_count, 0) as distinct_product_count,
    coalesce(i.distinct_seller_count,  0) as distinct_seller_count,
    r.review_score,
    r.review_sentiment,
    coalesce(r.has_review_comment, false) as has_review_comment,
    r.review_created_date

from orders          o
left join payments_agg p using (order_id)
left join items_agg    i using (order_id)
left join reviews_agg  r using (order_id)