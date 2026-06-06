with items as (

    select * from {{ ref('stg_order_items') }}

),

orders as (

    select
        order_id,
        actual_delivery_days,
        delivery_delay_days,
        is_late_delivery,
        is_delivered
    from {{ ref('stg_orders') }}

),

joined as (

    select
        i.seller_id,
        o.order_id,
        o.actual_delivery_days,
        o.delivery_delay_days,
        o.is_late_delivery,
        o.is_delivered,
        i.price,
        i.freight_value
    from items  i
    join orders o using (order_id)

)

select
    seller_id,
    count(distinct order_id)                              as total_orders,
    count(distinct case when is_delivered
                   then order_id end)                     as delivered_orders,
    sum(price)                                            as total_gmv,
    avg(price)                                            as avg_item_price,
    avg(case when is_delivered
             then actual_delivery_days end)               as avg_delivery_days,
    sum(case when is_late_delivery then 1 else 0 end)     as late_delivery_count,
    avg(case when is_late_delivery
             then delivery_delay_days end)                as avg_delay_days,
             
    sum(case when is_late_delivery then 1 else 0 end) / 
    nullif(count(distinct case when is_delivered then order_id end), 0) as late_delivery_rate

from joined
group by 1