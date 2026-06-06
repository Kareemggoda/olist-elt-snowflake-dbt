{{
  config(
    materialized = 'table'
  )
}}

with sellers as (

    select * from {{ ref('stg_sellers') }}

),

geo as (

    select * from {{ ref('stg_geolocation') }}

),

perf as (

    select * from {{ ref('int_order_delivery_metrics') }}

)

select
    s.seller_id,
    s.zip_code_prefix,
    s.seller_city,
    s.seller_state,
    g.latitude,
    g.longitude,

    coalesce(p.total_orders,        0) as total_orders,
    coalesce(p.delivered_orders,    0) as delivered_orders,
    coalesce(p.total_gmv,           0) as total_gmv,
    coalesce(p.avg_item_price,      0) as avg_item_price,
    p.avg_delivery_days,
    coalesce(p.late_delivery_count, 0) as late_delivery_count,
    p.avg_delay_days,
    coalesce(p.late_delivery_rate,  0) as late_delivery_rate,

    case
        when coalesce(p.total_gmv, 0) = 0 then 'inactive'
        when p.total_gmv < 1000           then 'micro'
        when p.total_gmv < 10000          then 'small'
        when p.total_gmv < 50000          then 'medium'
        else 'large'
    end                                                   as seller_tier,

    current_timestamp()                                   as dbt_updated_at

from sellers        s
left join geo       g using (zip_code_prefix)
left join perf      p using (seller_id)