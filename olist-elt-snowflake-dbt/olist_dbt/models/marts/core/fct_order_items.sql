{{
  config(
    materialized     = 'incremental',
    unique_key       = 'order_item_sk',
    cluster_by       = ['shipping_limit_date'],
    on_schema_change = 'sync_all_columns'
  )
}}

with items as (

    select * from {{ ref('stg_order_items') }}

),

orders as (

    select
        order_id,
        order_status,
        order_purchase_date,
        order_purchase_at,
        is_delivered,
        is_late_delivery,
        customer_id
    from {{ ref('stg_orders') }}

),

joined_data as (

    select 
        i.*,
        o.customer_id,
        o.order_purchase_date,
        o.order_status,
        o.order_purchase_at,
        o.is_delivered,
        o.is_late_delivery
    from items i
    join orders o on i.order_id = o.order_id

    {% if is_incremental() %}
    where o.order_purchase_at > (
        select coalesce(max(order_purchase_at), '1970-01-01')
        from {{ this }}
    )
    {% endif %}

)

select
    order_item_sk,
    order_id,
    order_item_seq,
    product_id,
    seller_id,
    customer_id,
    order_purchase_date                                   as date_fk,
    order_status,
    order_purchase_at,
    is_delivered,
    is_late_delivery,
    shipping_limit_date,
    price,
    freight_value,
    item_total,

    round(freight_value / nullif(price, 0) * 100, 2)     as freight_pct_of_price,

    case
        when freight_value = 0     then 'free shipping'
        when freight_value < 20    then 'low freight'
        when freight_value < 50    then 'medium freight'
        else 'high freight'
    end                                                   as freight_tier,

    current_timestamp()                                   as dbt_updated_at

from joined_data
