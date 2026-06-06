with source as (

    select * from {{ source('raw', 'orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        order_status,

        order_purchase_timestamp                           as order_purchase_at,
        order_purchase_timestamp::date                    as order_purchase_date,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,

        datediff('day',
            order_purchase_timestamp,
            order_delivered_customer_date)                as actual_delivery_days,

        datediff('day',
            order_purchase_timestamp,
            order_estimated_delivery_date)                as estimated_delivery_days,

        datediff('day',
            order_estimated_delivery_date,
            order_delivered_customer_date)                as delivery_delay_days,

        case
            when order_delivered_customer_date > order_estimated_delivery_date
            then true else false
        end                                               as is_late_delivery,

        case
            when order_status = 'delivered'
            then true else false
        end                                               as is_delivered,

        _load_timestamp

    from source

)

select * from renamed