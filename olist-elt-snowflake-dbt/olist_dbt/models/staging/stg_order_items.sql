with source as (

    select * from {{ source('raw', 'order_items') }}

),

renamed as (

    select
        {{ generate_surrogate_key(['order_id', 'order_item_id']) }} as order_item_sk,

        order_id,
        order_item_id                                     as order_item_seq,
        product_id,
        seller_id,

        shipping_limit_date::date                         as shipping_limit_date,

        price,
        freight_value,
        price + freight_value                             as item_total,

        _load_timestamp

    from source

)

select * from renamed