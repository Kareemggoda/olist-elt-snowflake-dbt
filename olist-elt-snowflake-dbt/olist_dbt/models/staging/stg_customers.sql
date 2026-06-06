with source as (

    select * from {{ source('raw', 'customers') }}

),

renamed as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix                          as zip_code_prefix,
        trim(lower(customer_city))                        as customer_city,
        trim(upper(customer_state))                       as customer_state,
        _load_timestamp
    from source

),

deduped as (

    select *,
        row_number() over (
            partition by customer_id
            order by _load_timestamp desc
        ) as rn
    from renamed

)

select
    customer_id,
    customer_unique_id,
    zip_code_prefix,
    customer_city,
    customer_state,
    _load_timestamp
from deduped
where rn = 1