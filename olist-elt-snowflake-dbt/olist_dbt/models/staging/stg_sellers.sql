with source as (

    select * from {{ source('raw', 'sellers') }}

),

renamed as (

    select
        seller_id,
        seller_zip_code_prefix                            as zip_code_prefix,
        trim(lower(seller_city))                          as seller_city,
        trim(upper(seller_state))                         as seller_state,
        _load_timestamp
    from source

),

deduped as (

    select *,
        row_number() over (
            partition by seller_id
            order by _load_timestamp desc
        ) as rn
    from renamed

)

select
    seller_id,
    zip_code_prefix,
    seller_city,
    seller_state,
    _load_timestamp
from deduped
where rn = 1