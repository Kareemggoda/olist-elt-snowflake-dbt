with source as (

    select * from {{ source('raw', 'geolocation') }}

),

renamed as (

    select
        geolocation_zip_code_prefix                       as zip_code_prefix,
        geolocation_lat                                   as latitude,
        geolocation_lng                                   as longitude,
        trim(lower(geolocation_city))                     as city,
        trim(upper(geolocation_state))                    as state
    from source

),

deduped as (

    select *,
        row_number() over (
            partition by zip_code_prefix
            order by zip_code_prefix
        ) as rn
    from renamed

)

select
    zip_code_prefix,
    latitude,
    longitude,
    city,
    state
from deduped
where rn = 1