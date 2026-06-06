{{
  config(
    materialized = 'table'
  )
}}

with snapshot as (

    select * from {{ ref('customers_snapshot') }}

),

geo as (

    select * from {{ ref('stg_geolocation') }}

)

select
    s.dbt_scd_id                                          as customer_sk,
    s.customer_id,
    s.customer_unique_id,
    
    -- 🛠️ بناخد الاسم المنظف النهائي للـ Mart
    s.customer_zip_code_prefix                            as zip_code_prefix, 
    s.customer_city,
    s.customer_state,
    g.latitude,
    g.longitude,
    s.dbt_valid_from                                       as valid_from,
    s.dbt_valid_to                                         as valid_to,
    case when s.dbt_valid_to is null
         then true else false
    end                                                   as is_current,
    s.dbt_updated_at

from snapshot      s
-- 🛠️ بنربط الاسم القديم اللي جوه الـ Snapshot مع الاسم الجديد اللي جوه الـ Geolocation
left join geo      g on s.customer_zip_code_prefix = g.zip_code_prefix