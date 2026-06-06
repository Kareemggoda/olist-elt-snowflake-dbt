{% snapshot customers_snapshot %}

{{
  config(
    target_schema  = 'snapshots',
    unique_key     = 'customer_id',
    strategy       = 'check',
    check_cols     = ['customer_city', 'customer_state',
                      'customer_zip_code_prefix']
  )
}}

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    _load_timestamp
from {{ source('raw', 'customers') }}

{% endsnapshot %}