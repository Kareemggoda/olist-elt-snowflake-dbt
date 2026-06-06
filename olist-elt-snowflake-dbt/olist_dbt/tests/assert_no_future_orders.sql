select
    order_id,
    order_purchase_date
from {{ ref('fct_orders') }}
where order_purchase_date > current_date