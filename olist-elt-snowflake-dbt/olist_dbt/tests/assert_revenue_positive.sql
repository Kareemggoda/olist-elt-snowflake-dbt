select
    order_id,
    total_revenue
from {{ ref('fct_orders') }}
where is_delivered 
  and total_revenue < 0 