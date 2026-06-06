-- Fails if any review score is outside 1-5
select
    review_id,
    review_score
from {{ ref('fct_order_reviews') }}
where review_score not between 1 and 5