select
    left_history.customer_id,
    left_history.customer_history_sk,
    right_history.customer_history_sk
from {{ ref('dim_customer_history') }} left_history
inner join {{ ref('dim_customer_history') }} right_history
    on left_history.customer_id = right_history.customer_id
    and left_history.customer_history_sk < right_history.customer_history_sk
    and left_history.valid_from < coalesce(right_history.valid_to, timestamp('9999-12-31'))
    and right_history.valid_from < coalesce(left_history.valid_to, timestamp('9999-12-31'))
