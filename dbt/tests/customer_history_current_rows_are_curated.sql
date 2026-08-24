select history.customer_id
from {{ ref('dim_customer_history') }} history
left join {{ ref('dim_customer') }} current_customers using (customer_id)
where history.is_current
  and current_customers.customer_id is null
