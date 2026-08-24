select customer_id
from {{ ref('dim_customer_history') }}
group by customer_id
having countif(is_current) != 1
