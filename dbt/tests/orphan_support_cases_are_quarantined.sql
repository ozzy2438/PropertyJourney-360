select cases.case_source_id
from {{ ref('stg_support_cases') }} cases
where not cases.customer_exists
  and cases.dq_status != 'ORPHAN_SUBJECT'
