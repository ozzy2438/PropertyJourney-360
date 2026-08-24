select ingestion_key
from {{ ref('stg_property_events') }}
where dq_status = 'VALID'
  and customer_id is not null
  and customer_source_id is not null
  and customer_id != source_subject_customer_id
