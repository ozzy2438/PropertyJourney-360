select curated.lineage_ingestion_key
from {{ ref('bridge_customer_source_identity') }} curated
inner join {{ ref('int_customer_identity_union') }} source_identity
    on curated.lineage_ingestion_key = source_identity.ingestion_key
where source_identity.dq_status != 'VALID'
