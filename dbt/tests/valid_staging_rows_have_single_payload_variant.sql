select 'salesforce_account' as record_type, ingestion_key as record_key
from {{ ref('stg_salesforce_accounts') }}
where dq_status = 'VALID' and payload_variant_count > 1
union all
select 'salesforce_contact', ingestion_key
from {{ ref('stg_salesforce_contacts') }}
where dq_status = 'VALID' and payload_variant_count > 1
union all
select 'legacy_customer', ingestion_key
from {{ ref('stg_legacy_customers') }}
where dq_status = 'VALID' and payload_variant_count > 1
union all
select 'support_customer', ingestion_key
from {{ ref('stg_support_customers') }}
where dq_status = 'VALID' and payload_variant_count > 1
union all
select 'salesforce_case', ingestion_key
from {{ ref('stg_salesforce_cases') }}
where dq_status = 'VALID' and payload_variant_count > 1
union all
select 'support_case', ingestion_key
from {{ ref('stg_support_cases') }}
where dq_status = 'VALID' and payload_variant_count > 1
union all
select 'marketing_consent', ingestion_key
from {{ ref('stg_marketing_consents') }}
where dq_status = 'VALID' and payload_variant_count > 1
union all
select 'property_event', ingestion_key
from {{ ref('stg_property_events') }}
where dq_status = 'VALID' and payload_variant_count > 1
union all
select 'property', ingestion_key
from {{ ref('stg_property_master') }}
where dq_status = 'VALID' and payload_variant_count > 1
union all
select 'market', market_record_id
from {{ ref('stg_market_data') }}
where dq_status = 'VALID' and payload_variant_count > 1
union all
select 'demographic', demographic_record_id
from {{ ref('stg_abs_demographics') }}
where dq_status = 'VALID' and payload_variant_count > 1
union all
select 'rba_indicator', indicator_record_id
from {{ ref('stg_rba_indicators') }}
where dq_status = 'VALID' and payload_variant_count > 1
