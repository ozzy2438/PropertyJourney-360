select 'salesforce_account' as record_type, ingestion_key
from {{ ref('stg_salesforce_accounts') }}
where dq_status = 'VALID' and (account_source_id is null or source_updated_at is null)
union all
select 'salesforce_contact', ingestion_key
from {{ ref('stg_salesforce_contacts') }}
where dq_status = 'VALID' and (source_record_id is null or source_updated_at is null)
union all
select 'legacy_customer', ingestion_key
from {{ ref('stg_legacy_customers') }}
where dq_status = 'VALID' and (source_record_id is null or source_updated_at is null)
union all
select 'support_customer', ingestion_key
from {{ ref('stg_support_customers') }}
where dq_status = 'VALID' and (source_record_id is null or source_updated_at is null)
union all
select 'salesforce_case', ingestion_key
from {{ ref('stg_salesforce_cases') }}
where dq_status = 'VALID'
  and (case_source_id is null or source_updated_at is null or opened_at is null)
union all
select 'support_case', ingestion_key
from {{ ref('stg_support_cases') }}
where dq_status = 'VALID'
  and (case_source_id is null or source_updated_at is null or opened_at is null)
union all
select 'marketing_consent', ingestion_key
from {{ ref('stg_marketing_consents') }}
where dq_status = 'VALID' and (consent_event_id is null or occurred_at is null)
union all
select 'property_event', ingestion_key
from {{ ref('stg_property_events') }}
where dq_status = 'VALID' and (event_id is null or occurred_at is null or received_at is null)
union all
select 'property', ingestion_key
from {{ ref('stg_property_master') }}
where dq_status = 'VALID' and (property_source_id is null or source_updated_at is null)
union all
select 'market', market_record_id
from {{ ref('stg_market_data') }}
where dq_status = 'VALID' and (geography_code is null or period_start is null or property_type is null)
union all
select 'demographic', demographic_record_id
from {{ ref('stg_abs_demographics') }}
where dq_status = 'VALID' and (geography_code is null or reference_period is null)
union all
select 'rba_indicator', indicator_record_id
from {{ ref('stg_rba_indicators') }}
where dq_status = 'VALID'
  and (series_id is null or observation_date is null or observation_value is null)
