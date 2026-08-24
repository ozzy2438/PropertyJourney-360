select
    'salesforce_accounts' as asset_name,
    max(source_updated_at) as source_high_watermark,
    max(ingested_at) as latest_ingested_at,
    count(*) as staged_record_count
from {{ ref('stg_salesforce_accounts') }}
union all
select 'salesforce_contacts', max(source_updated_at), max(ingested_at), count(*) from {{ ref('stg_salesforce_contacts') }}
union all
select 'salesforce_cases', max(source_updated_at), max(ingested_at), count(*) from {{ ref('stg_salesforce_cases') }}
union all
select 'legacy_customers', max(source_updated_at), max(ingested_at), count(*) from {{ ref('stg_legacy_customers') }}
union all
select 'support_cases', max(source_updated_at), max(ingested_at), count(*) from {{ ref('stg_support_cases') }}
union all
select 'marketing_consents', max(occurred_at), max(ingested_at), count(*) from {{ ref('stg_marketing_consents') }}
union all
select 'property_events', max(occurred_at), max(ingested_at), count(*) from {{ ref('stg_property_events') }}
union all
select 'property_master', max(source_updated_at), max(ingested_at), count(*) from {{ ref('stg_property_master') }}
union all
select 'property_market', max(source_retrieved_at), max(ingested_at), count(*) from {{ ref('stg_market_data') }}
union all
select 'abs_demographics', max(source_retrieved_at), max(ingested_at), count(*) from {{ ref('stg_abs_demographics') }}
union all
select 'rba_indicators', max(source_retrieved_at), max(ingested_at), count(*) from {{ ref('stg_rba_indicators') }}
