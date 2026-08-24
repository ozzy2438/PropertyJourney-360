with staged_issues as (
    select 'salesforce_contact' as record_type, source_system, source_record_id, ingestion_key, dq_status, ingested_at
    from {{ ref('stg_salesforce_contacts') }} where dq_status != 'VALID'
    union all
    select 'legacy_customer', source_system, source_record_id, ingestion_key, dq_status, ingested_at
    from {{ ref('stg_legacy_customers') }} where dq_status != 'VALID'
    union all
    select 'support_customer', source_system, source_record_id, ingestion_key, dq_status, ingested_at
    from {{ ref('stg_support_customers') }} where dq_status != 'VALID'
    union all
    select 'salesforce_account', source_system, account_source_id, ingestion_key, dq_status, ingested_at
    from {{ ref('stg_salesforce_accounts') }} where dq_status != 'VALID'
    union all
    select 'salesforce_case', source_system, case_source_id, ingestion_key, dq_status, ingested_at
    from {{ ref('stg_salesforce_cases') }} where dq_status != 'VALID'
    union all
    select 'support_case', source_system, case_source_id, ingestion_key, dq_status, ingested_at
    from {{ ref('stg_support_cases') }} where dq_status != 'VALID'
    union all
    select 'marketing_consent', source_system, consent_event_id, ingestion_key, dq_status, ingested_at
    from {{ ref('stg_marketing_consents') }} where dq_status != 'VALID'
    union all
    select 'property_event', source_system, event_id, ingestion_key, dq_status, ingested_at
    from {{ ref('stg_property_events') }} where dq_status != 'VALID'
    union all
    select 'property', source_system, property_source_id, ingestion_key, dq_status, ingested_at
    from {{ ref('stg_property_master') }} where dq_status != 'VALID'
),
identity_review as (
    select
        'identity_candidate' as record_type,
        'identity_resolution' as source_system,
        candidate_id as source_record_id,
        candidate_id as ingestion_key,
        'REVIEW_REQUIRED' as dq_status,
        greatest(left_identity.source_updated_at, right_identity.source_updated_at) as ingested_at
    from {{ ref('int_identity_decisions') }} decisions
    inner join {{ ref('int_customer_identity_union') }} left_identity
        on decisions.left_identity_key = left_identity.source_identity_key
    inner join {{ ref('int_customer_identity_union') }} right_identity
        on decisions.right_identity_key = right_identity.source_identity_key
    where decisions.decision = 'REVIEW_REQUIRED'
),
volume_issues as (
    select
        'source_volume' as record_type,
        'reconciliation' as source_system,
        reconciliation_result_id as source_record_id,
        reconciliation_result_id as ingestion_key,
        'SOURCE_VOLUME_ANOMALY' as dq_status,
        reconciled_at as ingested_at
    from {{ ref('dq_volume_anomaly') }}
    where volume_status = 'ANOMALY'
),
ambiguous_anonymous_ids as (
    select
        'property_event' as record_type,
        source_system,
        event_id as source_record_id,
        ingestion_key,
        'AMBIGUOUS_ANONYMOUS_ID' as dq_status,
        ingested_at
    from {{ ref('int_event_identity_attribution') }}
    where attribution_method = 'AMBIGUOUS_ANONYMOUS_ID'
),
all_issues as (
    select * from staged_issues
    union all
    select * from identity_review
    union all
    select * from volume_issues
    union all
    select * from ambiguous_anonymous_ids
)
select
    {{ stable_key(['record_type', 'ingestion_key', 'dq_status']) }} as issue_id,
    record_type,
    source_system,
    source_record_id,
    ingestion_key as lineage_ingestion_key,
    dq_status as issue_code,
    case
        when dq_status in (
            'INVALID_KEY', 'UNSUPPORTED_SCHEMA', 'ORPHAN_SUBJECT',
            'ORPHAN_PROPERTY', 'MISSING_IDENTIFY_SUBJECT'
        ) then 'HIGH'
        when dq_status in ('REVIEW_REQUIRED', 'SOURCE_VOLUME_ANOMALY', 'AMBIGUOUS_ANONYMOUS_ID') then 'MEDIUM'
        else 'LOW'
    end as severity,
    case
        when dq_status in ('REVIEW_REQUIRED', 'AMBIGUOUS_ANONYMOUS_ID') then 'IDENTITY_REVIEW'
        when dq_status = 'SOURCE_VOLUME_ANOMALY' then 'VOLUME_ANOMALY'
        else 'RECORD_VALIDATION'
    end as issue_type,
    ingested_at as detected_at
from all_issues
