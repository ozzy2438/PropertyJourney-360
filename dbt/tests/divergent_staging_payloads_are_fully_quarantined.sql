with variant_rows as (
    select 'salesforce_account' as record_type, account_source_id as record_key,
        orchestration_window_id, payload_fingerprint, payload_variant_count,
        payload_duplicate_row_count, explained_duplicate_count, dq_status
    from {{ ref('stg_salesforce_accounts') }}
    where account_source_id is not null and source_updated_at is not null
    union all
    select 'salesforce_contact', source_record_id, orchestration_window_id,
        payload_fingerprint, payload_variant_count, payload_duplicate_row_count,
        explained_duplicate_count, dq_status
    from {{ ref('stg_salesforce_contacts') }}
    where source_record_id is not null and source_updated_at is not null
    union all
    select 'legacy_customer', source_record_id, orchestration_window_id,
        payload_fingerprint, payload_variant_count, payload_duplicate_row_count,
        explained_duplicate_count, dq_status
    from {{ ref('stg_legacy_customers') }}
    where source_record_id is not null and source_updated_at is not null
    union all
    select 'support_customer', source_record_id, orchestration_window_id,
        payload_fingerprint, payload_variant_count, payload_duplicate_row_count,
        explained_duplicate_count, dq_status
    from {{ ref('stg_support_customers') }}
    where source_record_id is not null and source_updated_at is not null
    union all
    select 'salesforce_case', case_source_id, orchestration_window_id,
        payload_fingerprint, payload_variant_count, payload_duplicate_row_count,
        explained_duplicate_count, dq_status
    from {{ ref('stg_salesforce_cases') }}
    where case_source_id is not null and source_updated_at is not null
    union all
    select 'support_case', case_source_id, orchestration_window_id,
        payload_fingerprint, payload_variant_count, payload_duplicate_row_count,
        explained_duplicate_count, dq_status
    from {{ ref('stg_support_cases') }}
    where case_source_id is not null and source_updated_at is not null
    union all
    select 'marketing_consent', consent_event_id, orchestration_window_id,
        payload_fingerprint, payload_variant_count, payload_duplicate_row_count,
        explained_duplicate_count, dq_status
    from {{ ref('stg_marketing_consents') }}
    where consent_event_id is not null and occurred_at is not null
    union all
    select 'property_event', event_id, orchestration_window_id,
        payload_fingerprint, payload_variant_count, payload_duplicate_row_count,
        explained_duplicate_count, dq_status
    from {{ ref('stg_property_events') }}
    where event_id is not null
    union all
    select 'property', property_source_id, orchestration_window_id,
        payload_fingerprint, payload_variant_count, payload_duplicate_row_count,
        explained_duplicate_count, dq_status
    from {{ ref('stg_property_master') }}
    where property_source_id is not null and source_updated_at is not null
    union all
    select 'market', to_json_string(struct(geography_code, period_start, property_type)),
        orchestration_window_id, payload_fingerprint, payload_variant_count,
        payload_duplicate_row_count, explained_duplicate_count, dq_status
    from {{ ref('stg_market_data') }}
    where geography_code is not null and period_start is not null and property_type is not null
    union all
    select 'demographic', to_json_string(struct(geography_code, reference_period)),
        orchestration_window_id, payload_fingerprint, payload_variant_count,
        payload_duplicate_row_count, explained_duplicate_count, dq_status
    from {{ ref('stg_abs_demographics') }}
    where geography_code is not null and reference_period is not null
    union all
    select 'rba_indicator', to_json_string(struct(series_id, observation_date)),
        orchestration_window_id, payload_fingerprint, payload_variant_count,
        payload_duplicate_row_count, explained_duplicate_count, dq_status
    from {{ ref('stg_rba_indicators') }}
    where series_id is not null and observation_date is not null and observation_value is not null
),
violations as (
    select record_type, record_key, orchestration_window_id
    from variant_rows
    where payload_variant_count > 1
    group by record_type, record_key, orchestration_window_id
    having count(*) != max(payload_variant_count)
        or count(distinct payload_fingerprint) != max(payload_variant_count)
        or countif(dq_status != 'CONFLICTING_DUPLICATE') > 0
        or sum(explained_duplicate_count) != sum(payload_duplicate_row_count - 1)
)
select * from violations
