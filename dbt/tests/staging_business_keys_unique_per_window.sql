with violations as (
    select 'salesforce_account' as record_type, account_source_id as record_key
    from {{ ref('stg_salesforce_accounts') }} where dq_status != 'INVALID_KEY'
    group by account_source_id, orchestration_window_id, payload_fingerprint having count(*) > 1
    union all
    select 'salesforce_contact', source_record_id
    from {{ ref('stg_salesforce_contacts') }} where dq_status != 'INVALID_KEY'
    group by source_record_id, orchestration_window_id, payload_fingerprint having count(*) > 1
    union all
    select 'legacy_customer', source_record_id
    from {{ ref('stg_legacy_customers') }} where dq_status != 'INVALID_KEY'
    group by source_record_id, orchestration_window_id, payload_fingerprint having count(*) > 1
    union all
    select 'support_customer', source_record_id
    from {{ ref('stg_support_customers') }} where dq_status != 'INVALID_KEY'
    group by source_record_id, orchestration_window_id, payload_fingerprint having count(*) > 1
    union all
    select 'salesforce_case', case_source_id
    from {{ ref('stg_salesforce_cases') }} where dq_status != 'INVALID_KEY'
    group by case_source_id, orchestration_window_id, payload_fingerprint having count(*) > 1
    union all
    select 'support_case', case_source_id
    from {{ ref('stg_support_cases') }} where dq_status != 'INVALID_KEY'
    group by case_source_id, orchestration_window_id, payload_fingerprint having count(*) > 1
    union all
    select 'marketing_consent', consent_event_id
    from {{ ref('stg_marketing_consents') }} where dq_status != 'INVALID_KEY'
    group by consent_event_id, orchestration_window_id, payload_fingerprint having count(*) > 1
    union all
    select 'property_event', event_id
    from {{ ref('stg_property_events') }} where dq_status != 'INVALID_KEY'
    group by event_id, orchestration_window_id, payload_fingerprint having count(*) > 1
    union all
    select 'property', property_source_id
    from {{ ref('stg_property_master') }} where dq_status != 'INVALID_KEY'
    group by property_source_id, orchestration_window_id, payload_fingerprint having count(*) > 1
    union all
    select 'market', to_json_string(struct(geography_code, period_start, property_type))
    from {{ ref('stg_market_data') }} where dq_status != 'INVALID_KEY'
    group by geography_code, period_start, property_type, orchestration_window_id,
        payload_fingerprint having count(*) > 1
    union all
    select 'demographic', to_json_string(struct(geography_code, reference_period))
    from {{ ref('stg_abs_demographics') }} where dq_status != 'INVALID_KEY'
    group by geography_code, reference_period, orchestration_window_id,
        payload_fingerprint having count(*) > 1
    union all
    select 'rba_indicator', to_json_string(struct(series_id, observation_date))
    from {{ ref('stg_rba_indicators') }} where dq_status != 'INVALID_KEY'
    group by series_id, observation_date, orchestration_window_id,
        payload_fingerprint having count(*) > 1
)
select * from violations
