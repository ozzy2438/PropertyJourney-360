{% set run_window_id = var('orchestration_window_id', '') %}

with missing_events as (
    select events.event_id as record_id
    from {{ ref('int_event_identity_attribution') }} events
    left join {{ ref('fact_customer_property_interaction') }} facts
        on events.event_id = facts.interaction_id
    where '{{ run_window_id }}' != ''
      and events.orchestration_window_id = '{{ run_window_id }}'
      and facts.interaction_id is null
),
missing_consents as (
    select consents.consent_event_id as record_id
    from {{ ref('stg_marketing_consents') }} consents
    left join {{ ref('fact_consent_change') }} facts
        on consents.consent_event_id = facts.consent_change_id
    where '{{ run_window_id }}' != ''
      and consents.orchestration_window_id = '{{ run_window_id }}'
      and consents.dq_status = 'VALID'
      and facts.consent_change_id is null
),
missing_cases as (
    select {{ stable_key(['cases.source_system', 'cases.case_source_id']) }} as record_id
    from (
        select source_system, case_source_id, orchestration_window_id
        from {{ ref('stg_salesforce_cases') }} where dq_status = 'VALID'
        union all
        select source_system, case_source_id, orchestration_window_id
        from {{ ref('stg_support_cases') }} where dq_status = 'VALID'
    ) cases
    left join {{ ref('fact_support_case') }} facts
        on {{ stable_key(['cases.source_system', 'cases.case_source_id']) }} = facts.support_case_id
    where '{{ run_window_id }}' != ''
      and cases.orchestration_window_id = '{{ run_window_id }}'
      and facts.support_case_id is null
)
select 'event' as record_type, record_id from missing_events
union all
select 'consent', record_id from missing_consents
union all
select 'case', record_id from missing_cases
