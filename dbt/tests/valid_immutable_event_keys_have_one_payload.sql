with violations as (
    select 'property_event' as record_type, event_id as record_key
    from {{ ref('stg_property_events') }}
    where dq_status = 'VALID'
    group by event_id
    having count(distinct payload_fingerprint) > 1

    union all

    select 'marketing_consent', consent_event_id
    from {{ ref('stg_marketing_consents') }}
    where dq_status = 'VALID'
    group by consent_event_id
    having count(distinct payload_fingerprint) > 1
)
select * from violations
