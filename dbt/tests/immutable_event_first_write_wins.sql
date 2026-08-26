with immutable_rows as (
    select
        'property_event' as record_type,
        event_id as record_key,
        orchestration_window_id,
        ingested_at,
        payload_fingerprint,
        payload_variant_count,
        dq_status
    from {{ ref('stg_property_events') }}
    where event_id is not null

    union all

    select
        'marketing_consent',
        consent_event_id,
        orchestration_window_id,
        ingested_at,
        payload_fingerprint,
        payload_variant_count,
        dq_status
    from {{ ref('stg_marketing_consents') }}
    where consent_event_id is not null
),
ordered as (
    select
        *,
        first_value(payload_fingerprint) over (
            partition by record_type, record_key
            order by ingested_at is null, ingested_at, orchestration_window_id, payload_fingerprint
            rows between unbounded preceding and unbounded following
        ) as expected_canonical_payload_fingerprint,
        first_value(orchestration_window_id) over (
            partition by record_type, record_key
            order by ingested_at is null, ingested_at, orchestration_window_id, payload_fingerprint
            rows between unbounded preceding and unbounded following
        ) as expected_canonical_window_id
    from immutable_rows
)
select *
from ordered
where payload_variant_count = 1
  and (
      (
          orchestration_window_id = expected_canonical_window_id
          and dq_status in ('EXACT_DUPLICATE', 'EVENT_ID_COLLISION')
      )
      or (
          orchestration_window_id != expected_canonical_window_id
          and payload_fingerprint = expected_canonical_payload_fingerprint
          and dq_status != 'EXACT_DUPLICATE'
      )
      or (
          payload_fingerprint != expected_canonical_payload_fingerprint
          and dq_status != 'EVENT_ID_COLLISION'
      )
  )
