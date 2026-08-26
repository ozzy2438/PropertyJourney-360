{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='consent_change_id',
    partition_by={'field': 'occurred_date', 'data_type': 'date', 'granularity': 'month'},
    cluster_by=['customer_id', 'channel'],
    on_schema_change='fail'
) }}

{% set run_window_id = var('orchestration_window_id', '') %}

with selected_consents as (
    select *
    from {{ ref('stg_marketing_consents') }}
    where dq_status = 'VALID'
    {% if is_incremental() and run_window_id %}
      and orchestration_window_id = '{{ run_window_id }}'
    {% elif is_incremental() %}
      and occurred_at >= timestamp_sub(
          (select coalesce(max(occurred_at), timestamp('1970-01-01')) from {{ this }}),
          interval 3 day
      )
    {% endif %}
    qualify row_number() over (
        partition by consent_event_id order by occurred_at desc, ingested_at desc, ingestion_key desc
    ) = 1
)
select
    consents.consent_event_id as consent_change_id,
    identities.customer_id,
    consents.channel,
    consents.consent_status,
    consents.policy_version,
    consents.capture_source,
    consents.customer_source_system,
    consents.customer_source_id,
    consents.occurred_at,
    date(consents.occurred_at, 'Australia/Melbourne') as occurred_date,
    consents.ingested_at,
    consents.ingestion_key as lineage_ingestion_key
from selected_consents consents
inner join {{ ref('bridge_customer_source_identity') }} identities
    on consents.customer_source_system = identities.source_system
    and consents.customer_source_id = identities.source_record_id
