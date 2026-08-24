{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ingestion_key',
    partition_by={'field': 'event_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['event_type', 'property_id'],
    on_schema_change='fail'
) }}

{% set run_window_id = var('orchestration_window_id', '') %}

with known_properties as (
    select property_source_id as property_id
    from {{ ref('stg_property_master') }}
    qualify row_number() over (
        partition by property_source_id
        order by source_updated_at desc, ingested_at desc, ingestion_key desc
    ) = 1 and dq_status = 'VALID'
),
known_subjects as (
    select
        identities.source_system,
        identities.source_record_id,
        count(distinct membership.customer_id) as mapped_customer_count,
        array_agg(
            distinct membership.customer_id order by membership.customer_id limit 1
        )[offset(0)] as source_subject_customer_id
    from {{ ref('int_customer_identity_union') }} identities
    inner join {{ ref('int_identity_membership') }} membership using (source_identity_key)
    where identities.dq_status = 'VALID'
      and membership.is_active
      and (not membership.cluster_conflict or membership.registry_preserved)
    group by identities.source_system, identities.source_record_id
),
known_customers as (
    select distinct customer_id
    from {{ ref('int_identity_membership') }}
    where is_active and (not cluster_conflict or registry_preserved)
),
normalized as (
    select
        cast(raw_events.event_id as string) as event_id,
        upper(nullif(trim(cast(raw_events.event_type as string)), '')) as event_type,
        cast(raw_events.session_id as string) as session_id,
        cast(raw_events.anonymous_id as string) as anonymous_id,
        cast(raw_events.customer_id as string) as customer_id,
        lower(nullif(trim(cast(raw_events.customer_source_system as string)), ''))
            as customer_source_system,
        cast(raw_events.customer_source_id as string) as customer_source_id,
        cast(raw_events.property_id as string) as property_id,
        safe_cast(raw_events.price_observed as numeric) as price_observed,
        lower(nullif(trim(cast(raw_events.device_type as string)), '')) as device_type,
        to_json_string(raw_events.attributes) as attributes_json,
        safe_cast(raw_events.schema_version as int64) as schema_version,
        safe_cast(raw_events.occurred_at as timestamp) as occurred_at,
        safe_cast(raw_events.received_at as timestamp) as received_at,
        date(safe_cast(raw_events.occurred_at as timestamp), 'Australia/Melbourne') as event_date,
        safe_cast(raw_events._ingested_at as timestamp) as ingested_at,
        cast(raw_events._window_id as string) as orchestration_window_id,
        known_properties.property_id is not null as property_exists,
        coalesce(known_subjects.mapped_customer_count = 1, false) as source_subject_exists,
        known_subjects.source_subject_customer_id,
        known_customers.customer_id is not null as canonical_customer_exists
    from {{ source('raw', 'raw_property_events') }} raw_events
    left join known_properties
        on cast(raw_events.property_id as string) = known_properties.property_id
    left join known_subjects
        on lower(nullif(trim(cast(raw_events.customer_source_system as string)), ''))
            = known_subjects.source_system
        and cast(raw_events.customer_source_id as string) = known_subjects.source_record_id
    left join known_customers
        on cast(raw_events.customer_id as string) = known_customers.customer_id
    {% if is_incremental() %}
    where
        {% if run_window_id %}
        cast(_window_id as string) = '{{ run_window_id }}'
        {% else %}
        safe_cast(_ingested_at as timestamp) >= timestamp_sub(
            (select coalesce(max(ingested_at), timestamp('1970-01-01')) from {{ this }}),
            interval {{ var('event_lookback_days', 3) }} day
        )
        {% endif %}
    {% endif %}
),
fingerprinted as (
    select
        *,
        to_hex(sha256(to_json_string(struct(
            event_id,
            event_type,
            session_id,
            anonymous_id,
            customer_id,
            customer_source_system,
            customer_source_id,
            property_id,
            price_observed,
            device_type,
            attributes_json,
            schema_version,
            occurred_at,
            received_at
        )))) as payload_fingerprint
    from normalized
),
historical_payloads as (
    {% if is_incremental() %}
    select
        historical.event_id,
        historical.payload_fingerprint,
        historical.ingested_at,
        historical.orchestration_window_id
    from {{ this }} historical
    where historical.event_id is not null
      and not exists (
          select 1
          from fingerprinted current_rows
          where current_rows.event_id = historical.event_id
            and current_rows.orchestration_window_id = historical.orchestration_window_id
      )
    {% else %}
    select
        cast(null as string) as event_id,
        cast(null as string) as payload_fingerprint,
        cast(null as timestamp) as ingested_at,
        cast(null as string) as orchestration_window_id
    where false
    {% endif %}
),
all_event_payloads as (
    select event_id, payload_fingerprint, ingested_at, orchestration_window_id
    from fingerprinted
    where event_id is not null
    union all
    select event_id, payload_fingerprint, ingested_at, orchestration_window_id
    from historical_payloads
),
event_payload_stats as (
    select
        event_id,
        count(distinct payload_fingerprint) as immutable_payload_variant_count,
        array_agg(
            payload_fingerprint
            order by ingested_at is null, ingested_at, orchestration_window_id, payload_fingerprint
            limit 1
        )[offset(0)] as canonical_payload_fingerprint,
        array_agg(
            orchestration_window_id
            order by ingested_at is null, ingested_at, orchestration_window_id, payload_fingerprint
            limit 1
        )[offset(0)] as canonical_orchestration_window_id
    from all_event_payloads
    group by event_id
),
window_ranked as (
    select
        *,
        count(distinct payload_fingerprint) over (
            partition by event_id, orchestration_window_id
        ) as payload_variant_count,
        count(*) over (
            partition by event_id, orchestration_window_id, payload_fingerprint
        ) as payload_duplicate_row_count,
        row_number() over (
            partition by event_id, orchestration_window_id, payload_fingerprint
            order by ingested_at desc, payload_fingerprint desc
        ) as row_num
    from fingerprinted
),
source_rows as (
    select
        window_ranked.*,
        case
            when window_ranked.event_id is null then 0
            when window_ranked.orchestration_window_id
                = event_payload_stats.canonical_orchestration_window_id
                then window_ranked.payload_duplicate_row_count - 1
            when window_ranked.payload_fingerprint
                = event_payload_stats.canonical_payload_fingerprint
                then window_ranked.payload_duplicate_row_count
            else window_ranked.payload_duplicate_row_count - 1
        end as explained_duplicate_count,
        event_payload_stats.immutable_payload_variant_count,
        event_payload_stats.canonical_payload_fingerprint,
        event_payload_stats.canonical_orchestration_window_id
    from window_ranked
    left join event_payload_stats using (event_id)
)
select
    case
        when event_id is null
            then {{ stable_key(["'web_event'", 'orchestration_window_id', 'payload_fingerprint', 'row_num']) }}
        when payload_variant_count > 1
            then {{ stable_key(["'web_event'", 'event_id', 'orchestration_window_id', 'payload_fingerprint']) }}
        else {{ stable_key(["'web_event'", 'event_id', 'occurred_at', 'orchestration_window_id']) }}
    end as ingestion_key,
    'web_event' as source_system,
    * except (row_num),
    case
        when event_id is null then 'INVALID_KEY'
        when payload_variant_count > 1 then 'CONFLICTING_DUPLICATE'
        when orchestration_window_id != canonical_orchestration_window_id
            and payload_fingerprint = canonical_payload_fingerprint
            then 'EXACT_DUPLICATE'
        when payload_fingerprint != canonical_payload_fingerprint then 'EVENT_ID_COLLISION'
        when occurred_at is null or received_at is null or received_at < occurred_at
            then 'INVALID_TIMESTAMP'
        when event_type not in ('VIEW', 'SAVE', 'ENQUIRY', 'SEARCH', 'IDENTIFY') then 'INVALID_EVENT_TYPE'
        when schema_version not in (1, 2) then 'UNSUPPORTED_SCHEMA'
        when schema_version = 1 and device_type is not null then 'V1_DEVICE_TYPE_NOT_ALLOWED'
        when anonymous_id is null
            and customer_id is null
            and customer_source_system is null
            and customer_source_id is null
            then 'MISSING_EVENT_IDENTITY'
        when event_type = 'IDENTIFY'
            and customer_id is null
            and (customer_source_system is null or customer_source_id is null)
            then 'MISSING_IDENTIFY_SUBJECT'
        when customer_id is not null and not canonical_customer_exists then 'ORPHAN_SUBJECT'
        when (customer_source_system is null) != (customer_source_id is null)
            then 'ORPHAN_SUBJECT'
        when customer_source_id is not null and not source_subject_exists then 'ORPHAN_SUBJECT'
        when customer_id is not null
            and customer_source_id is not null
            and customer_id != source_subject_customer_id
            then 'CONFLICTING_SUBJECT'
        when event_type in ('VIEW', 'SAVE', 'ENQUIRY')
            and (property_id is null or not property_exists) then 'ORPHAN_PROPERTY'
        else 'VALID'
    end as dq_status
from source_rows
where row_num = 1 or event_id is null
