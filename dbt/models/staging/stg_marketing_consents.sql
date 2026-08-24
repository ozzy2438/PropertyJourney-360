with known_subjects as (
    select distinct identities.source_system, identities.source_record_id
    from {{ ref('int_customer_identity_union') }} identities
    inner join {{ ref('int_identity_membership') }} membership using (source_identity_key)
    where identities.dq_status = 'VALID'
      and membership.is_active
      and (not membership.cluster_conflict or membership.registry_preserved)
),
normalized as (
    select
        cast(raw_consents.consent_event_id as string) as consent_event_id,
        lower(nullif(trim(cast(raw_consents.subject_source_system as string)), ''))
            as customer_source_system,
        cast(raw_consents.subject_source_id as string) as customer_source_id,
        upper(nullif(trim(cast(raw_consents.channel as string)), '')) as channel,
        upper(nullif(trim(cast(raw_consents.consent_status as string)), '')) as consent_status,
        nullif(trim(cast(raw_consents.policy_version as string)), '') as policy_version,
        nullif(trim(cast(raw_consents.capture_source as string)), '') as capture_source,
        safe_cast(raw_consents.occurred_at as timestamp) as occurred_at,
        safe_cast(raw_consents._ingested_at as timestamp) as ingested_at,
        cast(raw_consents._window_id as string) as orchestration_window_id,
        known_subjects.source_record_id is not null as customer_exists
    from {{ source('raw', 'raw_marketing_consents') }} raw_consents
    left join known_subjects
        on lower(nullif(trim(cast(raw_consents.subject_source_system as string)), ''))
            = known_subjects.source_system
        and cast(raw_consents.subject_source_id as string) = known_subjects.source_record_id
),
fingerprinted as (
    select
        *,
        to_hex(sha256(to_json_string(struct(
            consent_event_id,
            customer_source_system,
            customer_source_id,
            channel,
            consent_status,
            policy_version,
            capture_source,
            occurred_at
        )))) as payload_fingerprint
    from normalized
),
immutable_history as (
    select
        *,
        first_value(payload_fingerprint) over (
            partition by consent_event_id
            order by ingested_at is null, ingested_at, orchestration_window_id, payload_fingerprint
            rows between unbounded preceding and unbounded following
        ) as canonical_payload_fingerprint,
        first_value(orchestration_window_id) over (
            partition by consent_event_id
            order by ingested_at is null, ingested_at, orchestration_window_id, payload_fingerprint
            rows between unbounded preceding and unbounded following
        ) as canonical_orchestration_window_id,
        count(distinct payload_fingerprint) over (
            partition by consent_event_id
        ) as immutable_payload_variant_count
    from fingerprinted
),
ranked as (
    select
        *,
        count(distinct payload_fingerprint) over (
            partition by consent_event_id, orchestration_window_id
        ) as payload_variant_count,
        count(*) over (
            partition by consent_event_id, orchestration_window_id, payload_fingerprint
        ) as payload_duplicate_row_count,
        row_number() over (
            partition by consent_event_id, orchestration_window_id, payload_fingerprint
            order by ingested_at desc, payload_fingerprint desc
        ) as row_num
    from immutable_history
),
classified as (
    select
        *,
        case
            when consent_event_id is null or occurred_at is null then 0
            when orchestration_window_id = canonical_orchestration_window_id
                then payload_duplicate_row_count - 1
            when payload_fingerprint = canonical_payload_fingerprint
                then payload_duplicate_row_count
            else payload_duplicate_row_count - 1
        end as explained_duplicate_count
    from ranked
)
select
    case
        when consent_event_id is null or occurred_at is null
            then {{ stable_key(["'marketing'", 'orchestration_window_id', 'payload_fingerprint', 'row_num']) }}
        else {{ stable_key(["'marketing'", 'consent_event_id', 'occurred_at', 'orchestration_window_id', 'payload_fingerprint']) }}
    end as ingestion_key,
    'marketing' as source_system,
    * except (row_num),
    case
        when consent_event_id is null or occurred_at is null then 'INVALID_KEY'
        when payload_variant_count > 1 then 'CONFLICTING_DUPLICATE'
        when orchestration_window_id != canonical_orchestration_window_id
            and payload_fingerprint = canonical_payload_fingerprint
            then 'EXACT_DUPLICATE'
        when payload_fingerprint != canonical_payload_fingerprint then 'EVENT_ID_COLLISION'
        when customer_source_system not in ('salesforce', 'legacy_crm', 'support')
            or customer_source_id is null or not customer_exists
            then 'ORPHAN_SUBJECT'
        when channel not in ('EMAIL', 'SMS', 'PUSH') then 'INVALID_CHANNEL'
        when consent_status not in ('GRANTED', 'REVOKED') then 'INVALID_STATUS'
        else 'VALID'
    end as dq_status
from classified
where row_num = 1 or consent_event_id is null or occurred_at is null
