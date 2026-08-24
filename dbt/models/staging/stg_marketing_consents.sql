with ranked as (
    select
        cast(consent_event_id as string) as consent_event_id,
        lower(nullif(trim(cast(subject_source_system as string)), '')) as customer_source_system,
        cast(subject_source_id as string) as customer_source_id,
        upper(nullif(trim(cast(channel as string)), '')) as channel,
        upper(nullif(trim(cast(consent_status as string)), '')) as consent_status,
        nullif(trim(cast(policy_version as string)), '') as policy_version,
        nullif(trim(cast(capture_source as string)), '') as capture_source,
        safe_cast(occurred_at as timestamp) as occurred_at,
        safe_cast(_ingested_at as timestamp) as ingested_at,
        row_number() over (
            partition by cast(consent_event_id as string)
            order by safe_cast(_ingested_at as timestamp) desc
        ) as row_num
    from {{ source('raw', 'raw_marketing_consents') }}
)
select
    {{ ingestion_key("'marketing'", 'consent_event_id', 'occurred_at') }} as ingestion_key,
    'marketing' as source_system,
    * except (row_num),
    case
        when consent_event_id is null or occurred_at is null then 'INVALID_KEY'
        when customer_source_system not in ('salesforce', 'legacy_crm', 'support') or customer_source_id is null
            then 'ORPHAN_SUBJECT'
        when channel not in ('EMAIL', 'SMS', 'PUSH') then 'INVALID_CHANNEL'
        when consent_status not in ('GRANTED', 'REVOKED') then 'INVALID_STATUS'
        else 'VALID'
    end as dq_status
from ranked
where row_num = 1
