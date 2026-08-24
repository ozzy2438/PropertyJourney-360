{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    partition_by={'field': 'event_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['event_type', 'property_id'],
    on_schema_change='fail'
) }}

with source_rows as (
    select
        cast(event_id as string) as event_id,
        upper(nullif(trim(cast(event_type as string)), '')) as event_type,
        cast(session_id as string) as session_id,
        cast(anonymous_id as string) as anonymous_id,
        cast(customer_id as string) as customer_id,
        lower(nullif(trim(cast(customer_source_system as string)), '')) as customer_source_system,
        cast(customer_source_id as string) as customer_source_id,
        cast(property_id as string) as property_id,
        safe_cast(price_observed as numeric) as price_observed,
        safe_cast(schema_version as int64) as schema_version,
        safe_cast(occurred_at as timestamp) as occurred_at,
        date(safe_cast(occurred_at as timestamp), 'Australia/Melbourne') as event_date,
        safe_cast(_ingested_at as timestamp) as ingested_at,
        row_number() over (
            partition by cast(event_id as string)
            order by safe_cast(_ingested_at as timestamp) desc
        ) as row_num
    from {{ source('raw', 'raw_property_events') }}
    {% if is_incremental() %}
    where safe_cast(_ingested_at as timestamp) >= timestamp_sub(
        (select coalesce(max(ingested_at), timestamp('1970-01-01')) from {{ this }}),
        interval {{ var('event_lookback_days', 3) }} day
    )
    {% endif %}
)
select
    {{ ingestion_key("'web_event'", 'event_id', 'occurred_at') }} as ingestion_key,
    'web_event' as source_system,
    * except (row_num),
    case
        when event_id is null then 'INVALID_KEY'
        when occurred_at is null then 'INVALID_TIMESTAMP'
        when event_type not in ('VIEW', 'SAVE', 'ENQUIRY', 'SEARCH', 'IDENTIFY') then 'INVALID_EVENT_TYPE'
        when schema_version not in (1, 2) then 'UNSUPPORTED_SCHEMA'
        when event_type = 'IDENTIFY'
            and customer_id is null
            and (customer_source_system is null or customer_source_id is null)
            then 'MISSING_IDENTIFY_SUBJECT'
        when event_type in ('VIEW', 'SAVE', 'ENQUIRY') and property_id is null then 'ORPHAN_PROPERTY'
        else 'VALID'
    end as dq_status
from source_rows
where row_num = 1
