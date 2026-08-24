{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='interaction_id',
    partition_by={'field': 'event_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['customer_id', 'event_type', 'property_id'],
    on_schema_change='fail'
) }}

select
    events.event_id as interaction_id,
    events.customer_id,
    properties.property_id,
    events.event_type,
    events.session_id,
    events.anonymous_id,
    events.attribution_method,
    events.customer_source_system,
    events.customer_source_id,
    events.price_observed,
    events.schema_version,
    events.occurred_at,
    events.event_date,
    events.ingested_at,
    events.ingestion_key as lineage_ingestion_key
from {{ ref('int_event_identity_attribution') }} events
left join {{ ref('dim_property') }} properties
    on events.property_id = properties.property_source_id
{% if is_incremental() %}
where events.event_date >= date_sub(
    (select coalesce(max(event_date), date('1970-01-01')) from {{ this }}),
    interval {{ var('event_lookback_days', 3) }} day
)
{% endif %}
