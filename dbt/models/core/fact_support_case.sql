{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='support_case_id',
    partition_by={'field': 'opened_date', 'data_type': 'date', 'granularity': 'month'},
    cluster_by=['customer_id', 'case_status'],
    on_schema_change='fail'
) }}

with cases as (
    select
        ingestion_key, source_system, customer_source_system, case_source_id,
        customer_source_id, account_source_id, case_status, priority, subject,
        opened_at, closed_at, source_updated_at, ingested_at, dq_status
    from {{ ref('stg_salesforce_cases') }} where dq_status = 'VALID'
    union all
    select
        ingestion_key, source_system, customer_source_system, case_source_id,
        customer_source_id, account_source_id, case_status, priority, subject,
        opened_at, closed_at, source_updated_at, ingested_at, dq_status
    from {{ ref('stg_support_cases') }} where dq_status = 'VALID'
),
identity_map as (
    select source_system, source_record_id, customer_id
    from {{ ref('bridge_customer_source_identity') }}
),
account_map as (
    select source_system, account_source_id, account_id
    from {{ ref('dim_account') }}
)
select
    {{ stable_key(['cases.source_system', 'cases.case_source_id']) }} as support_case_id,
    cases.source_system,
    cases.case_source_id,
    identity_map.customer_id,
    account_map.account_id,
    cases.case_status,
    cases.priority,
    cases.subject,
    cases.opened_at,
    date(cases.opened_at, 'Australia/Melbourne') as opened_date,
    cases.closed_at,
    cases.source_updated_at,
    cases.ingestion_key as lineage_ingestion_key
from cases
left join identity_map
    on cases.customer_source_system = identity_map.source_system
    and cases.customer_source_id = identity_map.source_record_id
left join account_map
    on cases.source_system = account_map.source_system
    and cases.account_source_id = account_map.account_source_id
{% if is_incremental() %}
where cases.source_updated_at >= timestamp_sub(
    (select coalesce(max(source_updated_at), timestamp('1970-01-01')) from {{ this }}),
    interval 3 day
)
{% endif %}
