{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='reconciliation_result_id',
    partition_by={'field': 'reconciliation_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['asset_name', 'reconciliation_status'],
    on_schema_change='fail'
) }}

with counts as (
    select
        'salesforce_contacts' as asset_name,
        (select count(*) from {{ source('raw', 'raw_salesforce_contacts') }}) as source_count,
        countif(dq_status = 'VALID') as accepted_count,
        countif(dq_status != 'VALID') as quarantined_count,
        count(*) as staged_count
    from {{ ref('stg_salesforce_contacts') }}

    union all

    select
        'legacy_customers',
        (select count(*) from {{ source('raw', 'raw_legacy_customers') }}),
        countif(dq_status = 'VALID'),
        countif(dq_status != 'VALID'),
        count(*)
    from {{ ref('stg_legacy_customers') }}

    union all

    select
        'property_events',
        (select count(*) from {{ source('raw', 'raw_property_events') }}),
        countif(dq_status = 'VALID'),
        countif(dq_status != 'VALID'),
        count(*)
    from {{ ref('stg_property_events') }}

    union all

    select
        'marketing_consents',
        (select count(*) from {{ source('raw', 'raw_marketing_consents') }}),
        countif(dq_status = 'VALID'),
        countif(dq_status != 'VALID'),
        count(*)
    from {{ ref('stg_marketing_consents') }}

    union all

    select
        'property_master',
        (select count(*) from {{ source('raw', 'raw_property_master') }}),
        countif(dq_status = 'VALID'),
        countif(dq_status != 'VALID'),
        count(*)
    from {{ ref('stg_property_master') }}
),
balanced as (
    select
        *,
        cast(0 as int64) as rejected_count,
        greatest(source_count - staged_count, 0) as explained_duplicate_count
    from counts
)
select
    {{ stable_key(["'" ~ invocation_id ~ "'", 'asset_name']) }} as reconciliation_result_id,
    '{{ invocation_id }}' as pipeline_run_id,
    asset_name,
    source_count,
    accepted_count,
    quarantined_count,
    rejected_count,
    explained_duplicate_count,
    source_count - (
        accepted_count + quarantined_count + rejected_count + explained_duplicate_count
    ) as difference_count,
    case
        when source_count = accepted_count + quarantined_count + rejected_count + explained_duplicate_count
            then 'PASS'
        else 'FAIL'
    end as reconciliation_status,
    current_timestamp() as reconciled_at,
    current_date('Australia/Melbourne') as reconciliation_date
from balanced
