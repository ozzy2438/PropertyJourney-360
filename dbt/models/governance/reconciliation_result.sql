{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='reconciliation_result_id',
    partition_by={'field': 'reconciliation_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['asset_name', 'reconciliation_status'],
    on_schema_change='fail'
) }}

{% set run_window_id = var('orchestration_window_id', '') %}
{% set pipeline_run_id = var('pipeline_run_id', invocation_id) %}

with raw_counts as (
    select 'salesforce_contacts' as asset_name, count(*) as source_count
    from {{ source('raw', 'raw_salesforce_contacts') }}
    {% if run_window_id %} where cast(_window_id as string) = '{{ run_window_id }}' {% endif %}

    union all
    select 'salesforce_accounts', count(*)
    from {{ source('raw', 'raw_salesforce_accounts') }}
    {% if run_window_id %} where cast(_window_id as string) = '{{ run_window_id }}' {% endif %}

    union all
    select 'salesforce_cases', count(*)
    from {{ source('raw', 'raw_salesforce_cases') }}
    {% if run_window_id %} where cast(_window_id as string) = '{{ run_window_id }}' {% endif %}

    union all
    select 'legacy_customers', count(*)
    from {{ source('raw', 'raw_legacy_customers') }}
    {% if run_window_id %} where cast(_window_id as string) = '{{ run_window_id }}' {% endif %}

    union all
    select 'support_cases', count(*)
    from {{ source('raw', 'raw_support_cases') }}
    {% if run_window_id %} where cast(_window_id as string) = '{{ run_window_id }}' {% endif %}

    union all
    select 'property_events', count(*)
    from {{ source('raw', 'raw_property_events') }}
    {% if run_window_id %} where cast(_window_id as string) = '{{ run_window_id }}' {% endif %}

    union all
    select 'marketing_consents', count(*)
    from {{ source('raw', 'raw_marketing_consents') }}
    {% if run_window_id %} where cast(_window_id as string) = '{{ run_window_id }}' {% endif %}

    union all
    select 'property_master', count(*)
    from {{ source('raw', 'raw_property_master') }}
    {% if run_window_id %} where cast(_window_id as string) = '{{ run_window_id }}' {% endif %}

    union all
    select 'property_market', count(*)
    from {{ source('raw', 'raw_property_market') }}
    {% if run_window_id %} where cast(_window_id as string) = '{{ run_window_id }}' {% endif %}

    union all
    select 'abs_demographics', count(*)
    from {{ source('raw', 'raw_abs_demographics') }}
    {% if run_window_id %} where cast(_window_id as string) = '{{ run_window_id }}' {% endif %}

    union all
    select 'rba_indicators', count(*)
    from {{ source('raw', 'raw_rba_indicators') }}
    {% if run_window_id %} where cast(_window_id as string) = '{{ run_window_id }}' {% endif %}
),
staged_counts as (
    select
        'salesforce_contacts' as asset_name,
        countif(dq_status = 'VALID') as accepted_count,
        countif(dq_status != 'VALID') as quarantined_count,
        count(*) as staged_count,
        coalesce(sum(explained_duplicate_count), 0) as explained_duplicate_count
    from {{ ref('stg_salesforce_contacts') }}
    {% if run_window_id %} where orchestration_window_id = '{{ run_window_id }}' {% endif %}

    union all
    select 'salesforce_accounts', countif(dq_status = 'VALID'), countif(dq_status != 'VALID'),
        count(*), coalesce(sum(explained_duplicate_count), 0)
    from {{ ref('stg_salesforce_accounts') }}
    {% if run_window_id %} where orchestration_window_id = '{{ run_window_id }}' {% endif %}

    union all
    select 'salesforce_cases', countif(dq_status = 'VALID'), countif(dq_status != 'VALID'),
        count(*), coalesce(sum(explained_duplicate_count), 0)
    from {{ ref('stg_salesforce_cases') }}
    {% if run_window_id %} where orchestration_window_id = '{{ run_window_id }}' {% endif %}

    union all
    select 'legacy_customers', countif(dq_status = 'VALID'), countif(dq_status != 'VALID'),
        count(*), coalesce(sum(explained_duplicate_count), 0)
    from {{ ref('stg_legacy_customers') }}
    {% if run_window_id %} where orchestration_window_id = '{{ run_window_id }}' {% endif %}

    union all
    select 'support_cases', countif(dq_status = 'VALID'), countif(dq_status != 'VALID'),
        count(*), coalesce(sum(explained_duplicate_count), 0)
    from {{ ref('stg_support_cases') }}
    {% if run_window_id %} where orchestration_window_id = '{{ run_window_id }}' {% endif %}

    union all
    select 'property_events', countif(dq_status = 'VALID'),
        countif(dq_status not in ('VALID', 'EXACT_DUPLICATE')),
        count(*), coalesce(sum(explained_duplicate_count), 0)
    from {{ ref('stg_property_events') }}
    {% if run_window_id %} where orchestration_window_id = '{{ run_window_id }}' {% endif %}

    union all
    select 'marketing_consents', countif(dq_status = 'VALID'),
        countif(dq_status not in ('VALID', 'EXACT_DUPLICATE')),
        count(*), coalesce(sum(explained_duplicate_count), 0)
    from {{ ref('stg_marketing_consents') }}
    {% if run_window_id %} where orchestration_window_id = '{{ run_window_id }}' {% endif %}

    union all
    select 'property_master', countif(dq_status = 'VALID'), countif(dq_status != 'VALID'),
        count(*), coalesce(sum(explained_duplicate_count), 0)
    from {{ ref('stg_property_master') }}
    {% if run_window_id %} where orchestration_window_id = '{{ run_window_id }}' {% endif %}

    union all
    select 'property_market', countif(dq_status = 'VALID'), countif(dq_status != 'VALID'),
        count(*), coalesce(sum(explained_duplicate_count), 0)
    from {{ ref('stg_market_data') }}
    {% if run_window_id %} where orchestration_window_id = '{{ run_window_id }}' {% endif %}

    union all
    select 'abs_demographics', countif(dq_status = 'VALID'), countif(dq_status != 'VALID'),
        count(*), coalesce(sum(explained_duplicate_count), 0)
    from {{ ref('stg_abs_demographics') }}
    {% if run_window_id %} where orchestration_window_id = '{{ run_window_id }}' {% endif %}

    union all
    select 'rba_indicators', countif(dq_status = 'VALID'), countif(dq_status != 'VALID'),
        count(*), coalesce(sum(explained_duplicate_count), 0)
    from {{ ref('stg_rba_indicators') }}
    {% if run_window_id %} where orchestration_window_id = '{{ run_window_id }}' {% endif %}
),
balanced as (
    select
        raw_counts.*,
        staged_counts.accepted_count,
        staged_counts.quarantined_count,
        cast(0 as int64) as rejected_count,
        staged_counts.explained_duplicate_count
    from raw_counts
    join staged_counts using (asset_name)
)
select
    {{ stable_key(["'" ~ pipeline_run_id ~ "'", 'asset_name']) }} as reconciliation_result_id,
    '{{ pipeline_run_id }}' as pipeline_run_id,
    nullif('{{ run_window_id }}', '') as orchestration_window_id,
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
