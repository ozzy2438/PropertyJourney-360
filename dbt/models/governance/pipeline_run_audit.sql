{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='pipeline_run_id',
    partition_by={'field': 'audit_date', 'data_type': 'date', 'granularity': 'day'},
    on_schema_change='fail'
) }}

{% set pipeline_run_id = var('pipeline_run_id', invocation_id) %}

select
    '{{ pipeline_run_id }}' as pipeline_run_id,
    min(reconciled_at) as transformed_at,
    max(reconciled_at) as reconciled_at,
    case
        when countif(reconciliation_status = 'FAIL') = 0 then 'TRANSFORMED_RECONCILED'
        else 'RECONCILIATION_FAILED'
    end as pipeline_status,
    count(*) as reconciliation_check_count,
    countif(reconciliation_status = 'FAIL') as failed_reconciliation_count,
    '{{ target.name }}' as dbt_target,
    current_date('Australia/Melbourne') as audit_date
from {{ ref('reconciliation_result') }}
where pipeline_run_id = '{{ pipeline_run_id }}'
