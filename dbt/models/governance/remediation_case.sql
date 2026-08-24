{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='remediation_case_id',
    partition_by={'field': 'opened_date', 'data_type': 'date', 'granularity': 'month'},
    cluster_by=['status', 'severity'],
    on_schema_change='fail'
) }}

select
    {{ stable_key(["'remediation'", 'issue_id']) }} as remediation_case_id,
    issue_id,
    issue_type,
    issue_code,
    severity,
    source_system,
    source_record_id,
    'OPEN' as status,
    detected_at as opened_at,
    date(detected_at, 'Australia/Melbourne') as opened_date,
    detected_at as updated_at,
    cast(null as string) as resolution_action,
    cast(null as string) as resolved_by,
    cast(null as string) as replay_id,
    cast(null as timestamp) as verified_at,
    cast(null as timestamp) as closed_at
from {{ ref('dq_issue') }}
where severity in ('HIGH', 'MEDIUM')
{% if is_incremental() %}
  and issue_id not in (select issue_id from {{ this }})
{% endif %}
