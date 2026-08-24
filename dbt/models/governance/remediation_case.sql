{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='remediation_case_id',
    partition_by={'field': 'opened_date', 'data_type': 'date', 'granularity': 'month'},
    cluster_by=['status', 'severity'],
    on_schema_change='fail'
) }}

with base_cases as (
    select
        {{ stable_key(["'remediation'", 'issue_id']) }} as remediation_case_id,
        issue_id,
        issue_type,
        issue_code,
        severity,
        source_system,
        source_record_id,
        detected_at as opened_at
    from {{ ref('dq_issue') }}
    where severity in ('HIGH', 'MEDIUM')
),
event_summary as (
    select
        remediation_case_id,
        array_agg(
            struct(to_status as status, actor, occurred_at)
            order by occurred_at desc, event_id desc limit 1
        )[offset(0)] as latest,
        array_agg(if(to_status = 'RESOLVED', resolution_action, null) ignore nulls
            order by occurred_at desc limit 1)[safe_offset(0)] as resolution_action,
        array_agg(if(to_status = 'RESOLVED', actor, null) ignore nulls
            order by occurred_at desc limit 1)[safe_offset(0)] as resolved_by,
        array_agg(replay_id ignore nulls order by occurred_at desc limit 1)[safe_offset(0)] as replay_id,
        max(if(to_status = 'VERIFIED', occurred_at, null)) as verified_at,
        max(if(to_status = 'CLOSED', occurred_at, null)) as closed_at
    from {{ source('governance_input', 'remediation_event') }}
    group by remediation_case_id
)
select
    base.remediation_case_id,
    base.issue_id,
    base.issue_type,
    base.issue_code,
    base.severity,
    base.source_system,
    base.source_record_id,
    coalesce(events.latest.status, 'OPEN') as status,
    base.opened_at,
    date(base.opened_at, 'Australia/Melbourne') as opened_date,
    coalesce(events.latest.occurred_at, base.opened_at) as updated_at,
    events.resolution_action,
    events.resolved_by,
    events.replay_id,
    events.verified_at,
    events.closed_at
from base_cases base
left join event_summary events using (remediation_case_id)
