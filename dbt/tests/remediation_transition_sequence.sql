with ordered as (
    select
        *,
        row_number() over (
            partition by remediation_case_id order by occurred_at, event_id
        ) as event_number,
        lag(to_status) over (
            partition by remediation_case_id order by occurred_at, event_id
        ) as prior_to_status
    from {{ source('governance_input', 'remediation_event') }}
)
select *
from ordered
where from_status != coalesce(prior_to_status, 'OPEN')
   or (from_status = 'OPEN' and to_status != 'IN_REVIEW')
   or (from_status = 'IN_REVIEW' and to_status != 'RESOLVED')
   or (from_status = 'RESOLVED' and to_status != 'REPLAYED')
   or (from_status = 'REPLAYED' and to_status != 'VERIFIED')
   or (from_status = 'VERIFIED' and to_status != 'CLOSED')
   or to_status = 'RESOLVED' and resolution_action is null
   or to_status = 'REPLAYED' and replay_id is null
   or to_status = 'VERIFIED' and verification_result_id is null
