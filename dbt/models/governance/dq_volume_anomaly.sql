with history as (
    select
        *,
        avg(source_count) over (
            partition by asset_name
            order by reconciled_at
            rows between 7 preceding and 1 preceding
        ) as prior_run_average,
        count(*) over (
            partition by asset_name
            order by reconciled_at
            rows between 7 preceding and 1 preceding
        ) as prior_run_count
    from {{ ref('reconciliation_result') }}
)
select
    reconciliation_result_id,
    pipeline_run_id,
    asset_name,
    source_count,
    prior_run_average,
    safe_divide(source_count - prior_run_average, prior_run_average) as volume_change_ratio,
    case
        when prior_run_count < 3 then 'INSUFFICIENT_HISTORY'
        when prior_run_average = 0 and source_count = 0 then 'WITHIN_RANGE'
        when prior_run_average = 0 then 'ANOMALY'
        when abs(safe_divide(source_count - prior_run_average, prior_run_average)) > 0.50 then 'ANOMALY'
        else 'WITHIN_RANGE'
    end as volume_status,
    reconciled_at
from history
