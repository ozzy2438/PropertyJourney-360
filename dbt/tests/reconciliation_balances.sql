{% set pipeline_run_id = var('pipeline_run_id', '') %}

select *
from {{ ref('reconciliation_result') }}
where ('{{ pipeline_run_id }}' = '' or pipeline_run_id = '{{ pipeline_run_id }}')
  and (
      reconciliation_status != 'PASS'
      or difference_count != 0
      or source_count != accepted_count + quarantined_count + rejected_count + explained_duplicate_count
  )
