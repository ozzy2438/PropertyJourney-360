select *
from {{ ref('reconciliation_result') }}
where reconciliation_status != 'PASS'
   or difference_count != 0
   or source_count != accepted_count + quarantined_count + rejected_count + explained_duplicate_count
