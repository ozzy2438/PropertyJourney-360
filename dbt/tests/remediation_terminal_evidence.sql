select *
from {{ ref('remediation_case') }}
where (status in ('RESOLVED', 'REPLAYED', 'VERIFIED', 'CLOSED') and resolution_action is null)
   or (status in ('REPLAYED', 'VERIFIED', 'CLOSED') and replay_id is null)
   or (status in ('VERIFIED', 'CLOSED') and verified_at is null)
   or (status = 'CLOSED' and closed_at is null)
