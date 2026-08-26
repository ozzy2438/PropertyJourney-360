select issues.issue_id
from {{ ref('dq_issue') }} issues
left join {{ ref('remediation_case') }} cases using (issue_id)
where issues.issue_code in ('CONFLICTING_DUPLICATE', 'EVENT_ID_COLLISION')
  and cases.issue_id is null
