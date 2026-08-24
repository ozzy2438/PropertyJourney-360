select *
from {{ ref('int_identity_decisions') }}
where decision = 'AUTO_MATCH'
  and (
      match_score < {{ var('identity_auto_match_threshold', 0.98) }}
      or array_length(matching_evidence) = 0
  )
