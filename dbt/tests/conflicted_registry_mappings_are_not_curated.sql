select membership.source_identity_key
from {{ ref('int_identity_membership') }} membership
inner join {{ ref('bridge_customer_source_identity') }} bridge using (source_identity_key)
where membership.is_active
  and membership.cluster_conflict
