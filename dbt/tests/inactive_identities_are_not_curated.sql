select identities.source_identity_key
from {{ ref('int_customer_identity_union') }} identities
inner join {{ ref('int_identity_membership') }} membership using (source_identity_key)
left join {{ ref('bridge_customer_source_identity') }} bridge using (source_identity_key)
where identities.dq_status != 'VALID'
  and (membership.is_active or bridge.source_identity_key is not null)
