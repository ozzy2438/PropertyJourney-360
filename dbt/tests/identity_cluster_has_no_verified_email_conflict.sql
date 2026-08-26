select
    membership.customer_id,
    count(distinct identities.email_normalized) as verified_email_count
from {{ ref('int_identity_membership') }} membership
inner join {{ ref('int_customer_identity_union') }} identities using (source_identity_key)
where identities.email_verified
  and identities.email_normalized is not null
group by membership.customer_id
having count(distinct identities.email_normalized) > 1
   and countif(not membership.cluster_conflict) > 0
