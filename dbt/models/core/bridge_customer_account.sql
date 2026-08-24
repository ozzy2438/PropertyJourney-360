select
    membership.customer_id,
    accounts.account_id,
    identities.source_system as relationship_source_system,
    identities.source_record_id as relationship_source_record_id,
    identities.source_updated_at as relationship_updated_at
from {{ ref('int_customer_identity_union') }} identities
inner join {{ ref('int_identity_membership') }} membership using (source_identity_key)
inner join {{ ref('dim_account') }} accounts
    on identities.source_system = accounts.source_system
    and identities.account_source_id = accounts.account_source_id
where identities.account_source_id is not null
qualify row_number() over (
    partition by membership.customer_id, accounts.account_id
    order by identities.source_updated_at desc
) = 1
