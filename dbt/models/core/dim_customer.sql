with resolved as (
    select
        membership.customer_id,
        identities.* except (source_identity_key)
    from {{ ref('int_customer_identity_union') }} identities
    inner join {{ ref('int_identity_membership') }} membership using (source_identity_key)
),
canonical as (
    select
        customer_id,
        array_agg(first_name ignore nulls order by email_verified desc, source_updated_at desc limit 1)[safe_offset(0)] as first_name,
        array_agg(last_name ignore nulls order by email_verified desc, source_updated_at desc limit 1)[safe_offset(0)] as last_name,
        array_agg(email_normalized ignore nulls order by email_verified desc, source_updated_at desc limit 1)[safe_offset(0)] as email,
        array_agg(phone_normalized ignore nulls order by source_updated_at desc limit 1)[safe_offset(0)] as phone,
        array_agg(address_line_1 ignore nulls order by source_updated_at desc limit 1)[safe_offset(0)] as address_line_1,
        array_agg(suburb ignore nulls order by source_updated_at desc limit 1)[safe_offset(0)] as suburb,
        array_agg(state ignore nulls order by source_updated_at desc limit 1)[safe_offset(0)] as state,
        array_agg(postcode ignore nulls order by source_updated_at desc limit 1)[safe_offset(0)] as postcode,
        array_agg(distinct source_system order by source_system) as source_systems,
        count(*) as identity_source_count,
        min(source_updated_at) as first_observed_at,
        max(source_updated_at) as record_updated_at
    from resolved
    group by customer_id
)
select
    customer_id,
    first_name,
    last_name,
    nullif(trim(concat(coalesce(first_name, ''), ' ', coalesce(last_name, ''))), '') as full_name,
    email,
    phone,
    address_line_1,
    suburb,
    state,
    postcode,
    source_systems,
    identity_source_count,
    first_observed_at,
    record_updated_at,
    {{ stable_key([
        'first_name', 'last_name', 'email', 'phone', 'address_line_1',
        'suburb', 'state', 'postcode'
    ]) }} as profile_hash
from canonical
