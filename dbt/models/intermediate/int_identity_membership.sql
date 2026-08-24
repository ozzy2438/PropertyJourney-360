with recursive
valid_identities as (
    select source_identity_key
    from {{ ref('int_customer_identity_union') }}
    where dq_status = 'VALID'
),
auto_edges as (
    select left_identity_key as from_key, right_identity_key as to_key
    from {{ ref('int_identity_decisions') }}
    where decision = 'AUTO_MATCH'

    union all

    select right_identity_key as from_key, left_identity_key as to_key
    from {{ ref('int_identity_decisions') }}
    where decision = 'AUTO_MATCH'
),
walk as (
    select
        source_identity_key as root_key,
        source_identity_key as member_key,
        [source_identity_key] as visited,
        0 as depth
    from valid_identities

    union all

    select
        walk.root_key,
        auto_edges.to_key as member_key,
        array_concat(walk.visited, [auto_edges.to_key]) as visited,
        walk.depth + 1 as depth
    from walk
    inner join auto_edges
        on walk.member_key = auto_edges.from_key
    where auto_edges.to_key not in unnest(walk.visited)
      and walk.depth < 20
),
components as (
    select
        root_key as source_identity_key,
        min(member_key) as component_key,
        max(depth) as resolution_hops
    from walk
    group by root_key
)
select
    components.source_identity_key,
    {{ stable_key(["'propertyjourney-customer'", 'components.component_key']) }} as customer_id,
    components.component_key,
    components.resolution_hops,
    '{{ var('identity_rule_version') }}' as rule_version
from components
