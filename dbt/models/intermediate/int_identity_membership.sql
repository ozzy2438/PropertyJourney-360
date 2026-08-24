{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='source_identity_key',
    on_schema_change='append_new_columns'
) }}

with recursive
all_identities as (
    select source_identity_key, email_normalized, email_verified, dq_status
    from {{ ref('int_customer_identity_union') }}
),
valid_identities as (
    select source_identity_key, email_normalized, email_verified
    from all_identities
    where dq_status = 'VALID'
),
existing_registry as (
    {% if is_incremental() %}
    select source_identity_key, customer_id, registry_preserved
    from {{ this }}
    {% else %}
    select
        cast(null as string) as source_identity_key,
        cast(null as string) as customer_id,
        cast(null as bool) as registry_preserved
    where false
    {% endif %}
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
),
component_safety as (
    select
        components.component_key,
        count(distinct if(identities.email_verified, identities.email_normalized, null))
            as verified_email_count,
        count(distinct registry.customer_id) as existing_customer_count,
        min(registry.customer_id) as existing_customer_id
    from components
    inner join valid_identities identities
        on components.source_identity_key = identities.source_identity_key
    left join existing_registry registry
        on components.source_identity_key = registry.source_identity_key
    group by components.component_key
),
active_membership_preliminary as (
    select
        components.source_identity_key,
        case
            when registry.customer_id is not null then registry.customer_id
            when component_safety.verified_email_count > 1
                or component_safety.existing_customer_count > 1
                then {{ stable_key(["'propertyjourney-customer'", 'components.source_identity_key']) }}
            when component_safety.existing_customer_count = 1
                then component_safety.existing_customer_id
            else {{ stable_key(["'propertyjourney-customer'", 'components.component_key']) }}
        end as customer_id,
        components.component_key,
        components.resolution_hops,
        component_safety.verified_email_count > 1
            or component_safety.existing_customer_count > 1 as cluster_conflict,
        case
            when component_safety.verified_email_count > 1
                or component_safety.existing_customer_count > 1
                then coalesce(registry.registry_preserved, false)
            else true
        end as registry_preserved,
        true as is_active,
        '{{ var('identity_rule_version') }}' as rule_version
    from components
    inner join component_safety using (component_key)
    left join existing_registry registry using (source_identity_key)
),
registry_customer_safety as (
    select
        membership.customer_id,
        count(distinct if(identities.email_verified, identities.email_normalized, null))
            as verified_email_count
    from active_membership_preliminary membership
    inner join valid_identities identities using (source_identity_key)
    group by membership.customer_id
),
active_membership as (
    select
        membership.* except (cluster_conflict),
        membership.cluster_conflict
            or registry_customer_safety.verified_email_count > 1 as cluster_conflict
    from active_membership_preliminary membership
    inner join registry_customer_safety using (customer_id)
),
inactive_membership as (
    select
        identities.source_identity_key,
        coalesce(
            registry.customer_id,
            {{ stable_key(["'propertyjourney-customer'", 'identities.source_identity_key']) }}
        ) as customer_id,
        identities.source_identity_key as component_key,
        0 as resolution_hops,
        false as cluster_conflict,
        coalesce(registry.registry_preserved, false) as registry_preserved,
        false as is_active,
        '{{ var('identity_rule_version') }}' as rule_version
    from all_identities identities
    left join existing_registry registry using (source_identity_key)
    where identities.dq_status != 'VALID'
)
select * from active_membership
union all
select * from inactive_membership
