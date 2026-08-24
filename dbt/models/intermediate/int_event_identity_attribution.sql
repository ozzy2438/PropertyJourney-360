with valid_events as (
    select *
    from {{ ref('stg_property_events') }}
    where dq_status = 'VALID'
),
identity_map as (
    select
        identities.source_system,
        identities.source_record_id,
        membership.customer_id
    from {{ ref('int_customer_identity_union') }} identities
    inner join {{ ref('int_identity_membership') }} membership using (source_identity_key)
),
directly_identified as (
    select
        events.event_id,
        events.anonymous_id,
        events.occurred_at as identified_at,
        coalesce(canonical_customer.customer_id, identity_map.customer_id) as customer_id
    from valid_events events
    left join {{ ref('dim_customer') }} canonical_customer
        on events.customer_id = canonical_customer.customer_id
    left join identity_map
        on events.customer_source_system = identity_map.source_system
        and events.customer_source_id = identity_map.source_record_id
    where events.anonymous_id is not null
      and events.event_type = 'IDENTIFY'
      and coalesce(canonical_customer.customer_id, identity_map.customer_id) is not null
),
anonymous_links as (
    select
        anonymous_id,
        count(distinct customer_id) as linked_customer_count,
        array_agg(
            struct(customer_id, identified_at)
            order by identified_at desc, event_id desc
            limit 1
        )[offset(0)] as latest_identity
    from directly_identified
    group by anonymous_id
),
attributed as (
    select
        events.* except (customer_id),
        coalesce(
            canonical_customer.customer_id,
            direct_identity.customer_id,
            if(anonymous_links.linked_customer_count = 1, anonymous_links.latest_identity.customer_id, null)
        ) as customer_id,
        case
            when canonical_customer.customer_id is not null then 'DIRECT_CUSTOMER_ID'
            when direct_identity.customer_id is not null then 'DIRECT_SOURCE_ID'
            when anonymous_links.linked_customer_count = 1 then 'ANONYMOUS_IDENTIFY_LINK'
            when anonymous_links.linked_customer_count > 1 then 'AMBIGUOUS_ANONYMOUS_ID'
            else 'UNATTRIBUTED'
        end as attribution_method,
        anonymous_links.latest_identity.identified_at as anonymous_identity_observed_at
    from valid_events events
    left join {{ ref('dim_customer') }} canonical_customer
        on events.customer_id = canonical_customer.customer_id
    left join identity_map direct_identity
        on events.customer_source_system = direct_identity.source_system
        and events.customer_source_id = direct_identity.source_record_id
    left join anonymous_links using (anonymous_id)
)
select * from attributed
