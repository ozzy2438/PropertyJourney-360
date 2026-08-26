{% set run_window_id = var('orchestration_window_id', '') %}

with event_history as (
    select *
    from {{ ref('stg_property_events') }}
    where dq_status = 'VALID'
    qualify row_number() over (
        partition by event_id order by occurred_at desc, ingested_at desc, ingestion_key desc
    ) = 1
),
identity_map as (
    select
        identities.source_system,
        identities.source_record_id,
        membership.customer_id
    from {{ ref('int_customer_identity_union') }} identities
    inner join {{ ref('int_identity_membership') }} membership using (source_identity_key)
    where identities.dq_status = 'VALID'
      and membership.is_active
      and (not membership.cluster_conflict or membership.registry_preserved)
),
known_customers as (
    select distinct customer_id
    from {{ ref('int_identity_membership') }}
    where is_active and (not cluster_conflict or registry_preserved)
),
directly_identified as (
    select
        events.event_id,
        events.anonymous_id,
        events.session_id,
        events.orchestration_window_id,
        events.occurred_at as identified_at,
        coalesce(known_customers.customer_id, identity_map.customer_id) as customer_id
    from event_history events
    left join known_customers
        on events.customer_id = known_customers.customer_id
    left join identity_map
        on events.customer_source_system = identity_map.source_system
        and events.customer_source_id = identity_map.source_record_id
    where events.anonymous_id is not null
      and events.session_id is not null
      and events.event_type = 'IDENTIFY'
      and coalesce(known_customers.customer_id, identity_map.customer_id) is not null
),
target_events as (
    select events.*
    from event_history events
    {% if run_window_id %}
    where events.orchestration_window_id = '{{ run_window_id }}'
       or exists (
           select 1
           from directly_identified identified
           where identified.orchestration_window_id = '{{ run_window_id }}'
             and events.anonymous_id = identified.anonymous_id
             and events.session_id = identified.session_id
             and events.occurred_at <= identified.identified_at
             and events.occurred_at >= timestamp_sub(
                 identified.identified_at,
                 interval {{ var('anonymous_link_window_hours', 24) }} hour
             )
       )
    {% endif %}
),
anonymous_link_candidates as (
    select
        events.event_id,
        count(distinct identified.customer_id) as linked_customer_count,
        array_agg(
            struct(identified.customer_id, identified.identified_at, identified.event_id)
            order by identified.identified_at desc, identified.event_id desc
            limit 1
        )[offset(0)] as latest_identity
    from target_events events
    inner join directly_identified identified
        on events.anonymous_id = identified.anonymous_id
        and events.session_id = identified.session_id
        and events.occurred_at <= identified.identified_at
        and events.occurred_at >= timestamp_sub(
            identified.identified_at,
            interval {{ var('anonymous_link_window_hours', 24) }} hour
        )
    group by events.event_id
),
attributed as (
    select
        events.* except (customer_id),
        coalesce(
            known_customers.customer_id,
            direct_identity.customer_id,
            if(
                anonymous_links.linked_customer_count = 1,
                anonymous_links.latest_identity.customer_id,
                null
            )
        ) as customer_id,
        case
            when known_customers.customer_id is not null then 'DIRECT_CUSTOMER_ID'
            when direct_identity.customer_id is not null then 'DIRECT_SOURCE_ID'
            when anonymous_links.linked_customer_count = 1 then 'ANONYMOUS_IDENTIFY_LINK'
            when anonymous_links.linked_customer_count > 1 then 'AMBIGUOUS_ANONYMOUS_ID'
            else 'UNATTRIBUTED'
        end as attribution_method,
        anonymous_links.latest_identity.identified_at as anonymous_identity_observed_at
    from target_events events
    left join known_customers
        on events.customer_id = known_customers.customer_id
    left join identity_map direct_identity
        on events.customer_source_system = direct_identity.source_system
        and events.customer_source_id = direct_identity.source_record_id
    left join anonymous_link_candidates anonymous_links using (event_id)
)
select * from attributed
