with recent_interactions as (
    select
        interactions.customer_id,
        interactions.event_type,
        interactions.occurred_at,
        interactions.price_observed,
        properties.suburb,
        properties.property_type
    from {{ ref('fact_customer_property_interaction') }} interactions
    left join {{ ref('dim_property') }} properties using (property_id)
    where interactions.customer_id is not null
      and interactions.event_date >= date_sub(current_date('Australia/Melbourne'), interval 30 day)
),
suburb_counts as (
    select customer_id, suburb, count(*) as interaction_count
    from recent_interactions
    where suburb is not null
    group by customer_id, suburb
),
preferred_suburbs as (
    select
        customer_id,
        array_agg(suburb order by interaction_count desc, suburb limit 5) as preferred_suburbs
    from suburb_counts
    group by customer_id
),
property_type_counts as (
    select customer_id, property_type, count(*) as interaction_count
    from recent_interactions
    where property_type is not null
    group by customer_id, property_type
),
preferred_property_type as (
    select customer_id, property_type
    from property_type_counts
    qualify row_number() over (
        partition by customer_id
        order by interaction_count desc, property_type
    ) = 1
),
observed_prices as (
    select
        customer_id,
        min(price_observed) as observed_price_min,
        max(price_observed) as observed_price_max
    from recent_interactions
    where price_observed is not null
    group by customer_id
)
select
    customer_360.customer_id,
    customer_360.personalisation_allowed as eligible_for_personalisation,
    if(customer_360.personalisation_allowed, engagement.views_7d, 0) as views_7d,
    if(customer_360.personalisation_allowed, engagement.views_30d, 0) as views_30d,
    if(customer_360.personalisation_allowed, engagement.saved_properties_30d, 0) as saved_properties_30d,
    if(customer_360.personalisation_allowed, engagement.enquiries_30d, 0) as enquiries_30d,
    if(
        customer_360.personalisation_allowed,
        coalesce(preferred_suburbs.preferred_suburbs, cast([] as array<string>)),
        cast([] as array<string>)
    ) as preferred_suburbs,
    if(customer_360.personalisation_allowed, preferred_property_type.property_type, null) as preferred_property_type,
    if(customer_360.personalisation_allowed, observed_prices.observed_price_min, null) as observed_price_min,
    if(customer_360.personalisation_allowed, observed_prices.observed_price_max, null) as observed_price_max,
    if(customer_360.personalisation_allowed, engagement.last_activity_at, null) as last_activity_at,
    if(
        customer_360.personalisation_allowed,
        safe_divide(engagement.interactions_30d, greatest(engagement.sessions_30d, 1)),
        0.0
    ) as engagement_frequency,
    if(customer_360.personalisation_allowed, engagement.engagement_score, 0) as engagement_score,
    customer_360.profile_as_of_at as feature_as_of_at
from {{ ref('customer_360_current') }} customer_360
inner join {{ ref('mart_customer_engagement') }} engagement using (customer_id)
left join preferred_suburbs using (customer_id)
left join preferred_property_type using (customer_id)
left join observed_prices using (customer_id)
