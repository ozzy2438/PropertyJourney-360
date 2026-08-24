with interactions as (
    select
        customer_id,
        countif(event_date >= date_sub(current_date('Australia/Melbourne'), interval 7 day)) as interactions_7d,
        countif(event_date >= date_sub(current_date('Australia/Melbourne'), interval 30 day)) as interactions_30d,
        countif(event_date >= date_sub(current_date('Australia/Melbourne'), interval 90 day)) as interactions_90d,
        countif(event_type = 'VIEW' and event_date >= date_sub(current_date('Australia/Melbourne'), interval 7 day)) as views_7d,
        countif(event_type = 'VIEW' and event_date >= date_sub(current_date('Australia/Melbourne'), interval 30 day)) as views_30d,
        countif(event_type = 'SAVE' and event_date >= date_sub(current_date('Australia/Melbourne'), interval 30 day)) as saved_properties_30d,
        countif(event_type = 'ENQUIRY' and event_date >= date_sub(current_date('Australia/Melbourne'), interval 30 day)) as enquiries_30d,
        count(distinct if(event_date >= date_sub(current_date('Australia/Melbourne'), interval 30 day), session_id, null)) as sessions_30d,
        max(occurred_at) as last_activity_at
    from {{ ref('fact_customer_property_interaction') }}
    where customer_id is not null
    group by customer_id
),
support as (
    select
        customer_id,
        countif(closed_at is null) as open_support_cases,
        countif(opened_date >= date_sub(current_date('Australia/Melbourne'), interval 90 day)) as support_cases_90d
    from {{ ref('fact_support_case') }}
    where customer_id is not null
    group by customer_id
)
select
    customers.customer_id,
    coalesce(interactions.interactions_7d, 0) as interactions_7d,
    coalesce(interactions.interactions_30d, 0) as interactions_30d,
    coalesce(interactions.interactions_90d, 0) as interactions_90d,
    coalesce(interactions.views_7d, 0) as views_7d,
    coalesce(interactions.views_30d, 0) as views_30d,
    coalesce(interactions.saved_properties_30d, 0) as saved_properties_30d,
    coalesce(interactions.enquiries_30d, 0) as enquiries_30d,
    coalesce(interactions.sessions_30d, 0) as sessions_30d,
    interactions.last_activity_at,
    coalesce(support.open_support_cases, 0) as open_support_cases,
    coalesce(support.support_cases_90d, 0) as support_cases_90d,
    cast(
        least(
            100,
            coalesce(interactions.views_30d, 0)
                + (coalesce(interactions.saved_properties_30d, 0) * 4)
                + (coalesce(interactions.enquiries_30d, 0) * 10)
        ) as int64
    ) as engagement_score
from {{ ref('dim_customer') }} customers
left join interactions using (customer_id)
left join support using (customer_id)
