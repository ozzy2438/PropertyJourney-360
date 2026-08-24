with consent_ranked as (
    select
        *,
        row_number() over (
            partition by customer_id, channel
            order by occurred_at desc, consent_change_id desc
        ) as row_num
    from {{ ref('fact_consent_change') }}
),
consent_current as (
    select
        customer_id,
        max(if(channel = 'EMAIL', consent_status, null)) as email_consent_status,
        max(if(channel = 'SMS', consent_status, null)) as sms_consent_status,
        max(if(channel = 'PUSH', consent_status, null)) as push_consent_status,
        max(occurred_at) as consent_updated_at
    from consent_ranked
    where row_num = 1
    group by customer_id
),
accounts as (
    select
        customer_id,
        array_agg(account_id order by account_id) as account_ids
    from {{ ref('bridge_customer_account') }}
    group by customer_id
)
select
    customers.customer_id,
    customers.first_name,
    customers.last_name,
    customers.full_name,
    customers.email,
    customers.phone,
    customers.address_line_1,
    customers.suburb,
    customers.state,
    customers.postcode,
    customers.source_systems,
    customers.identity_source_count,
    coalesce(accounts.account_ids, cast([] as array<string>)) as account_ids,
    coalesce(consent.email_consent_status, 'UNKNOWN') as email_consent_status,
    coalesce(consent.sms_consent_status, 'UNKNOWN') as sms_consent_status,
    coalesce(consent.push_consent_status, 'UNKNOWN') as push_consent_status,
    consent.consent_updated_at,
    engagement.interactions_30d,
    engagement.views_30d,
    engagement.saved_properties_30d,
    engagement.enquiries_30d,
    engagement.last_activity_at,
    engagement.open_support_cases,
    engagement.engagement_score,
    (
        coalesce(consent.email_consent_status, 'UNKNOWN') = 'GRANTED'
        or coalesce(consent.sms_consent_status, 'UNKNOWN') = 'GRANTED'
        or coalesce(consent.push_consent_status, 'UNKNOWN') = 'GRANTED'
    ) as personalisation_allowed,
    customers.record_updated_at,
    greatest(
        customers.record_updated_at,
        coalesce(consent.consent_updated_at, customers.record_updated_at),
        coalesce(engagement.last_activity_at, customers.record_updated_at)
    ) as profile_as_of_at
from {{ ref('dim_customer') }} customers
left join consent_current consent using (customer_id)
left join accounts using (customer_id)
left join {{ ref('mart_customer_engagement') }} engagement using (customer_id)
