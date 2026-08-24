with identities as (
    select
        source_system,
        source_record_id,
        account_source_id,
        first_name,
        last_name,
        name_normalized,
        email_raw,
        email_normalized,
        email_verified,
        phone_raw,
        phone_normalized,
        address_line_1,
        suburb,
        state,
        postcode,
        source_updated_at,
        ingested_at,
        ingestion_key,
        dq_status
    from {{ ref('stg_salesforce_contacts') }}

    union all

    select
        source_system,
        source_record_id,
        account_source_id,
        first_name,
        last_name,
        name_normalized,
        email_raw,
        email_normalized,
        email_verified,
        phone_raw,
        phone_normalized,
        address_line_1,
        suburb,
        state,
        postcode,
        source_updated_at,
        ingested_at,
        ingestion_key,
        dq_status
    from {{ ref('stg_legacy_customers') }}

    union all

    select
        source_system,
        source_record_id,
        cast(null as string) as account_source_id,
        first_name,
        last_name,
        name_normalized,
        email_raw,
        email_normalized,
        email_verified,
        phone_raw,
        phone_normalized,
        address_line_1,
        suburb,
        state,
        postcode,
        source_updated_at,
        ingested_at,
        ingestion_key,
        dq_status
    from {{ ref('stg_support_customers') }}
)
select
    {{ stable_key(['source_system', 'source_record_id']) }} as source_identity_key,
    *
from identities
