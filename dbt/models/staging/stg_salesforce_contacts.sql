with ranked as (
    select
        cast(id as string) as source_record_id,
        cast(account_id as string) as account_source_id,
        nullif(trim(cast(first_name as string)), '') as first_name,
        nullif(trim(cast(last_name as string)), '') as last_name,
        cast(email as string) as email_raw,
        {{ normalize_email('email') }} as email_normalized,
        coalesce(safe_cast(email_verified as bool), false) as email_verified,
        cast(mobile_phone as string) as phone_raw,
        {{ normalize_au_phone('mobile_phone') }} as phone_normalized,
        nullif(trim(cast(mailing_street as string)), '') as address_line_1,
        nullif(trim(cast(mailing_city as string)), '') as suburb,
        upper(nullif(trim(cast(mailing_state as string)), '')) as state,
        {{ normalize_postcode('mailing_postal_code') }} as postcode,
        safe_cast(system_modstamp as timestamp) as source_updated_at,
        safe_cast(_ingested_at as timestamp) as ingested_at,
        row_number() over (
            partition by cast(id as string)
            order by safe_cast(system_modstamp as timestamp) desc, safe_cast(_ingested_at as timestamp) desc
        ) as row_num
    from {{ source('raw', 'raw_salesforce_contacts') }}
)
select
    {{ ingestion_key("'salesforce'", 'source_record_id', 'source_updated_at') }} as ingestion_key,
    'salesforce' as source_system,
    * except (row_num),
    {{ normalize_name("concat(coalesce(first_name, ''), ' ', coalesce(last_name, ''))") }} as name_normalized,
    case
        when source_record_id is null or source_updated_at is null then 'INVALID_KEY'
        when email_raw is not null and email_normalized is null then 'INVALID_EMAIL'
        when phone_raw is not null and phone_normalized is null then 'INVALID_PHONE'
        when postcode is null and address_line_1 is not null then 'INVALID_POSTCODE'
        when email_normalized is null and phone_normalized is null then 'MISSING_IDENTITY'
        else 'VALID'
    end as dq_status
from ranked
where row_num = 1
