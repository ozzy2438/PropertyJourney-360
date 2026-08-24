with ranked as (
    select
        cast(id as string) as account_source_id,
        nullif(trim(cast(name as string)), '') as account_name,
        nullif(regexp_replace(cast(abn as string), r'[^0-9]', ''), '') as abn,
        nullif(trim(cast(industry as string)), '') as industry,
        nullif(trim(cast(billing_street as string)), '') as billing_street,
        nullif(trim(cast(billing_city as string)), '') as billing_suburb,
        upper(nullif(trim(cast(billing_state as string)), '')) as billing_state,
        {{ normalize_postcode('billing_postal_code') }} as billing_postcode,
        safe_cast(system_modstamp as timestamp) as source_updated_at,
        safe_cast(_ingested_at as timestamp) as ingested_at,
        row_number() over (
            partition by cast(id as string)
            order by safe_cast(system_modstamp as timestamp) desc, safe_cast(_ingested_at as timestamp) desc
        ) as row_num
    from {{ source('raw', 'raw_salesforce_accounts') }}
)
select
    {{ ingestion_key("'salesforce'", 'account_source_id', 'source_updated_at') }} as ingestion_key,
    'salesforce' as source_system,
    * except (row_num),
    case
        when account_source_id is null or source_updated_at is null then 'INVALID_KEY'
        when billing_postcode is null and billing_state is not null then 'INVALID_POSTCODE'
        else 'VALID'
    end as dq_status
from ranked
where row_num = 1
