with ranked as (
    select
        cast(id as string) as case_source_id,
        cast(contact_id as string) as customer_source_id,
        cast(account_id as string) as account_source_id,
        upper(nullif(trim(cast(status as string)), '')) as case_status,
        upper(nullif(trim(cast(priority as string)), '')) as priority,
        nullif(trim(cast(subject as string)), '') as subject,
        safe_cast(created_date as timestamp) as opened_at,
        safe_cast(closed_date as timestamp) as closed_at,
        safe_cast(system_modstamp as timestamp) as source_updated_at,
        safe_cast(_ingested_at as timestamp) as ingested_at,
        row_number() over (
            partition by cast(id as string)
            order by safe_cast(system_modstamp as timestamp) desc, safe_cast(_ingested_at as timestamp) desc
        ) as row_num
    from {{ source('raw', 'raw_salesforce_cases') }}
)
select
    {{ ingestion_key("'salesforce'", 'case_source_id', 'source_updated_at') }} as ingestion_key,
    'salesforce' as source_system,
    'salesforce' as customer_source_system,
    * except (row_num),
    case
        when case_source_id is null or source_updated_at is null then 'INVALID_KEY'
        when opened_at is null then 'INVALID_TIMESTAMP'
        when customer_source_id is null and account_source_id is null then 'ORPHAN_SUBJECT'
        else 'VALID'
    end as dq_status
from ranked
where row_num = 1
