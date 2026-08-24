with ranked as (
    select
        cast(case_id as string) as case_source_id,
        cast(customer_id as string) as customer_source_id,
        upper(nullif(trim(cast(status as string)), '')) as case_status,
        upper(nullif(trim(cast(priority as string)), '')) as priority,
        nullif(trim(cast(subject as string)), '') as subject,
        safe_cast(opened_at as timestamp) as opened_at,
        safe_cast(closed_at as timestamp) as closed_at,
        safe_cast(updated_at as timestamp) as source_updated_at,
        safe_cast(_ingested_at as timestamp) as ingested_at,
        row_number() over (
            partition by cast(case_id as string)
            order by safe_cast(updated_at as timestamp) desc, safe_cast(_ingested_at as timestamp) desc
        ) as row_num
    from {{ source('raw', 'raw_support_cases') }}
)
select
    {{ ingestion_key("'support'", 'case_source_id', 'source_updated_at') }} as ingestion_key,
    'support' as source_system,
    'support' as customer_source_system,
    * except (row_num),
    cast(null as string) as account_source_id,
    case
        when case_source_id is null or source_updated_at is null then 'INVALID_KEY'
        when opened_at is null then 'INVALID_TIMESTAMP'
        when customer_source_id is null then 'ORPHAN_SUBJECT'
        else 'VALID'
    end as dq_status
from ranked
where row_num = 1
