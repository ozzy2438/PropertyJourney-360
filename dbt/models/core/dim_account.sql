select
    {{ stable_key(['source_system', 'account_source_id']) }} as account_id,
    source_system,
    account_source_id,
    account_name,
    abn,
    industry,
    billing_street,
    billing_suburb,
    billing_state,
    billing_postcode,
    source_updated_at,
    ingestion_key as lineage_ingestion_key
from {{ ref('stg_salesforce_accounts') }}
where dq_status = 'VALID'
qualify row_number() over (
    partition by source_system, account_source_id
    order by source_updated_at desc, ingested_at desc, ingestion_key desc
) = 1
