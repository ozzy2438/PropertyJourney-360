with normalized as (
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
        cast(_window_id as string) as orchestration_window_id
    from {{ source('raw', 'raw_salesforce_accounts') }}
),
fingerprinted as (
    select
        *,
        to_hex(sha256(to_json_string(struct(
            account_source_id,
            account_name,
            abn,
            industry,
            billing_street,
            billing_suburb,
            billing_state,
            billing_postcode,
            source_updated_at
        )))) as payload_fingerprint
    from normalized
),
ranked as (
    select
        *,
        case
            when account_source_id is null or source_updated_at is null then 0
            else count(*) over (
                partition by account_source_id, orchestration_window_id, payload_fingerprint
            ) - 1
        end as explained_duplicate_count,
        count(distinct payload_fingerprint) over (
            partition by account_source_id, orchestration_window_id
        ) as payload_variant_count,
        count(*) over (
            partition by account_source_id, orchestration_window_id, payload_fingerprint
        ) as payload_duplicate_row_count,
        row_number() over (
            partition by account_source_id, orchestration_window_id, payload_fingerprint
            order by source_updated_at desc, ingested_at desc, payload_fingerprint desc
        ) as row_num
    from fingerprinted
)
select
    case
        when account_source_id is null or source_updated_at is null
            then {{ stable_key(["'salesforce_account'", 'orchestration_window_id', 'payload_fingerprint', 'row_num']) }}
        when payload_variant_count > 1
            then {{ stable_key(["'salesforce_account'", 'account_source_id', 'source_updated_at', 'orchestration_window_id', 'payload_fingerprint']) }}
        else {{ ingestion_key("'salesforce'", 'account_source_id', 'source_updated_at') }}
    end as ingestion_key,
    'salesforce' as source_system,
    * except (row_num),
    case
        when account_source_id is null or source_updated_at is null then 'INVALID_KEY'
        when payload_variant_count > 1 then 'CONFLICTING_DUPLICATE'
        when billing_postcode is null and billing_state is not null then 'INVALID_POSTCODE'
        else 'VALID'
    end as dq_status
from ranked
where row_num = 1 or account_source_id is null or source_updated_at is null
