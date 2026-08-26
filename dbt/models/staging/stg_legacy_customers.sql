with normalized as (
    select
        cast(customer_id as string) as source_record_id,
        cast(account_id as string) as account_source_id,
        nullif(trim(cast(first_name as string)), '') as first_name,
        nullif(trim(cast(last_name as string)), '') as last_name,
        cast(email as string) as email_raw,
        {{ normalize_email('email') }} as email_normalized,
        coalesce(safe_cast(email_verified as bool), false) as email_verified,
        cast(phone as string) as phone_raw,
        {{ normalize_au_phone('phone') }} as phone_normalized,
        nullif(trim(cast(address_line_1 as string)), '') as address_line_1,
        nullif(trim(cast(suburb as string)), '') as suburb,
        upper(nullif(trim(cast(state as string)), '')) as state,
        {{ normalize_postcode('postcode') }} as postcode,
        safe_cast(updated_at as timestamp) as source_updated_at,
        safe_cast(_ingested_at as timestamp) as ingested_at,
        cast(_window_id as string) as orchestration_window_id
    from {{ source('raw', 'raw_legacy_customers') }}
),
fingerprinted as (
    select
        *,
        to_hex(sha256(to_json_string(struct(
            source_record_id,
            account_source_id,
            first_name,
            last_name,
            email_normalized,
            email_verified,
            phone_normalized,
            address_line_1,
            suburb,
            state,
            postcode,
            source_updated_at
        )))) as payload_fingerprint
    from normalized
),
ranked as (
    select
        *,
        case
            when source_record_id is null or source_updated_at is null then 0
            else count(*) over (
                partition by source_record_id, orchestration_window_id, payload_fingerprint
            ) - 1
        end as explained_duplicate_count,
        count(distinct payload_fingerprint) over (
            partition by source_record_id, orchestration_window_id
        ) as payload_variant_count,
        count(*) over (
            partition by source_record_id, orchestration_window_id, payload_fingerprint
        ) as payload_duplicate_row_count,
        row_number() over (
            partition by source_record_id, orchestration_window_id, payload_fingerprint
            order by source_updated_at desc, ingested_at desc, payload_fingerprint desc
        ) as row_num
    from fingerprinted
)
select
    case
        when source_record_id is null or source_updated_at is null
            then {{ stable_key(["'legacy_crm'", 'orchestration_window_id', 'payload_fingerprint', 'row_num']) }}
        when payload_variant_count > 1
            then {{ stable_key(["'legacy_crm'", 'source_record_id', 'source_updated_at', 'orchestration_window_id', 'payload_fingerprint']) }}
        else {{ ingestion_key("'legacy_crm'", 'source_record_id', 'source_updated_at') }}
    end as ingestion_key,
    'legacy_crm' as source_system,
    * except (row_num),
    {{ normalize_name("concat(coalesce(first_name, ''), ' ', coalesce(last_name, ''))") }} as name_normalized,
    case
        when source_record_id is null or source_updated_at is null then 'INVALID_KEY'
        when payload_variant_count > 1 then 'CONFLICTING_DUPLICATE'
        when email_raw is not null and email_normalized is null then 'INVALID_EMAIL'
        when phone_raw is not null and phone_normalized is null then 'INVALID_PHONE'
        when postcode is null and address_line_1 is not null then 'INVALID_POSTCODE'
        when email_normalized is null and phone_normalized is null then 'MISSING_IDENTITY'
        else 'VALID'
    end as dq_status
from ranked
where row_num = 1 or source_record_id is null or source_updated_at is null
