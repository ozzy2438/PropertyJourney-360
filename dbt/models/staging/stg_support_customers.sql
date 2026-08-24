with normalized as (
    select
        cast(customer_id as string) as source_record_id,
        nullif(trim(cast(customer_first_name as string)), '') as first_name,
        nullif(trim(cast(customer_last_name as string)), '') as last_name,
        cast(customer_email as string) as email_raw,
        {{ normalize_email('customer_email') }} as email_normalized,
        coalesce(safe_cast(customer_email_verified as bool), false) as email_verified,
        cast(customer_phone as string) as phone_raw,
        {{ normalize_au_phone('customer_phone') }} as phone_normalized,
        cast(null as string) as address_line_1,
        cast(null as string) as suburb,
        cast(null as string) as state,
        cast(null as string) as postcode,
        safe_cast(updated_at as timestamp) as source_updated_at,
        safe_cast(_ingested_at as timestamp) as ingested_at,
        cast(_window_id as string) as orchestration_window_id
    from {{ source('raw', 'raw_support_cases') }}
    where customer_id is not null
),
fingerprinted as (
    select
        *,
        to_hex(sha256(to_json_string(struct(
            source_record_id,
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
        when source_updated_at is null
            then {{ stable_key(["'support_customer'", 'source_record_id', 'orchestration_window_id', 'payload_fingerprint', 'row_num']) }}
        when payload_variant_count > 1
            then {{ stable_key(["'support_customer'", 'source_record_id', 'source_updated_at', 'orchestration_window_id', 'payload_fingerprint']) }}
        else {{ ingestion_key("'support'", 'source_record_id', 'source_updated_at') }}
    end as ingestion_key,
    'support' as source_system,
    * except (row_num),
    {{ normalize_name("concat(coalesce(first_name, ''), ' ', coalesce(last_name, ''))") }} as name_normalized,
    case
        when source_record_id is null or source_updated_at is null then 'INVALID_KEY'
        when payload_variant_count > 1 then 'CONFLICTING_DUPLICATE'
        when email_raw is not null and email_normalized is null then 'INVALID_EMAIL'
        when phone_raw is not null and phone_normalized is null then 'INVALID_PHONE'
        when email_normalized is null and phone_normalized is null then 'MISSING_IDENTITY'
        else 'VALID'
    end as dq_status
from ranked
where row_num = 1 or source_updated_at is null
