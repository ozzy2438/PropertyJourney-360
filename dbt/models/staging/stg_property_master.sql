with normalized as (
    select
        cast(property_id as string) as property_source_id,
        cast(address_id as string) as address_id,
        nullif(trim(cast(address_line_1 as string)), '') as address_line_1,
        nullif(trim(cast(suburb as string)), '') as suburb,
        upper(nullif(trim(cast(state as string)), '')) as state,
        {{ normalize_postcode('postcode') }} as postcode,
        nullif(trim(cast(property_type as string)), '') as property_type,
        safe_cast(latitude as float64) as latitude,
        safe_cast(longitude as float64) as longitude,
        cast(geometry_wkt as string) as geometry_wkt,
        nullif(trim(cast(source_dataset as string)), '') as source_dataset,
        safe_cast(source_updated_at as timestamp) as source_updated_at,
        safe_cast(source_retrieved_at as timestamp) as source_retrieved_at,
        lower(nullif(trim(cast(source_payload_sha256 as string)), '')) as source_payload_sha256,
        nullif(trim(cast(source_payload_object as string)), '') as source_payload_object,
        safe_cast(_ingested_at as timestamp) as ingested_at,
        cast(_window_id as string) as orchestration_window_id
    from {{ source('raw', 'raw_property_master') }}
),
fingerprinted as (
    select
        *,
        to_hex(sha256(to_json_string(struct(
            property_source_id,
            address_id,
            address_line_1,
            suburb,
            state,
            postcode,
            property_type,
            latitude,
            longitude,
            geometry_wkt,
            source_dataset,
            source_updated_at,
            source_retrieved_at,
            source_payload_sha256,
            source_payload_object
        )))) as payload_fingerprint
    from normalized
),
ranked as (
    select
        *,
        case
            when property_source_id is null or source_updated_at is null then 0
            else count(*) over (
                partition by property_source_id, orchestration_window_id, payload_fingerprint
            ) - 1
        end as explained_duplicate_count,
        count(distinct payload_fingerprint) over (
            partition by property_source_id, orchestration_window_id
        ) as payload_variant_count,
        count(*) over (
            partition by property_source_id, orchestration_window_id, payload_fingerprint
        ) as payload_duplicate_row_count,
        row_number() over (
            partition by property_source_id, orchestration_window_id, payload_fingerprint
            order by source_updated_at desc, ingested_at desc, payload_fingerprint desc
        ) as row_num
    from fingerprinted
)
select
    case
        when property_source_id is null or source_updated_at is null
            then {{ stable_key(["'vic_property'", 'orchestration_window_id', 'payload_fingerprint', 'row_num']) }}
        when payload_variant_count > 1
            then {{ stable_key(["'vic_property'", 'property_source_id', 'source_updated_at', 'orchestration_window_id', 'payload_fingerprint']) }}
        else {{ ingestion_key("'vic_property'", 'property_source_id', 'source_updated_at') }}
    end as ingestion_key,
    'vic_property' as source_system,
    * except (row_num),
    case
        when property_source_id is null or source_updated_at is null then 'INVALID_KEY'
        when payload_variant_count > 1 then 'CONFLICTING_DUPLICATE'
        when source_retrieved_at is null or source_payload_object is null
            or not regexp_contains(source_payload_sha256, r'^[a-f0-9]{64}$')
            then 'INVALID_PROVENANCE'
        when state != 'VIC' then 'OUT_OF_SCOPE_STATE'
        when postcode is null then 'INVALID_POSTCODE'
        when latitude not between -39.3 and -33.9 or longitude not between 140.9 and 150.1
            then 'INVALID_COORDINATES'
        else 'VALID'
    end as dq_status
from ranked
where row_num = 1 or property_source_id is null or source_updated_at is null
