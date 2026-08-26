with normalized as (
    select
        cast(geography_code as string) as geography_code,
        upper(nullif(trim(cast(geography_type as string)), '')) as geography_type,
        safe_cast(reference_period as date) as reference_period,
        safe_cast(population as int64) as population,
        safe_cast(median_household_income_weekly as numeric) as median_household_income_weekly,
        safe_cast(owner_occupied_dwellings as int64) as owner_occupied_dwellings,
        safe_cast(rented_dwellings as int64) as rented_dwellings,
        nullif(trim(cast(source_dataset as string)), '') as source_dataset,
        safe_cast(source_retrieved_at as timestamp) as source_retrieved_at,
        lower(nullif(trim(cast(source_payload_sha256 as string)), '')) as source_payload_sha256,
        nullif(trim(cast(source_payload_object as string)), '') as source_payload_object,
        safe_cast(_ingested_at as timestamp) as ingested_at,
        cast(_window_id as string) as orchestration_window_id
    from {{ source('raw', 'raw_abs_demographics') }}
),
fingerprinted as (
    select
        *,
        to_hex(sha256(to_json_string(struct(
            geography_code,
            geography_type,
            reference_period,
            population,
            median_household_income_weekly,
            owner_occupied_dwellings,
            rented_dwellings,
            source_dataset,
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
            when geography_code is null or safe_cast(reference_period as date) is null then 0
            else count(*) over (
                partition by geography_code, reference_period,
                    orchestration_window_id, payload_fingerprint
            ) - 1
        end as explained_duplicate_count,
        count(distinct payload_fingerprint) over (
            partition by geography_code, reference_period, orchestration_window_id
        ) as payload_variant_count,
        count(*) over (
            partition by geography_code, reference_period,
                orchestration_window_id, payload_fingerprint
        ) as payload_duplicate_row_count,
        row_number() over (
            partition by geography_code, reference_period,
                orchestration_window_id, payload_fingerprint
            order by ingested_at desc, payload_fingerprint desc
        ) as row_num
    from fingerprinted
)
select
    case
        when geography_code is null or reference_period is null
            then {{ stable_key(['geography_code', 'reference_period', 'orchestration_window_id', 'payload_fingerprint', 'row_num']) }}
        when payload_variant_count > 1
            then {{ stable_key(['geography_code', 'reference_period', 'orchestration_window_id', 'payload_fingerprint']) }}
        else {{ stable_key(['geography_code', 'reference_period']) }}
    end as demographic_record_id,
    * except (row_num),
    case
        when geography_code is null or reference_period is null then 'INVALID_KEY'
        when payload_variant_count > 1 then 'CONFLICTING_DUPLICATE'
        when source_retrieved_at is null or source_payload_object is null
            or not regexp_contains(source_payload_sha256, r'^[a-f0-9]{64}$')
            then 'INVALID_PROVENANCE'
        when population is null or median_household_income_weekly is null
            or owner_occupied_dwellings is null or rented_dwellings is null
            or population < 0 or median_household_income_weekly < 0
            or owner_occupied_dwellings < 0 or rented_dwellings < 0
            then 'INVALID_MEASURE'
        else 'VALID'
    end as dq_status
from ranked
where row_num = 1 or geography_code is null or reference_period is null
