with normalized as (
    select
        cast(series_id as string) as series_id,
        nullif(trim(cast(series_name as string)), '') as series_name,
        safe_cast(observation_date as date) as observation_date,
        safe_cast(observation_value as numeric) as observation_value,
        nullif(trim(cast(unit as string)), '') as unit,
        safe_cast(source_retrieved_at as timestamp) as source_retrieved_at,
        lower(nullif(trim(cast(source_payload_sha256 as string)), '')) as source_payload_sha256,
        nullif(trim(cast(source_payload_object as string)), '') as source_payload_object,
        safe_cast(_ingested_at as timestamp) as ingested_at,
        cast(_window_id as string) as orchestration_window_id
    from {{ source('raw', 'raw_rba_indicators') }}
),
fingerprinted as (
    select
        *,
        to_hex(sha256(to_json_string(struct(
            series_id,
            series_name,
            observation_date,
            observation_value,
            unit,
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
            when series_id is null or safe_cast(observation_date as date) is null
                or safe_cast(observation_value as numeric) is null then 0
            else count(*) over (
                partition by series_id, observation_date,
                    orchestration_window_id, payload_fingerprint
            ) - 1
        end as explained_duplicate_count,
        count(distinct payload_fingerprint) over (
            partition by series_id, observation_date, orchestration_window_id
        ) as payload_variant_count,
        count(*) over (
            partition by series_id, observation_date,
                orchestration_window_id, payload_fingerprint
        ) as payload_duplicate_row_count,
        row_number() over (
            partition by series_id, observation_date,
                orchestration_window_id, payload_fingerprint
            order by ingested_at desc, payload_fingerprint desc
        ) as row_num
    from fingerprinted
)
select
    case
        when series_id is null or observation_date is null or observation_value is null
            then {{ stable_key(['series_id', 'observation_date', 'orchestration_window_id', 'payload_fingerprint', 'row_num']) }}
        when payload_variant_count > 1
            then {{ stable_key(['series_id', 'observation_date', 'orchestration_window_id', 'payload_fingerprint']) }}
        else {{ stable_key(['series_id', 'observation_date']) }}
    end as indicator_record_id,
    * except (row_num),
    case
        when series_id is null or observation_date is null or observation_value is null then 'INVALID_KEY'
        when payload_variant_count > 1 then 'CONFLICTING_DUPLICATE'
        when source_retrieved_at is null or source_payload_object is null
            or not regexp_contains(source_payload_sha256, r'^[a-f0-9]{64}$')
            then 'INVALID_PROVENANCE'
        else 'VALID'
    end as dq_status
from ranked
where row_num = 1 or series_id is null or observation_date is null or observation_value is null
