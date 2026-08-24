with ranked as (
    select
        cast(series_id as string) as series_id,
        nullif(trim(cast(series_name as string)), '') as series_name,
        safe_cast(observation_date as date) as observation_date,
        safe_cast(observation_value as numeric) as observation_value,
        nullif(trim(cast(unit as string)), '') as unit,
        safe_cast(_ingested_at as timestamp) as ingested_at,
        row_number() over (
            partition by cast(series_id as string), safe_cast(observation_date as date)
            order by safe_cast(_ingested_at as timestamp) desc
        ) as row_num
    from {{ source('raw', 'raw_rba_indicators') }}
)
select
    {{ stable_key(['series_id', 'observation_date']) }} as indicator_record_id,
    * except (row_num),
    case
        when series_id is null or observation_date is null or observation_value is null then 'INVALID_KEY'
        else 'VALID'
    end as dq_status
from ranked
where row_num = 1
