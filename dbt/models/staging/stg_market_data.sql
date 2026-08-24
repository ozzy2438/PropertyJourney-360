with ranked as (
    select
        cast(geography_code as string) as geography_code,
        upper(nullif(trim(cast(geography_type as string)), '')) as geography_type,
        safe_cast(period_start as date) as period_start,
        nullif(trim(cast(property_type as string)), '') as property_type,
        safe_cast(median_price as numeric) as median_price,
        safe_cast(sales_volume as int64) as sales_volume,
        nullif(trim(cast(source_dataset as string)), '') as source_dataset,
        safe_cast(_ingested_at as timestamp) as ingested_at,
        row_number() over (
            partition by cast(geography_code as string), safe_cast(period_start as date), cast(property_type as string)
            order by safe_cast(_ingested_at as timestamp) desc
        ) as row_num
    from {{ source('raw', 'raw_property_market') }}
)
select
    {{ stable_key(['geography_code', 'period_start', 'property_type']) }} as market_record_id,
    * except (row_num),
    case
        when geography_code is null or period_start is null then 'INVALID_KEY'
        when median_price < 0 or sales_volume < 0 then 'INVALID_MEASURE'
        else 'VALID'
    end as dq_status
from ranked
where row_num = 1
