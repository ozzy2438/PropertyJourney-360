with ranked as (
    select
        cast(geography_code as string) as geography_code,
        upper(nullif(trim(cast(geography_type as string)), '')) as geography_type,
        safe_cast(reference_period as date) as reference_period,
        safe_cast(population as int64) as population,
        safe_cast(median_household_income_weekly as numeric) as median_household_income_weekly,
        safe_cast(owner_occupied_dwellings as int64) as owner_occupied_dwellings,
        safe_cast(rented_dwellings as int64) as rented_dwellings,
        nullif(trim(cast(source_dataset as string)), '') as source_dataset,
        safe_cast(_ingested_at as timestamp) as ingested_at,
        row_number() over (
            partition by cast(geography_code as string), safe_cast(reference_period as date)
            order by safe_cast(_ingested_at as timestamp) desc
        ) as row_num
    from {{ source('raw', 'raw_abs_demographics') }}
)
select
    {{ stable_key(['geography_code', 'reference_period']) }} as demographic_record_id,
    * except (row_num),
    case
        when geography_code is null or reference_period is null then 'INVALID_KEY'
        when population < 0 or median_household_income_weekly < 0 then 'INVALID_MEASURE'
        else 'VALID'
    end as dq_status
from ranked
where row_num = 1
