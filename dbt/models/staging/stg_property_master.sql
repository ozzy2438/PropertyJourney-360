with ranked as (
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
        safe_cast(_ingested_at as timestamp) as ingested_at,
        row_number() over (
            partition by cast(property_id as string)
            order by safe_cast(source_updated_at as timestamp) desc, safe_cast(_ingested_at as timestamp) desc
        ) as row_num
    from {{ source('raw', 'raw_property_master') }}
)
select
    {{ ingestion_key("'vic_property'", 'property_source_id', 'source_updated_at') }} as ingestion_key,
    'vic_property' as source_system,
    * except (row_num),
    case
        when property_source_id is null or source_updated_at is null then 'INVALID_KEY'
        when state != 'VIC' then 'OUT_OF_SCOPE_STATE'
        when postcode is null then 'INVALID_POSTCODE'
        when latitude not between -39.3 and -33.9 or longitude not between 140.9 and 150.1
            then 'INVALID_COORDINATES'
        else 'VALID'
    end as dq_status
from ranked
where row_num = 1
