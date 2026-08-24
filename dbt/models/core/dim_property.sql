select
    {{ stable_key(['source_system', 'property_source_id']) }} as property_id,
    source_system,
    property_source_id,
    address_id,
    address_line_1,
    suburb,
    state,
    postcode,
    property_type,
    latitude,
    longitude,
    safe.st_geogfromtext(geometry_wkt) as property_geography,
    source_dataset,
    source_updated_at,
    source_retrieved_at,
    source_payload_sha256,
    source_payload_object,
    ingestion_key as lineage_ingestion_key
from {{ ref('stg_property_master') }}
where dq_status = 'VALID'
qualify row_number() over (
    partition by source_system, property_source_id
    order by source_updated_at desc, ingested_at desc, ingestion_key desc
) = 1
