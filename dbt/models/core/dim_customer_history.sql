{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='customer_history_sk',
    partition_by={'field': 'valid_from', 'data_type': 'timestamp', 'granularity': 'month'},
    cluster_by=['customer_id', 'is_current'],
    on_schema_change='fail'
) }}

{% if is_incremental() %}

with incoming as (
    select * from {{ ref('dim_customer') }}
),
current_records as (
    select *
    from {{ this }}
    where is_current
),
changed as (
    select
        incoming.*,
        greatest(
            incoming.record_updated_at,
            timestamp_add(current_records.valid_from, interval 1 microsecond)
        ) as effective_at
    from incoming
    inner join current_records using (customer_id)
    where incoming.profile_hash != current_records.profile_hash
),
new_customers as (
    select
        incoming.*,
        incoming.record_updated_at as effective_at
    from incoming
    left join current_records using (customer_id)
    where current_records.customer_id is null
),
removed_customers as (
    select
        current_records.customer_id,
        greatest(
            coalesce(
                safe_cast(nullif('{{ var('orchestration_window_end', '') }}', '') as timestamp),
                current_timestamp()
            ),
            timestamp_add(current_records.valid_from, interval 1 microsecond)
        ) as effective_at
    from current_records
    left join incoming using (customer_id)
    where incoming.customer_id is null
),
closures as (
    select customer_id, effective_at from changed
    union all
    select customer_id, effective_at from removed_customers
),
records_to_close as (
    select
        current_records.customer_history_sk,
        current_records.customer_id,
        current_records.first_name,
        current_records.last_name,
        current_records.full_name,
        current_records.email,
        current_records.phone,
        current_records.address_line_1,
        current_records.suburb,
        current_records.state,
        current_records.postcode,
        current_records.source_systems,
        current_records.identity_source_count,
        current_records.valid_from,
        closures.effective_at as valid_to,
        false as is_current,
        current_records.profile_hash,
        current_records.source_record_updated_at,
        current_timestamp() as dbt_updated_at
    from current_records
    inner join closures using (customer_id)
),
versions_to_insert as (
    select * from changed
    union all
    select * from new_customers
),
new_versions as (
    select
        {{ stable_key(['customer_id', 'effective_at']) }} as customer_history_sk,
        customer_id,
        first_name,
        last_name,
        full_name,
        email,
        phone,
        address_line_1,
        suburb,
        state,
        postcode,
        source_systems,
        identity_source_count,
        effective_at as valid_from,
        cast(null as timestamp) as valid_to,
        true as is_current,
        profile_hash,
        record_updated_at as source_record_updated_at,
        current_timestamp() as dbt_updated_at
    from versions_to_insert
)
select * from records_to_close
union all
select * from new_versions

{% else %}

select
    {{ stable_key(['customer_id', 'record_updated_at']) }} as customer_history_sk,
    customer_id,
    first_name,
    last_name,
    full_name,
    email,
    phone,
    address_line_1,
    suburb,
    state,
    postcode,
    source_systems,
    identity_source_count,
    record_updated_at as valid_from,
    cast(null as timestamp) as valid_to,
    true as is_current,
    profile_hash,
    record_updated_at as source_record_updated_at,
    current_timestamp() as dbt_updated_at
from {{ ref('dim_customer') }}

{% endif %}
