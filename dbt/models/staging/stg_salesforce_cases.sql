with known_contacts as (
    select distinct identities.source_record_id as contact_id
    from {{ ref('int_customer_identity_union') }} identities
    inner join {{ ref('int_identity_membership') }} membership using (source_identity_key)
    where identities.source_system = 'salesforce'
      and identities.dq_status = 'VALID'
      and membership.is_active
      and (not membership.cluster_conflict or membership.registry_preserved)
),
ranked_accounts as (
    select account_source_id as account_id, dq_status
    from {{ ref('stg_salesforce_accounts') }}
    qualify row_number() over (
        partition by account_source_id
        order by source_updated_at desc, ingested_at desc, ingestion_key desc
    ) = 1
),
known_accounts as (
    select account_id from ranked_accounts where dq_status = 'VALID'
),
normalized as (
    select
        cast(raw_cases.id as string) as case_source_id,
        cast(raw_cases.contact_id as string) as customer_source_id,
        cast(raw_cases.account_id as string) as account_source_id,
        upper(nullif(trim(cast(raw_cases.status as string)), '')) as case_status,
        upper(nullif(trim(cast(raw_cases.priority as string)), '')) as priority,
        nullif(trim(cast(raw_cases.subject as string)), '') as subject,
        safe_cast(raw_cases.created_date as timestamp) as opened_at,
        safe_cast(raw_cases.closed_date as timestamp) as closed_at,
        safe_cast(raw_cases.system_modstamp as timestamp) as source_updated_at,
        safe_cast(raw_cases._ingested_at as timestamp) as ingested_at,
        cast(raw_cases._window_id as string) as orchestration_window_id,
        known_contacts.contact_id is not null as customer_exists,
        known_accounts.account_id is not null as account_exists
    from {{ source('raw', 'raw_salesforce_cases') }} raw_cases
    left join known_contacts
        on cast(raw_cases.contact_id as string) = known_contacts.contact_id
    left join known_accounts
        on cast(raw_cases.account_id as string) = known_accounts.account_id
),
fingerprinted as (
    select
        *,
        to_hex(sha256(to_json_string(struct(
            case_source_id,
            customer_source_id,
            account_source_id,
            case_status,
            priority,
            subject,
            opened_at,
            closed_at,
            source_updated_at
        )))) as payload_fingerprint
    from normalized
),
ranked as (
    select
        *,
        case
            when case_source_id is null or source_updated_at is null then 0
            else count(*) over (
                partition by case_source_id, orchestration_window_id, payload_fingerprint
            ) - 1
        end as explained_duplicate_count,
        count(distinct payload_fingerprint) over (
            partition by case_source_id, orchestration_window_id
        ) as payload_variant_count,
        count(*) over (
            partition by case_source_id, orchestration_window_id, payload_fingerprint
        ) as payload_duplicate_row_count,
        row_number() over (
            partition by case_source_id, orchestration_window_id, payload_fingerprint
            order by source_updated_at desc, ingested_at desc, payload_fingerprint desc
        ) as row_num
    from fingerprinted
)
select
    case
        when case_source_id is null or source_updated_at is null
            then {{ stable_key(["'salesforce_case'", 'orchestration_window_id', 'payload_fingerprint', 'row_num']) }}
        when payload_variant_count > 1
            then {{ stable_key(["'salesforce_case'", 'case_source_id', 'source_updated_at', 'orchestration_window_id', 'payload_fingerprint']) }}
        else {{ ingestion_key("'salesforce'", 'case_source_id', 'source_updated_at') }}
    end as ingestion_key,
    'salesforce' as source_system,
    'salesforce' as customer_source_system,
    * except (row_num),
    case
        when case_source_id is null or source_updated_at is null then 'INVALID_KEY'
        when payload_variant_count > 1 then 'CONFLICTING_DUPLICATE'
        when opened_at is null then 'INVALID_TIMESTAMP'
        when customer_source_id is null and account_source_id is null then 'ORPHAN_SUBJECT'
        when customer_source_id is not null and not customer_exists then 'ORPHAN_SUBJECT'
        when account_source_id is not null and not account_exists then 'ORPHAN_SUBJECT'
        else 'VALID'
    end as dq_status
from ranked
where row_num = 1 or case_source_id is null or source_updated_at is null
