with invalid_salesforce_cases as (
    select case_source_id as source_record_id
    from {{ ref('stg_salesforce_cases') }}
    where dq_status = 'VALID'
      and (
          (customer_source_id is null and account_source_id is null)
          or (customer_source_id is not null and not customer_exists)
          or (account_source_id is not null and not account_exists)
      )
),
invalid_consents as (
    select consent_event_id as source_record_id
    from {{ ref('stg_marketing_consents') }}
    where not customer_exists and dq_status = 'VALID'
),
invalid_events as (
    select event_id as source_record_id
    from {{ ref('stg_property_events') }}
    where dq_status = 'VALID'
      and (
          (event_type in ('VIEW', 'SAVE', 'ENQUIRY') and not property_exists)
          or (customer_id is not null and not canonical_customer_exists)
          or ((customer_source_system is null) != (customer_source_id is null))
          or (customer_source_id is not null and not source_subject_exists)
      )
)
select 'salesforce_case' as record_type, source_record_id from invalid_salesforce_cases
union all
select 'marketing_consent', source_record_id from invalid_consents
union all
select 'property_event', source_record_id from invalid_events
