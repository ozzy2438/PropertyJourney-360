with candidates as (
    select
        candidate_id,
        left_identity_key,
        right_identity_key,
        left_source_system,
        right_source_system,
        left_source_record_id,
        right_source_record_id,
        match_score,
        rule_version,
        matching_evidence,
        decision
    from {{ ref('int_identity_candidates') }}
),
candidate_members as (
    select left_identity_key as source_identity_key from candidates
    union distinct
    select right_identity_key as source_identity_key from candidates
),
unmatched as (
    select
        {{ stable_key(['i.source_identity_key', "'no-match'", "'" ~ var('identity_rule_version') ~ "'"]) }} as candidate_id,
        i.source_identity_key as left_identity_key,
        cast(null as string) as right_identity_key,
        i.source_system as left_source_system,
        cast(null as string) as right_source_system,
        i.source_record_id as left_source_record_id,
        cast(null as string) as right_source_record_id,
        cast(0.0 as float64) as match_score,
        '{{ var('identity_rule_version') }}' as rule_version,
        cast([] as array<string>) as matching_evidence,
        'NO_MATCH' as decision
    from {{ ref('int_customer_identity_union') }} i
    left join candidate_members c using (source_identity_key)
    where i.dq_status = 'VALID'
      and c.source_identity_key is null
)
select * from candidates
union all
select * from unmatched
