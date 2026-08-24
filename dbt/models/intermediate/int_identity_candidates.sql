with eligible as (
    select *
    from {{ ref('int_customer_identity_union') }}
    where dq_status = 'VALID'
),
pairs as (
    select
        a.source_identity_key as left_identity_key,
        b.source_identity_key as right_identity_key,
        a.source_system as left_source_system,
        b.source_system as right_source_system,
        a.source_record_id as left_source_record_id,
        b.source_record_id as right_source_record_id,
        a.email_normalized is not null
            and a.email_normalized = b.email_normalized as email_exact,
        a.email_verified and b.email_verified
            and a.email_normalized is not null
            and a.email_normalized = b.email_normalized as verified_email_exact,
        a.phone_normalized is not null
            and a.phone_normalized = b.phone_normalized as phone_exact,
        a.name_normalized is not null
            and a.name_normalized = b.name_normalized as name_exact,
        a.postcode is not null
            and a.postcode = b.postcode as postcode_exact
    from eligible a
    inner join eligible b
        on a.source_identity_key < b.source_identity_key
        and (
            (a.email_normalized is not null and a.email_normalized = b.email_normalized)
            or (a.phone_normalized is not null and a.phone_normalized = b.phone_normalized)
            or (
                a.name_normalized is not null
                and a.name_normalized = b.name_normalized
                and a.postcode is not null
                and a.postcode = b.postcode
            )
        )
),
scored as (
    select
        *,
        case
            when verified_email_exact and phone_exact then 1.00
            when verified_email_exact then 0.99
            when phone_exact and name_exact then 0.985
            when phone_exact then 0.97
            when email_exact and name_exact and postcode_exact then 0.90
            when email_exact then 0.82
            when name_exact and postcode_exact then 0.75
            else 0.00
        end as match_score
    from pairs
)
select
    {{ stable_key(['left_identity_key', 'right_identity_key', "'" ~ var('identity_rule_version') ~ "'"]) }} as candidate_id,
    left_identity_key,
    right_identity_key,
    left_source_system,
    right_source_system,
    left_source_record_id,
    right_source_record_id,
    match_score,
    '{{ var('identity_rule_version') }}' as rule_version,
    array(
        select evidence
        from unnest([
            if(verified_email_exact, 'VERIFIED_EMAIL_EXACT', null),
            if(email_exact and not verified_email_exact, 'UNVERIFIED_EMAIL_EXACT', null),
            if(phone_exact, 'AU_PHONE_EXACT', null),
            if(name_exact, 'NORMALIZED_NAME_EXACT', null),
            if(postcode_exact, 'POSTCODE_EXACT', null)
        ]) as evidence
        where evidence is not null
    ) as matching_evidence,
    case
        when match_score >= {{ var('identity_auto_match_threshold', 0.98) }} then 'AUTO_MATCH'
        when match_score >= {{ var('identity_review_threshold', 0.72) }} then 'REVIEW_REQUIRED'
        else 'NO_MATCH'
    end as decision
from scored
