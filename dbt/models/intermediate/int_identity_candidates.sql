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
        (a.email_verified or b.email_verified)
            and a.email_normalized is not null
            and a.email_normalized = b.email_normalized as one_verified_email_exact,
        a.email_verified and b.email_verified
            and a.email_normalized is not null
            and b.email_normalized is not null
            and a.email_normalized != b.email_normalized as verified_email_conflict,
        a.phone_normalized is not null
            and a.phone_normalized = b.phone_normalized as phone_exact,
        a.postcode is not null and a.postcode = b.postcode as postcode_exact,
        if(
            a.email_normalized is not null and b.email_normalized is not null,
            1.0 - safe_divide(
                edit_distance(a.email_normalized, b.email_normalized),
                greatest(length(a.email_normalized), length(b.email_normalized))
            ),
            null
        ) as email_similarity,
        if(
            a.name_normalized is not null and b.name_normalized is not null,
            1.0 - safe_divide(
                edit_distance(a.name_normalized, b.name_normalized),
                greatest(length(a.name_normalized), length(b.name_normalized))
            ),
            null
        ) as name_similarity,
        if(
            a.address_line_1 is not null and b.address_line_1 is not null,
            1.0 - safe_divide(
                edit_distance(
                    regexp_replace(lower(trim(a.address_line_1)), r'[^a-z0-9]+', ''),
                    regexp_replace(lower(trim(b.address_line_1)), r'[^a-z0-9]+', '')
                ),
                greatest(
                    length(regexp_replace(lower(trim(a.address_line_1)), r'[^a-z0-9]+', '')),
                    length(regexp_replace(lower(trim(b.address_line_1)), r'[^a-z0-9]+', ''))
                )
            ),
            null
        ) as address_similarity,
        a.phone_normalized is not null and b.phone_normalized is not null as phone_comparable,
        a.postcode is not null and b.postcode is not null as postcode_comparable
    from eligible a
    inner join eligible b
        on a.source_identity_key < b.source_identity_key
        and (
            (a.email_normalized is not null and a.email_normalized = b.email_normalized)
            or (a.phone_normalized is not null and a.phone_normalized = b.phone_normalized)
            or (
                a.postcode is not null
                and a.postcode = b.postcode
                and substr(a.name_normalized, 1, 4) = substr(b.name_normalized, 1, 4)
            )
            or (
                a.postcode is not null
                and a.postcode = b.postcode
                and a.address_line_1 is not null
                and regexp_replace(lower(trim(a.address_line_1)), r'[^a-z0-9]+', '')
                    = regexp_replace(lower(trim(b.address_line_1)), r'[^a-z0-9]+', '')
            )
        )
),
features as (
    select
        *,
        if(email_similarity is not null, 0.35, 0.0)
            + if(phone_comparable, 0.30, 0.0)
            + if(name_similarity is not null, 0.20, 0.0)
            + if(address_similarity is not null, 0.10, 0.0)
            + if(postcode_comparable, 0.05, 0.0) as evidence_coverage,
        coalesce(email_similarity, 0.0) * 0.35
            + if(phone_exact, 0.30, 0.0)
            + coalesce(name_similarity, 0.0) * 0.20
            + coalesce(address_similarity, 0.0) * 0.10
            + if(postcode_exact, 0.05, 0.0) as weighted_similarity
    from pairs
),
scored as (
    select
        *,
        case
            when verified_email_conflict then 0.0
            when verified_email_exact then 0.995
            when one_verified_email_exact and name_similarity >= 0.90 then 0.99
            when email_exact and name_similarity >= 0.90 then 0.985
            when phone_exact and name_similarity >= 0.96 and postcode_exact then 0.975
            when phone_exact and name_similarity >= 0.94 then 0.97
            else coalesce(safe_divide(weighted_similarity, evidence_coverage), 0.0)
                * (0.55 + 0.45 * evidence_coverage)
        end as match_score
    from features
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
            if(verified_email_conflict, 'VERIFIED_EMAIL_CONFLICT', null),
            if(one_verified_email_exact and not verified_email_exact, 'EMAIL_EXACT_ONE_VERIFIED', null),
            if(email_exact and not one_verified_email_exact, 'UNVERIFIED_EMAIL_EXACT', null),
            if(phone_exact, 'AU_PHONE_EXACT', null),
            if(name_similarity >= 0.90, 'NORMALIZED_NAME_STRONG', null),
            if(address_similarity >= 0.90, 'NORMALIZED_ADDRESS_STRONG', null),
            if(postcode_exact, 'POSTCODE_EXACT', null)
        ]) as evidence
        where evidence is not null
    ) as matching_evidence,
    case
        when verified_email_conflict then 'REVIEW_REQUIRED'
        when verified_email_exact
            or (email_exact and name_similarity >= 0.90)
            or (phone_exact and name_similarity >= 0.96 and postcode_exact)
            or (phone_exact and name_similarity >= 0.94)
            then 'AUTO_MATCH'
        when match_score >= {{ var('identity_auto_match_threshold', 0.94) }}
            and evidence_coverage >= {{ var('identity_minimum_automatic_coverage', 0.70) }}
            then 'AUTO_MATCH'
        when match_score >= {{ var('identity_review_threshold', 0.70) }} then 'REVIEW_REQUIRED'
        else 'NO_MATCH'
    end as decision
from scored
