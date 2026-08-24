with decision_summary as (
    select
        identity_key,
        case
            when countif(decision = 'AUTO_MATCH') > 0 then 'AUTO_MATCH'
            when countif(decision = 'REVIEW_REQUIRED') > 0 then 'REVIEW_REQUIRED'
            else 'NO_MATCH'
        end as resolution_decision,
        max(match_score) as highest_match_score
    from {{ ref('int_identity_decisions') }},
    unnest([left_identity_key, right_identity_key]) as identity_key
    where identity_key is not null
    group by identity_key
)
select
    membership.customer_id,
    identities.source_identity_key,
    identities.source_system,
    identities.source_record_id,
    identities.source_updated_at,
    identities.ingestion_key as lineage_ingestion_key,
    coalesce(decision_summary.resolution_decision, 'NO_MATCH') as resolution_decision,
    coalesce(decision_summary.highest_match_score, 0.0) as highest_match_score,
    membership.rule_version
from {{ ref('int_customer_identity_union') }} identities
inner join {{ ref('int_identity_membership') }} membership using (source_identity_key)
left join decision_summary on identities.source_identity_key = decision_summary.identity_key
