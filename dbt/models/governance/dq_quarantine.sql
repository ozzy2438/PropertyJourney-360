select
    issue_id as quarantine_id,
    issue_id,
    record_type,
    source_system,
    source_record_id,
    lineage_ingestion_key,
    issue_code,
    severity,
    detected_at as quarantined_at,
    to_json_string(struct(
        record_type,
        source_system,
        source_record_id,
        lineage_ingestion_key,
        issue_code
    )) as quarantine_envelope,
    'AWAITING_REMEDIATION' as quarantine_status
from {{ ref('dq_issue') }}
where issue_type = 'RECORD_VALIDATION'
