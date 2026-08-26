# Cost discipline

## Design controls

- Small geographic/public-data slices and synthetic customer counts are the default.
- GCS lifecycle removes temporary dev raw snapshots after the configured retention period; bucket
  versioning/public-access prevention remain enabled.
- BigQuery event/fact tables are date partitioned only when workloads justify it; affected-partition
  MERGE predicates and maximum-bytes-billed limit avoid full scans.
- Cloud Run scales to zero and remains private.
- Pub/Sub retention and dead-letter limits are explicit.
- Local Docker Airflow is the required scheduler. Cloud Composer is not part of the baseline.
- Budgets and alerts detect spend; they do not technically cap billing.

## Cost evidence

| Item | Target/limit | Actual |
|---|---:|---:|
| Monthly dev budget alert | Configurable low threshold | Not deployed/observed |
| BigQuery bytes per representative query | Recorded with query/job ID | Pending cloud run |
| Cloud Run minimum instances | 0 | Pending Terraform apply verification |
| Public raw retention | Environment-configured lifecycle | Pending bucket verification |
| Sustained validation cost | Owner-approved before start | Not authorised |

Approximate calculations are forecasts and must be labelled as such. Actual cost requires billing
export or console evidence bound to the project and period. Expensive temporary resources are torn
down after evidence capture; teardown is itself verified.
