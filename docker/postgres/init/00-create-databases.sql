-- Local-only database bootstrap. Production databases are managed services and use IAM.
SELECT 'CREATE DATABASE propertyjourney360 OWNER pj360'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'propertyjourney360')\gexec

SELECT 'CREATE DATABASE airflow OWNER pj360'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

\connect propertyjourney360

CREATE SCHEMA IF NOT EXISTS legacy AUTHORIZATION pj360;
CREATE SCHEMA IF NOT EXISTS support AUTHORIZATION pj360;
CREATE SCHEMA IF NOT EXISTS marketing AUTHORIZATION pj360;

CREATE TABLE IF NOT EXISTS legacy.customer (
    source_record_id text PRIMARY KEY,
    source_updated_at timestamptz NOT NULL,
    email text,
    phone text,
    first_name text,
    last_name text,
    address_line text,
    suburb text,
    postcode text,
    source_payload jsonb NOT NULL,
    seed_run_id text NOT NULL
);

CREATE TABLE IF NOT EXISTS support.customer (
    source_record_id text PRIMARY KEY,
    source_updated_at timestamptz NOT NULL,
    email text,
    phone text,
    first_name text,
    last_name text,
    address_line text,
    suburb text,
    postcode text,
    source_payload jsonb NOT NULL,
    seed_run_id text NOT NULL
);

-- No foreign key is intentional: the source can contain orphan cases for DQ validation.
CREATE TABLE IF NOT EXISTS support.case_record (
    case_id text PRIMARY KEY,
    customer_source_record_id text NOT NULL,
    opened_at timestamptz NOT NULL,
    status text NOT NULL CHECK (status IN ('NEW', 'IN_PROGRESS', 'CLOSED')),
    subject text NOT NULL,
    description text NOT NULL,
    source_updated_at timestamptz NOT NULL,
    seed_run_id text NOT NULL
);

CREATE TABLE IF NOT EXISTS marketing.consent_history (
    consent_event_id text PRIMARY KEY,
    customer_source_record_id text NOT NULL,
    channel text NOT NULL CHECK (channel IN ('EMAIL', 'SMS')),
    consent_status text NOT NULL CHECK (consent_status IN ('GRANTED', 'WITHDRAWN')),
    effective_at timestamptz NOT NULL,
    source_updated_at timestamptz NOT NULL,
    seed_run_id text NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_legacy_customer_updated
    ON legacy.customer (source_updated_at, source_record_id);
CREATE INDEX IF NOT EXISTS idx_support_customer_updated
    ON support.customer (source_updated_at, source_record_id);
CREATE INDEX IF NOT EXISTS idx_support_case_updated
    ON support.case_record (source_updated_at, case_id);
CREATE INDEX IF NOT EXISTS idx_consent_updated
    ON marketing.consent_history (source_updated_at, consent_event_id);
