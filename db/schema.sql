-- Provider Intelligence schema (provider_intel.v1)
PRAGMA foreign_keys = ON;
PRAGMA user_version = 1;

CREATE TABLE IF NOT EXISTS schema_migrations (
  schema_version INTEGER PRIMARY KEY NOT NULL,
  migration_name TEXT NOT NULL,
  schema_checksum TEXT NOT NULL,
  applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS providers (
  provider_id TEXT PRIMARY KEY NOT NULL,
  provider_name TEXT NOT NULL DEFAULT '',
  credentials TEXT NOT NULL DEFAULT '',
  npi TEXT NOT NULL DEFAULT '',
  primary_license_state TEXT NOT NULL DEFAULT '',
  primary_license_type TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT ''
);
DROP INDEX IF EXISTS uq_providers_npi;
CREATE UNIQUE INDEX IF NOT EXISTS uq_providers_npi ON providers(npi) WHERE npi <> '';
CREATE INDEX IF NOT EXISTS idx_providers_name ON providers(provider_name);

CREATE TABLE IF NOT EXISTS practices (
  practice_id TEXT PRIMARY KEY NOT NULL,
  practice_name TEXT NOT NULL DEFAULT '',
  website TEXT NOT NULL DEFAULT '',
  intake_url TEXT NOT NULL DEFAULT '',
  phone TEXT NOT NULL DEFAULT '',
  fax TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_practices_name_website ON practices(practice_name, website);

CREATE TABLE IF NOT EXISTS practice_locations (
  location_id TEXT PRIMARY KEY NOT NULL,
  practice_id TEXT NOT NULL,
  address_1 TEXT NOT NULL DEFAULT '',
  city TEXT NOT NULL DEFAULT '',
  state TEXT NOT NULL DEFAULT '',
  zip TEXT NOT NULL DEFAULT '',
  metro TEXT NOT NULL DEFAULT '',
  phone TEXT NOT NULL DEFAULT '',
  telehealth TEXT NOT NULL DEFAULT 'unknown',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (practice_id) REFERENCES practices(practice_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_practice_locations_practice ON practice_locations(practice_id);
CREATE INDEX IF NOT EXISTS idx_practice_locations_state ON practice_locations(state);
CREATE UNIQUE INDEX IF NOT EXISTS uq_practice_location_identity
  ON practice_locations(practice_id, city, state, COALESCE(NULLIF(phone, ''), '<blank>'));

CREATE TABLE IF NOT EXISTS licenses (
  license_id TEXT PRIMARY KEY NOT NULL,
  provider_id TEXT NOT NULL,
  license_state TEXT NOT NULL DEFAULT '',
  license_type TEXT NOT NULL DEFAULT '',
  license_number TEXT NOT NULL DEFAULT '',
  license_status TEXT NOT NULL DEFAULT 'unknown',
  source_url TEXT NOT NULL DEFAULT '',
  retrieved_at TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (provider_id) REFERENCES providers(provider_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_licenses_provider ON licenses(provider_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_license_identity
  ON licenses(provider_id, license_state, license_type, COALESCE(NULLIF(license_number, ''), '<blank>'));

CREATE TABLE IF NOT EXISTS provider_practice_records (
  record_id TEXT PRIMARY KEY NOT NULL,
  provider_id TEXT NOT NULL,
  practice_id TEXT NOT NULL,
  location_id TEXT NOT NULL,
  provider_name_snapshot TEXT NOT NULL DEFAULT '',
  practice_name_snapshot TEXT NOT NULL DEFAULT '',
  npi TEXT NOT NULL DEFAULT '',
  license_state TEXT NOT NULL DEFAULT '',
  license_type TEXT NOT NULL DEFAULT '',
  license_status TEXT NOT NULL DEFAULT 'unknown',
  diagnoses_asd TEXT NOT NULL DEFAULT 'unclear',
  diagnoses_adhd TEXT NOT NULL DEFAULT 'unclear',
  prescriptive_authority TEXT NOT NULL DEFAULT 'unknown',
  prescriptive_basis TEXT NOT NULL DEFAULT '',
  age_groups_json TEXT NOT NULL DEFAULT '[]',
  telehealth TEXT NOT NULL DEFAULT 'unknown',
  insurance_notes TEXT NOT NULL DEFAULT '',
  waitlist_notes TEXT NOT NULL DEFAULT '',
  referral_requirements TEXT NOT NULL DEFAULT '',
  source_urls_json TEXT NOT NULL DEFAULT '[]',
  field_confidence_json TEXT NOT NULL DEFAULT '{}',
  record_confidence REAL NOT NULL DEFAULT 0.0,
  outreach_fit_score REAL NOT NULL DEFAULT 0.0,
  outreach_ready INTEGER NOT NULL DEFAULT 0,
  outreach_reasons_json TEXT NOT NULL DEFAULT '[]',
  conflict_note TEXT NOT NULL DEFAULT '',
  review_status TEXT NOT NULL DEFAULT 'pending',
  export_status TEXT NOT NULL DEFAULT 'pending',
  blocked_reason TEXT NOT NULL DEFAULT '',
  last_verified_at TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (provider_id) REFERENCES providers(provider_id) ON DELETE CASCADE,
  FOREIGN KEY (practice_id) REFERENCES practices(practice_id) ON DELETE CASCADE,
  FOREIGN KEY (location_id) REFERENCES practice_locations(location_id) ON DELETE CASCADE,
  CHECK (record_confidence >= 0 AND record_confidence <= 1),
  CHECK (outreach_fit_score >= 0 AND outreach_fit_score <= 1),
  CHECK (outreach_ready IN (0, 1))
);
CREATE INDEX IF NOT EXISTS idx_provider_practice_records_provider ON provider_practice_records(provider_id);
CREATE INDEX IF NOT EXISTS idx_provider_practice_records_practice ON provider_practice_records(practice_id);
CREATE INDEX IF NOT EXISTS idx_provider_practice_records_review ON provider_practice_records(review_status, export_status);
CREATE INDEX IF NOT EXISTS idx_provider_practice_records_outreach ON provider_practice_records(outreach_ready, outreach_fit_score);
CREATE UNIQUE INDEX IF NOT EXISTS uq_provider_practice_identity
  ON provider_practice_records(provider_id, practice_id, location_id);

CREATE TABLE IF NOT EXISTS source_documents (
  source_document_id TEXT PRIMARY KEY NOT NULL,
  crawl_job_pk TEXT NOT NULL DEFAULT '',
  source_url TEXT NOT NULL DEFAULT '',
  normalized_url TEXT NOT NULL DEFAULT '',
  source_tier TEXT NOT NULL DEFAULT '',
  source_type TEXT NOT NULL DEFAULT '',
  extraction_profile TEXT NOT NULL DEFAULT '',
  status_code INTEGER NOT NULL DEFAULT 0,
  content_hash TEXT NOT NULL DEFAULT '',
  content TEXT NOT NULL DEFAULT '',
  snapshot_path TEXT NOT NULL DEFAULT '',
  fetched_at TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_source_documents_url_hash
  ON source_documents(normalized_url, content_hash);
CREATE INDEX IF NOT EXISTS idx_source_documents_tier ON source_documents(source_tier, source_type);

CREATE TABLE IF NOT EXISTS extracted_records (
  extracted_id TEXT PRIMARY KEY NOT NULL,
  source_document_id TEXT NOT NULL,
  source_url TEXT NOT NULL DEFAULT '',
  source_tier TEXT NOT NULL DEFAULT '',
  source_type TEXT NOT NULL DEFAULT '',
  extraction_profile TEXT NOT NULL DEFAULT '',
  provider_name TEXT NOT NULL DEFAULT '',
  credentials TEXT NOT NULL DEFAULT '',
  npi TEXT NOT NULL DEFAULT '',
  practice_name TEXT NOT NULL DEFAULT '',
  intake_url TEXT NOT NULL DEFAULT '',
  phone TEXT NOT NULL DEFAULT '',
  fax TEXT NOT NULL DEFAULT '',
  address_1 TEXT NOT NULL DEFAULT '',
  city TEXT NOT NULL DEFAULT '',
  state TEXT NOT NULL DEFAULT '',
  zip TEXT NOT NULL DEFAULT '',
  metro TEXT NOT NULL DEFAULT '',
  license_state TEXT NOT NULL DEFAULT '',
  license_type TEXT NOT NULL DEFAULT '',
  license_status TEXT NOT NULL DEFAULT 'unknown',
  diagnoses_asd TEXT NOT NULL DEFAULT 'unclear',
  diagnoses_adhd TEXT NOT NULL DEFAULT 'unclear',
  age_groups_json TEXT NOT NULL DEFAULT '[]',
  telehealth TEXT NOT NULL DEFAULT 'unknown',
  insurance_notes TEXT NOT NULL DEFAULT '',
  waitlist_notes TEXT NOT NULL DEFAULT '',
  referral_requirements TEXT NOT NULL DEFAULT '',
  evidence_json TEXT NOT NULL DEFAULT '[]',
  created_at TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (source_document_id) REFERENCES source_documents(source_document_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_extracted_records_source ON extracted_records(source_document_id);

CREATE TABLE IF NOT EXISTS field_evidence (
  evidence_id TEXT PRIMARY KEY NOT NULL,
  record_id TEXT NOT NULL,
  field_name TEXT NOT NULL DEFAULT '',
  field_value TEXT NOT NULL DEFAULT '',
  quote TEXT NOT NULL DEFAULT '',
  source_url TEXT NOT NULL DEFAULT '',
  source_document_id TEXT NOT NULL DEFAULT '',
  source_tier TEXT NOT NULL DEFAULT '',
  captured_at TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (record_id) REFERENCES provider_practice_records(record_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_field_evidence_record ON field_evidence(record_id, field_name);
CREATE UNIQUE INDEX IF NOT EXISTS uq_field_evidence_identity
  ON field_evidence(record_id, field_name, COALESCE(NULLIF(field_value, ''), '<blank>'), source_url);

CREATE TABLE IF NOT EXISTS contradictions (
  contradiction_id TEXT PRIMARY KEY NOT NULL,
  record_id TEXT NOT NULL,
  field_name TEXT NOT NULL DEFAULT '',
  preferred_value TEXT NOT NULL DEFAULT '',
  conflicting_value TEXT NOT NULL DEFAULT '',
  preferred_source_url TEXT NOT NULL DEFAULT '',
  conflicting_source_url TEXT NOT NULL DEFAULT '',
  note TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (record_id) REFERENCES provider_practice_records(record_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_contradictions_record ON contradictions(record_id, field_name);

CREATE TABLE IF NOT EXISTS review_queue (
  review_id TEXT PRIMARY KEY NOT NULL,
  record_id TEXT NOT NULL DEFAULT '',
  review_type TEXT NOT NULL DEFAULT '',
  provider_name TEXT NOT NULL DEFAULT '',
  practice_name TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL DEFAULT '',
  source_url TEXT NOT NULL DEFAULT '',
  evidence_quote TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'pending',
  created_at TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_review_queue_status ON review_queue(status, review_type);

CREATE TABLE IF NOT EXISTS prescriber_rules (
  rule_id TEXT PRIMARY KEY NOT NULL,
  schema_name TEXT NOT NULL DEFAULT 'prescriber_rules.v1',
  state TEXT NOT NULL DEFAULT '',
  credential TEXT NOT NULL DEFAULT '',
  license_type TEXT NOT NULL DEFAULT '',
  authority TEXT NOT NULL DEFAULT 'unknown',
  limitations TEXT NOT NULL DEFAULT '',
  rationale TEXT NOT NULL DEFAULT '',
  citation_title TEXT NOT NULL DEFAULT '',
  citation_url TEXT NOT NULL DEFAULT '',
  retrieved_at TEXT NOT NULL DEFAULT '',
  active INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_prescriber_rules_lookup ON prescriber_rules(state, credential, license_type, active);

CREATE TABLE IF NOT EXISTS crawl_jobs (
  crawl_job_pk TEXT PRIMARY KEY NOT NULL,
  seed_name TEXT NOT NULL DEFAULT '',
  seed_domain TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'created',
  mode TEXT NOT NULL DEFAULT 'seed',
  last_status_code INTEGER NOT NULL DEFAULT 0,
  started_at TEXT NOT NULL DEFAULT '',
  completed_at TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_crawl_jobs_seed_domain ON crawl_jobs(seed_domain);
CREATE INDEX IF NOT EXISTS idx_crawl_jobs_status ON crawl_jobs(status);

CREATE TABLE IF NOT EXISTS seed_telemetry (
  seed_domain TEXT PRIMARY KEY NOT NULL,
  seed_name TEXT NOT NULL DEFAULT '',
  attempts INTEGER NOT NULL DEFAULT 0,
  successes INTEGER NOT NULL DEFAULT 0,
  failures INTEGER NOT NULL DEFAULT 0,
  success_runs INTEGER NOT NULL DEFAULT 0,
  failure_runs INTEGER NOT NULL DEFAULT 0,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  last_status_code INTEGER NOT NULL DEFAULT 0,
  last_success_at TEXT NOT NULL DEFAULT '',
  last_failure_at TEXT NOT NULL DEFAULT '',
  last_run_started_at TEXT NOT NULL DEFAULT '',
  last_run_completed_at TEXT NOT NULL DEFAULT '',
  last_run_status TEXT NOT NULL DEFAULT 'unknown',
  last_run_pages_fetched INTEGER NOT NULL DEFAULT 0,
  last_run_success_pages INTEGER NOT NULL DEFAULT 0,
  last_run_failure_pages INTEGER NOT NULL DEFAULT 0,
  last_run_job_pk TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_seed_telemetry_failures
  ON seed_telemetry(consecutive_failures, last_failure_at);

CREATE TABLE IF NOT EXISTS crawl_results (
  crawl_result_pk TEXT PRIMARY KEY NOT NULL,
  crawl_job_pk TEXT NOT NULL,
  requested_url TEXT NOT NULL DEFAULT '',
  target_url TEXT NOT NULL DEFAULT '',
  status_code INTEGER NOT NULL DEFAULT 0,
  content_hash TEXT NOT NULL DEFAULT '',
  content TEXT NOT NULL DEFAULT '',
  fetched_at TEXT NOT NULL DEFAULT '',
  error_message TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (crawl_job_pk) REFERENCES crawl_jobs(crawl_job_pk) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_crawl_result_lookup
  ON crawl_results(target_url, content_hash);
CREATE INDEX IF NOT EXISTS idx_crawl_results_job ON crawl_results(crawl_job_pk);
