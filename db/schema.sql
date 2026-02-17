-- CannaRadar production schema (v1.5)
PRAGMA foreign_keys = ON;
PRAGMA user_version = 5;

CREATE TABLE IF NOT EXISTS organizations (
  org_pk TEXT PRIMARY KEY NOT NULL,
  legal_name TEXT NOT NULL DEFAULT '',
  dba_name TEXT NOT NULL DEFAULT '',
  state TEXT,
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  last_seen_at TEXT NOT NULL DEFAULT '',
  deleted_at TEXT NOT NULL DEFAULT '',
  CHECK (length(org_pk) > 0)
);

CREATE TABLE IF NOT EXISTS companies (
  company_pk TEXT PRIMARY KEY NOT NULL,
  organization_pk TEXT NOT NULL,
  legal_name TEXT NOT NULL DEFAULT '',
  dba_name TEXT NOT NULL DEFAULT '',
  state TEXT,
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  last_seen_at TEXT NOT NULL DEFAULT '',
  deleted_at TEXT NOT NULL DEFAULT '',
  CHECK (length(company_pk) > 0),
  FOREIGN KEY (organization_pk) REFERENCES organizations(org_pk) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS licenses (
  license_pk TEXT PRIMARY KEY NOT NULL,
  org_pk TEXT NOT NULL,
  state TEXT,
  license_id TEXT,
  license_type TEXT,
  status TEXT NOT NULL DEFAULT 'unknown',
  source_url TEXT NOT NULL DEFAULT '',
  retrieved_at TEXT NOT NULL DEFAULT '',
  fingerprint TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  deleted_at TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (org_pk) REFERENCES organizations(org_pk) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_licenses_org_pk ON licenses(org_pk);
CREATE INDEX IF NOT EXISTS idx_licenses_state ON licenses(state);
CREATE UNIQUE INDEX IF NOT EXISTS uq_licenses_org_pk_state_license_id ON licenses(org_pk, state, COALESCE(NULLIF(license_id, ''), '<manual>'));

CREATE TABLE IF NOT EXISTS locations (
  location_pk TEXT PRIMARY KEY NOT NULL,
  org_pk TEXT NOT NULL,
  canonical_name TEXT NOT NULL DEFAULT '',
  address_1 TEXT NOT NULL DEFAULT '',
  city TEXT,
  state TEXT NOT NULL DEFAULT '',
  zip TEXT,
  website_domain TEXT NOT NULL DEFAULT '',
  phone TEXT,
  fit_score INTEGER DEFAULT 0,
  last_crawled_at TEXT,
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  last_seen_at TEXT NOT NULL DEFAULT '',
  deleted_at TEXT NOT NULL DEFAULT '',
  CHECK (fit_score BETWEEN 0 AND 100),
  CHECK (length(location_pk) > 0),
  FOREIGN KEY (org_pk) REFERENCES organizations(org_pk) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_locations_org_pk ON locations(org_pk);
CREATE INDEX IF NOT EXISTS idx_locations_state ON locations(state);
CREATE INDEX IF NOT EXISTS idx_locations_website_domain ON locations(website_domain);
CREATE UNIQUE INDEX IF NOT EXISTS uq_locations_org_name ON locations(org_pk, canonical_name, state, COALESCE(NULLIF(website_domain, ''), '<no_domain>'));
CREATE UNIQUE INDEX IF NOT EXISTS uq_locations_website_domain_phone ON locations(COALESCE(NULLIF(website_domain, ''), '<no_domain>'), COALESCE(NULLIF(phone, ''), '<no_phone>'), state);
CREATE UNIQUE INDEX IF NOT EXISTS uq_locations_address ON locations(COALESCE(NULLIF(address_1, ''), '<no_address>'), city, state, zip;

CREATE TABLE IF NOT EXISTS domains (
  domain_pk TEXT PRIMARY KEY NOT NULL,
  location_pk TEXT NOT NULL,
  domain TEXT NOT NULL DEFAULT '',
  is_primary INTEGER NOT NULL DEFAULT 1,
  confidence REAL NOT NULL DEFAULT 0.6,
  source_url TEXT NOT NULL DEFAULT '',
  last_seen_at TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  deleted_at TEXT NOT NULL DEFAULT '',
  CHECK (length(domain_pk) > 0),
  FOREIGN KEY (location_pk) REFERENCES locations(location_pk) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_domains_domain ON domains(domain);
CREATE INDEX IF NOT EXISTS idx_domains_location_pk ON domains(location_pk);

CREATE TABLE IF NOT EXISTS contacts (
  contact_pk TEXT PRIMARY KEY NOT NULL,
  location_pk TEXT NOT NULL,
  full_name TEXT NOT NULL DEFAULT '',
  role TEXT NOT NULL DEFAULT '',
  email TEXT NOT NULL DEFAULT '',
  phone TEXT NOT NULL DEFAULT '',
  source_kind TEXT NOT NULL DEFAULT 'unknown',
  confidence REAL NOT NULL DEFAULT 0.5,
  verification_status TEXT NOT NULL DEFAULT 'unverified',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  last_seen_at TEXT NOT NULL DEFAULT '',
  deleted_at TEXT NOT NULL DEFAULT '',
  CHECK (length(contact_pk) > 0),
  CHECK (confidence >= 0 AND confidence <= 1),
  FOREIGN KEY (location_pk) REFERENCES locations(location_pk) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_contacts_location_pk ON contacts(location_pk);
CREATE INDEX IF NOT EXISTS idx_contacts_role ON contacts(role);
CREATE UNIQUE INDEX IF NOT EXISTS uq_contacts_email ON contacts(location_pk, COALESCE(NULLIF(email, ''), '<blank>'));
CREATE UNIQUE INDEX IF NOT EXISTS uq_contacts_phone ON contacts(location_pk, COALESCE(NULLIF(phone, ''), '<blank>'));

CREATE TABLE IF NOT EXISTS contact_points (
  contact_pk TEXT PRIMARY KEY NOT NULL,
  location_pk TEXT NOT NULL,
  type TEXT NOT NULL,
  value TEXT,
  confidence REAL NOT NULL DEFAULT 0.0,
  source_url TEXT NOT NULL DEFAULT '',
  first_seen_at TEXT NOT NULL DEFAULT '',
  last_seen_at TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  deleted_at TEXT NOT NULL DEFAULT '',
  CHECK (length(contact_pk) > 0),
  CHECK (confidence >= 0 AND confidence <= 1),
  FOREIGN KEY (location_pk) REFERENCES locations(location_pk) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_contact_points_location_pk ON contact_points(location_pk);
CREATE INDEX IF NOT EXISTS idx_contact_points_type ON contact_points(type);
CREATE UNIQUE INDEX IF NOT EXISTS uq_contact_points ON contact_points(location_pk, type, COALESCE(NULLIF(value, ''), '<blank>'));

CREATE TABLE IF NOT EXISTS enrichment_sources (
  enrichment_source_pk TEXT PRIMARY KEY NOT NULL,
  source_type TEXT NOT NULL,
  source_name TEXT NOT NULL,
  source_url TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  success INTEGER NOT NULL DEFAULT 0,
  payload_hash TEXT NOT NULL DEFAULT '',
  status_code INTEGER DEFAULT 0,
  error_message TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  deleted_at TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_enrichment_source ON enrichment_sources(source_type, source_name, source_url);
CREATE INDEX IF NOT EXISTS idx_enrichment_sources_fetched_at ON enrichment_sources(fetched_at);

CREATE TABLE IF NOT EXISTS crawl_jobs (
  crawl_job_pk TEXT PRIMARY KEY NOT NULL,
  seed_name TEXT NOT NULL DEFAULT '',
  seed_domain TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'created',
  mode TEXT NOT NULL DEFAULT 'seed',
  last_status_code INTEGER NOT NULL DEFAULT 0,
  started_at TEXT,
  completed_at TEXT,
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  deleted_at TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_crawl_jobs_seed_domain ON crawl_jobs(seed_domain);
CREATE INDEX IF NOT EXISTS idx_crawl_jobs_status ON crawl_jobs(status);

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
  deleted_at TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (crawl_job_pk) REFERENCES crawl_jobs(crawl_job_pk) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_crawl_result_lookup ON crawl_results(target_url, content_hash);
CREATE INDEX IF NOT EXISTS idx_crawl_results_job ON crawl_results(crawl_job_pk);

CREATE TABLE IF NOT EXISTS evidence (
  evidence_pk TEXT PRIMARY KEY NOT NULL,
  entity_type TEXT,
  entity_pk TEXT,
  field_name TEXT NOT NULL DEFAULT '',
  field_value TEXT NOT NULL DEFAULT '',
  source_url TEXT NOT NULL DEFAULT '',
  snippet TEXT,
  captured_at TEXT NOT NULL DEFAULT '',
  deleted_at TEXT NOT NULL DEFAULT '',
  CHECK (length(evidence_pk) > 0)
);

CREATE INDEX IF NOT EXISTS idx_evidence_entity ON evidence(entity_type, entity_pk);
CREATE UNIQUE INDEX IF NOT EXISTS uq_evidence ON evidence(entity_type, entity_pk, field_name, COALESCE(NULLIF(field_value, ''), '<blank>'));

CREATE TABLE IF NOT EXISTS entity_resolutions (
  resolution_pk TEXT PRIMARY KEY NOT NULL,
  canonical_location_pk TEXT NOT NULL,
  candidate_location_pk TEXT NOT NULL,
  resolution_status TEXT NOT NULL DEFAULT 'pending',
  reason TEXT NOT NULL DEFAULT '',
  confidence REAL NOT NULL DEFAULT 0.0,
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  deleted_at TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (canonical_location_pk) REFERENCES locations(location_pk) ON DELETE CASCADE,
  FOREIGN KEY (candidate_location_pk) REFERENCES locations(location_pk) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_entity_resolutions_status ON entity_resolutions(resolution_status);

CREATE TABLE IF NOT EXISTS lead_scores (
  score_pk TEXT PRIMARY KEY NOT NULL,
  location_pk TEXT NOT NULL,
  score_total INTEGER NOT NULL DEFAULT 0,
  tier TEXT NOT NULL DEFAULT 'C',
  run_id TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT '',
  as_of TEXT NOT NULL DEFAULT '',
  deleted_at TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (location_pk) REFERENCES locations(location_pk) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_lead_scores_location ON lead_scores(location_pk);
CREATE INDEX IF NOT EXISTS idx_lead_scores_tier ON lead_scores(tier);
CREATE INDEX IF NOT EXISTS idx_lead_scores_as_of ON lead_scores(as_of);

CREATE TABLE IF NOT EXISTS scoring_features (
  feature_pk TEXT PRIMARY KEY NOT NULL,
  score_pk TEXT NOT NULL,
  feature_name TEXT NOT NULL DEFAULT '',
  feature_value REAL NOT NULL DEFAULT 0.0,
  created_at TEXT NOT NULL DEFAULT '',
  CHECK (feature_value >= -1 AND feature_value <= 1),
  FOREIGN KEY (score_pk) REFERENCES lead_scores(score_pk) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_score_features ON scoring_features(score_pk, feature_name);

CREATE TABLE IF NOT EXISTS outreach_events (
  event_pk TEXT PRIMARY KEY NOT NULL,
  location_pk TEXT NOT NULL,
  channel TEXT,
  outcome TEXT,
  notes TEXT,
  created_at TEXT NOT NULL DEFAULT '',
  created_by TEXT NOT NULL DEFAULT 'manual',
  deleted_at TEXT NOT NULL DEFAULT '',
  CHECK (length(event_pk) > 0),
  FOREIGN KEY (location_pk) REFERENCES locations(location_pk) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_outreach_events_location_pk ON outreach_events(location_pk);

CREATE TABLE IF NOT EXISTS schema_migrations (
  schema_version INTEGER PRIMARY KEY,
  migration_name TEXT NOT NULL,
  schema_checksum TEXT NOT NULL,
  applied_at TEXT NOT NULL
);
