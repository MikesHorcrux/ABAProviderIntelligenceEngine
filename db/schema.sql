-- CannaRadar V1 canonical schema
PRAGMA foreign_keys = ON;
PRAGMA user_version = 4;

CREATE TABLE IF NOT EXISTS organizations (
  org_pk TEXT PRIMARY KEY NOT NULL,
  legal_name TEXT NOT NULL DEFAULT '',
  dba_name TEXT NOT NULL DEFAULT '',
  state TEXT,
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT '',
  CHECK (length(org_pk) > 0)
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
  FOREIGN KEY (org_pk) REFERENCES organizations(org_pk) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_licenses_org_pk ON licenses(org_pk);
CREATE INDEX IF NOT EXISTS idx_licenses_state ON licenses(state);
CREATE UNIQUE INDEX IF NOT EXISTS uq_licenses_org_pk_state_license_id ON licenses(org_pk, state, COALESCE(NULLIF(license_id, ''), '<manual>');

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
  CHECK (fit_score BETWEEN 0 AND 100),
  CHECK (length(location_pk) > 0),
  FOREIGN KEY (org_pk) REFERENCES organizations(org_pk) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_locations_org_pk ON locations(org_pk);
CREATE INDEX IF NOT EXISTS idx_locations_state ON locations(state);
CREATE INDEX IF NOT EXISTS idx_locations_website_domain ON locations(website_domain);
CREATE UNIQUE INDEX IF NOT EXISTS uq_locations_org_name ON locations(org_pk, canonical_name, state, COALESCE(NULLIF(website_domain, ''), '<no_domain>');

CREATE TABLE IF NOT EXISTS contact_points (
  contact_pk TEXT PRIMARY KEY NOT NULL,
  location_pk TEXT NOT NULL,
  type TEXT NOT NULL,
  value TEXT,
  confidence REAL NOT NULL DEFAULT 0.0,
  source_url TEXT NOT NULL DEFAULT '',
  first_seen_at TEXT NOT NULL DEFAULT '',
  last_seen_at TEXT NOT NULL DEFAULT '',
  CHECK (length(contact_pk) > 0),
  CHECK (confidence >= 0 AND confidence <= 1),
  FOREIGN KEY (location_pk) REFERENCES locations(location_pk) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_contact_points_location_pk ON contact_points(location_pk);
CREATE INDEX IF NOT EXISTS idx_contact_points_type ON contact_points(type);
CREATE UNIQUE INDEX IF NOT EXISTS uq_contact_points ON contact_points(location_pk, type, COALESCE(NULLIF(value, ''), '<blank>');

CREATE TABLE IF NOT EXISTS evidence (
  evidence_pk TEXT PRIMARY KEY NOT NULL,
  entity_type TEXT,
  entity_pk TEXT,
  field_name TEXT NOT NULL DEFAULT '',
  field_value TEXT NOT NULL DEFAULT '',
  source_url TEXT NOT NULL DEFAULT '',
  snippet TEXT,
  captured_at TEXT NOT NULL DEFAULT '',
  CHECK (length(evidence_pk) > 0)
);

CREATE INDEX IF NOT EXISTS idx_evidence_entity ON evidence(entity_type, entity_pk);
CREATE UNIQUE INDEX IF NOT EXISTS uq_evidence ON evidence(entity_type, entity_pk, field_name, COALESCE(NULLIF(field_value, ''), '<blank>');

CREATE TABLE IF NOT EXISTS outreach_events (
  event_pk TEXT PRIMARY KEY NOT NULL,
  location_pk TEXT NOT NULL,
  channel TEXT,
  outcome TEXT,
  notes TEXT,
  created_at TEXT NOT NULL DEFAULT '',
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
