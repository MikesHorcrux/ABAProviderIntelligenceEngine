-- CannaRadar V1 canonical schema

CREATE TABLE IF NOT EXISTS organizations (
  org_pk TEXT PRIMARY KEY,
  legal_name TEXT,
  dba_name TEXT,
  state TEXT,
  created_at TEXT,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS licenses (
  license_pk TEXT PRIMARY KEY,
  org_pk TEXT,
  state TEXT,
  license_id TEXT,
  license_type TEXT,
  status TEXT,
  source_url TEXT,
  retrieved_at TEXT,
  fingerprint TEXT,
  FOREIGN KEY (org_pk) REFERENCES organizations(org_pk)
);

CREATE TABLE IF NOT EXISTS locations (
  location_pk TEXT PRIMARY KEY,
  org_pk TEXT,
  canonical_name TEXT,
  address_1 TEXT,
  city TEXT,
  state TEXT,
  zip TEXT,
  website_domain TEXT,
  phone TEXT,
  fit_score INTEGER DEFAULT 0,
  last_crawled_at TEXT,
  created_at TEXT,
  updated_at TEXT,
  FOREIGN KEY (org_pk) REFERENCES organizations(org_pk)
);

CREATE TABLE IF NOT EXISTS contact_points (
  contact_pk TEXT PRIMARY KEY,
  location_pk TEXT,
  type TEXT,
  value TEXT,
  confidence REAL,
  source_url TEXT,
  first_seen_at TEXT,
  last_seen_at TEXT,
  FOREIGN KEY (location_pk) REFERENCES locations(location_pk)
);

CREATE TABLE IF NOT EXISTS evidence (
  evidence_pk TEXT PRIMARY KEY,
  entity_type TEXT,
  entity_pk TEXT,
  field_name TEXT,
  field_value TEXT,
  source_url TEXT,
  snippet TEXT,
  captured_at TEXT
);

CREATE TABLE IF NOT EXISTS outreach_events (
  event_pk TEXT PRIMARY KEY,
  location_pk TEXT,
  channel TEXT,
  outcome TEXT,
  notes TEXT,
  created_at TEXT,
  FOREIGN KEY (location_pk) REFERENCES locations(location_pk)
);
