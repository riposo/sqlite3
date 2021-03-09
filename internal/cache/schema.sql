CREATE TABLE IF NOT EXISTS cache_keys (
  key TEXT PRIMARY KEY,
  value TEXT,
  expires_at INTEGER DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_cache_keys_expires_at ON cache_keys(expires_at);

-- Same table as exists in the storage backend, but used to track
-- migration status for both. Only one schema actually has to create
-- it.
CREATE TABLE IF NOT EXISTS metainfo (
  name TEXT NOT NULL,
  value TEXT NOT NULL,

  PRIMARY KEY (name)
);
INSERT INTO metainfo VALUES ('cache_schema_version', '1')
ON CONFLICT (name) DO NOTHING;
