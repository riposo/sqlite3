CREATE TABLE IF NOT EXISTS permission_principals (
  user_id TEXT COLLATE BINARY NOT NULL,
  principal TEXT NOT NULL,

  PRIMARY KEY (user_id, principal)
);

CREATE TABLE IF NOT EXISTS permission_paths (
  path TEXT COLLATE BINARY NOT NULL,
  permission TEXT NOT NULL,
  principal TEXT NOT NULL,

  PRIMARY KEY (path, permission, principal)
);
CREATE INDEX IF NOT EXISTS idx_permission_paths_permission
  ON permission_paths(permission);
CREATE INDEX IF NOT EXISTS idx_permission_paths_principal
  ON permission_paths(principal);

-- Same table as exists in the storage backend, but used to track
-- migration status for both. Only one schema actually has to create
-- it.
CREATE TABLE IF NOT EXISTS metainfo (
  name TEXT NOT NULL,
  value TEXT NOT NULL,

  PRIMARY KEY (name)
);
INSERT INTO metainfo VALUES ('permission_schema_version', '1')
ON CONFLICT (name) DO NOTHING;
