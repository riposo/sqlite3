CREATE TABLE IF NOT EXISTS storage_objects (
  path TEXT COLLATE BINARY NOT NULL,
  id TEXT COLLATE BINARY NOT NULL,
  last_modified INTEGER,
  data TEXT NOT NULL DEFAULT '{}',
  deleted BOOLEAN NOT NULL,

  PRIMARY KEY (path, id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_objects_path_last_modified
ON storage_objects(path, last_modified DESC);
CREATE INDEX IF NOT EXISTS idx_storage_objects_last_modified
ON storage_objects(last_modified);

CREATE TABLE IF NOT EXISTS storage_timestamps (
  path TEXT COLLATE BINARY NOT NULL,
  last_modified INTEGER NOT NULL,

  PRIMARY KEY (path)
);

--
-- triggers
--
CREATE TRIGGER IF NOT EXISTS tgr_storage_objects_after_insert
AFTER INSERT ON storage_objects
FOR EACH ROW WHEN (NEW.last_modified IS NULL)
BEGIN
  INSERT INTO storage_timestamps (path, last_modified)
  VALUES (NEW.path, CAST(JULIANDAY('now') * 86400000 - 210866760000000 AS INTEGER))
  ON CONFLICT (path) DO UPDATE
  SET last_modified = CASE WHEN last_modified < EXCLUDED.last_modified THEN EXCLUDED.last_modified ELSE last_modified + 1 END;

  UPDATE storage_objects
  SET last_modified = (SELECT last_modified FROM storage_timestamps WHERE path = NEW.path)
  WHERE path = NEW.path AND id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS tgr_storage_objects_after_update
AFTER UPDATE OF data, deleted ON storage_objects
FOR EACH ROW WHEN (NEW.last_modified IS NULL)
BEGIN
  INSERT INTO storage_timestamps (path, last_modified)
  VALUES (NEW.path, CAST(JULIANDAY('now') * 86400000 - 210866760000000 AS INTEGER))
  ON CONFLICT (path) DO UPDATE
  SET last_modified = CASE WHEN last_modified < EXCLUDED.last_modified THEN EXCLUDED.last_modified ELSE last_modified + 1 END;

  UPDATE storage_objects
  SET last_modified = (SELECT last_modified FROM storage_timestamps WHERE path = NEW.path)
  WHERE path = NEW.path AND id = NEW.id;
END;

--
-- metainfo table
--
CREATE TABLE IF NOT EXISTS metainfo (
  name TEXT NOT NULL,
  value TEXT NOT NULL,

  PRIMARY KEY (name)
);
INSERT INTO metainfo VALUES ('storage_schema_version', '1')
ON CONFLICT (name) DO NOTHING;
