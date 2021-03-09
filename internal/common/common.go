package common

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3" // this is specifically for sqlite3
)

// Connect connects to a PG database.
func Connect(ctx context.Context, dsn string, versionField string, targetVersion int32, fs embed.FS) (*sql.DB, error) {
	schema := "sqlite3"
	if pos := strings.Index(dsn, "://"); pos > -1 {
		schema, dsn = dsn[:pos], dsn[pos+3:]
	}

	db, err := sql.Open(schema, dsn)
	if err != nil {
		return nil, err
	}

	if err := validateEncoding(ctx, db, "UTF-8"); err != nil {
		_ = db.Close()
		return nil, err
	}

	version, err := schemaVersion(ctx, db, versionField)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	// Create schema if version is 0, migrate otherwise.
	if version == 0 {
		err = createSchema(ctx, db, fs)
	} else {
		err = migrateSchema(ctx, db, version, targetVersion)
	}
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

// validateEncoding makes sure database is set to specific encoding.
func validateEncoding(ctx context.Context, db *sql.DB, encoding string) error {
	var value string
	if err := db.QueryRowContext(ctx, `
		PRAGMA encoding
	`).Scan(&value); err != nil {
		return fmt.Errorf("encoding check failed with %w", err)
	} else if strings.ToUpper(value) != encoding {
		return fmt.Errorf("unexpected database encoding %q", value)
	}
	return nil
}

// tableExists returns true if a table exists.
func tableExists(ctx context.Context, db *sql.DB, table string) (bool, error) {
	var value string
	err := db.QueryRowContext(ctx, `
		SELECT name
		FROM sqlite_master
		WHERE type = $1
		  AND name = $2;
	`, "table", table).Scan(&value)

	if err == sql.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("table check failed with %w", err)
	}
	return true, nil
}

// schemaVersion returns the stored schema version.
func schemaVersion(ctx context.Context, db *sql.DB, field string) (version int32, err error) {
	if ok, err := tableExists(ctx, db, "metainfo"); err != nil {
		return 0, err
	} else if !ok {
		return 0, nil
	}

	if err = db.QueryRowContext(ctx, `
		SELECT COALESCE(CAST value AS INTEGER), 0) AS version
		FROM metainfo
		WHERE name = $1
	`, field).Scan(&version); err == sql.ErrNoRows {
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("schema check failed with %w", err)
	}
	return
}
