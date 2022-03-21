package storage

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"net/url"

	"github.com/riposo/riposo/pkg/conn/storage"
	"github.com/riposo/riposo/pkg/riposo"
	"github.com/riposo/riposo/pkg/schema"
	"github.com/riposo/sqlite3/internal/common"
	"go.uber.org/multierr"
)

const schemaVersion = 1

//go:embed schema.sql
var embedFS embed.FS

func init() {
	storage.Register("sqlite3", func(ctx context.Context, uri *url.URL, hlp riposo.Helpers) (storage.Backend, error) {
		return Connect(ctx, uri.String(), hlp)
	})
}

// --------------------------------------------------------------------

type conn struct {
	db   *sql.DB
	hlp  riposo.Helpers
	stmt struct {
		getModTime,
		existsObject,
		getObject,
		getObjectDeleted,
		getObjectModTime,
		createObject,
		updateObject,
		deleteObject,
		deleteObjectNested,
		purgeObjects *sql.Stmt
	}
}

// Connect connects to a PostgreSQL server.
func Connect(ctx context.Context, dsn string, hlp riposo.Helpers) (storage.Backend, error) {
	// connect to the DB.
	db, err := common.Connect(ctx, dsn, "storage_schema_version", schemaVersion, embedFS)
	if err != nil {
		return nil, err
	}

	cn := &conn{db: db, hlp: hlp}
	if err := cn.prepare(ctx); err != nil {
		_ = cn.Close()
		return nil, err
	}

	return cn, nil
}

//nolint:sqlclosecheck
func (cn *conn) prepare(ctx context.Context) (err error) {
	// create connection struct, prepare statements.
	if cn.stmt.getModTime, err = cn.db.PrepareContext(ctx, sqlGetModTime); err != nil {
		return err
	}
	if cn.stmt.existsObject, err = cn.db.PrepareContext(ctx, sqlExistsObject); err != nil {
		return err
	}
	if cn.stmt.getObject, err = cn.db.PrepareContext(ctx, sqlGetObject); err != nil {
		return err
	}
	if cn.stmt.getObjectDeleted, err = cn.db.PrepareContext(ctx, sqlGetObjectDeleted); err != nil {
		return err
	}
	if cn.stmt.getObjectModTime, err = cn.db.PrepareContext(ctx, sqlGetObjectModTime); err != nil {
		return err
	}
	if cn.stmt.createObject, err = cn.db.PrepareContext(ctx, sqlCreateObject); err != nil {
		return err
	}
	if cn.stmt.updateObject, err = cn.db.PrepareContext(ctx, sqlUpdateObject); err != nil {
		return err
	}
	if cn.stmt.deleteObject, err = cn.db.PrepareContext(ctx, sqlDeleteObject); err != nil {
		return err
	}
	if cn.stmt.deleteObjectNested, err = cn.db.PrepareContext(ctx, sqlDeleteObjectNested); err != nil {
		return err
	}
	if cn.stmt.purgeObjects, err = cn.db.PrepareContext(ctx, sqlPurgeObjects); err != nil {
		return err
	}
	return nil
}

// Ping implements storage.Backend interface.
func (cn *conn) Ping(ctx context.Context) error {
	return cn.db.PingContext(ctx)
}

// Begin implements storage.Backend interface.
func (cn *conn) Begin(ctx context.Context) (storage.Transaction, error) {
	tx, err := cn.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &transaction{Tx: tx, cn: cn, ctx: ctx}, nil
}

// Close closes the DB connection.
func (cn *conn) Close() (err error) {
	if cn.stmt.getModTime != nil {
		err = multierr.Append(err, cn.stmt.getModTime.Close())
	}
	if cn.stmt.existsObject != nil {
		err = multierr.Append(err, cn.stmt.existsObject.Close())
	}
	if cn.stmt.getObject != nil {
		err = multierr.Append(err, cn.stmt.getObject.Close())
	}
	if cn.stmt.getObjectDeleted != nil {
		err = multierr.Append(err, cn.stmt.getObjectDeleted.Close())
	}
	if cn.stmt.getObjectModTime != nil {
		err = multierr.Append(err, cn.stmt.getObjectModTime.Close())
	}
	if cn.stmt.createObject != nil {
		err = multierr.Append(err, cn.stmt.createObject.Close())
	}
	if cn.stmt.updateObject != nil {
		err = multierr.Append(err, cn.stmt.updateObject.Close())
	}
	if cn.stmt.deleteObject != nil {
		err = multierr.Append(err, cn.stmt.deleteObject.Close())
	}
	if cn.stmt.deleteObjectNested != nil {
		err = multierr.Append(err, cn.stmt.deleteObjectNested.Close())
	}
	if cn.stmt.purgeObjects != nil {
		err = multierr.Append(err, cn.stmt.purgeObjects.Close())
	}
	if cn.db != nil {
		err = multierr.Append(err, cn.db.Close())
	}
	return
}

// --------------------------------------------------------------------

type transaction struct {
	*sql.Tx
	cn  *conn
	ctx context.Context
}

// Commit implements storage.Transaction interface.
func (tx *transaction) Commit() error {
	return normErr(tx.Tx.Commit())
}

// Rollback implements storage.Transaction interface.
func (tx *transaction) Rollback() error {
	return normErr(tx.Tx.Rollback())
}

// Flush implements storage.Transaction interface.
func (tx *transaction) Flush() error {
	_, err1 := tx.ExecContext(tx.ctx, `DELETE FROM storage_objects`)
	_, err2 := tx.ExecContext(tx.ctx, `DELETE FROM storage_timestamps`)
	if errors.Is(err1, sql.ErrTxDone) || errors.Is(err2, sql.ErrTxDone) {
		return storage.ErrTxDone
	}
	return multierr.Combine(err1, err2)
}

// ModTime implements storage.Transaction interface.
func (tx *transaction) ModTime(path riposo.Path) (riposo.Epoch, error) {
	if !path.IsNode() {
		return 0, storage.ErrInvalidPath
	}

	stmt := tx.StmtContext(tx.ctx, tx.cn.stmt.getModTime)
	defer stmt.Close()

	var modTime riposo.Epoch
	ns, _ := path.Split()
	if err := stmt.
		QueryRowContext(tx.ctx, ns).
		Scan(&modTime); err != nil && err != sql.ErrNoRows {
		return 0, normErr(err)
	}
	return modTime, nil
}

// Exists implements storage.Transaction interface.
func (tx *transaction) Exists(path riposo.Path) (bool, error) {
	if path.IsNode() {
		return false, storage.ErrInvalidPath
	}

	stmt := tx.StmtContext(tx.ctx, tx.cn.stmt.existsObject)
	defer stmt.Close()

	var ok bool
	ns, objID := path.Split()
	err := stmt.
		QueryRowContext(tx.ctx, ns, objID).
		Scan(&ok)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return ok, err
}

// Get implements storage.Transaction interface.
func (tx *transaction) Get(path riposo.Path) (*schema.Object, error) {
	if path.IsNode() {
		return nil, storage.ErrInvalidPath
	}

	ns, objID := path.Split()
	return tx.get(ns, objID, false)
}

// GetForUpdate implements storage.Transaction interface.
func (tx *transaction) GetForUpdate(path riposo.Path) (*schema.Object, error) {
	if path.IsNode() {
		return nil, storage.ErrInvalidPath
	}

	if err := tx.writeLock(); err != nil {
		return nil, err
	}

	ns, objID := path.Split()
	return tx.get(ns, objID, false)
}

// Create implements storage.Transaction interface.
func (tx *transaction) Create(path riposo.Path, obj *schema.Object) error {
	if !path.IsNode() {
		return storage.ErrInvalidPath
	}

	if err := tx.writeLock(); err != nil {
		return err
	}

	ns, _ := path.Split()
	if obj.ID != "" {
		if exists, err := tx.Exists(path.WithObjectID(obj.ID)); err != nil {
			return err
		} else if exists {
			return storage.ErrObjectExists
		}
	} else {
		obj.ID = tx.cn.hlp.NextID()
	}
	obj.Norm()

	stmt := tx.StmtContext(tx.ctx, tx.cn.stmt.createObject)
	defer stmt.Close()

	if _, err := stmt.ExecContext(tx.ctx, ns, obj.ID, obj.Extra); err != nil {
		return normErr(err)
	}

	modTime, err := tx.getObjectModTime(ns, obj.ID)
	if err != nil {
		return err
	}
	obj.ModTime = modTime
	return nil
}

// Update implements storage.Transaction interface.
func (tx *transaction) Update(path riposo.Path, obj *schema.Object) error {
	obj.Norm()

	stmt := tx.StmtContext(tx.ctx, tx.cn.stmt.updateObject)
	defer stmt.Close()

	ns, objID := path.Split()
	if _, err := stmt.ExecContext(tx.ctx, ns, objID, obj.Extra); err != nil {
		return normErr(err)
	}

	modTime, err := tx.getObjectModTime(ns, objID)
	if err != nil {
		return err
	}
	obj.ModTime = modTime
	return nil
}

// Delete implements storage.Transaction interface.
func (tx *transaction) Delete(path riposo.Path) (*schema.Object, error) {
	if path.IsNode() {
		return nil, storage.ErrInvalidPath
	}

	stmt1 := tx.StmtContext(tx.ctx, tx.cn.stmt.deleteObject)
	defer stmt1.Close()

	ns, objID := path.Split()
	res, err := stmt1.ExecContext(tx.ctx, ns, objID)
	if err != nil {
		return nil, normErr(err)
	}

	num, err := res.RowsAffected()
	if err != nil {
		return nil, err
	} else if num == 0 {
		return nil, storage.ErrNotFound
	}

	stmt2 := tx.StmtContext(tx.ctx, tx.cn.stmt.deleteObjectNested)
	defer stmt2.Close()

	if _, err := stmt2.ExecContext(tx.ctx, string(path)+"/%"); err != nil {
		return nil, normErr(err)
	}

	return tx.get(ns, objID, true)
}

// CountAll implements storage.Transaction interface.
func (tx *transaction) CountAll(path riposo.Path, opt storage.CountOptions) (int64, error) {
	if !path.IsNode() {
		return 0, storage.ErrInvalidPath
	}

	stmt := newQueryBuilder()
	defer stmt.Release()

	ns, _ := path.Split()
	stmt.AppendString(`SELECT COUNT(*) FROM storage_objects`)
	stmt.Where(`path = `)
	stmt.AppendValue(ns)
	stmt.Where(`NOT deleted`)
	stmt.ConditionFilter(opt.Condition)

	var cnt int64
	err := stmt.
		QueryRowContext(tx.ctx, tx).
		Scan(&cnt)
	return cnt, normErr(err)
}

// ListAll implements storage.Transaction interface.
func (tx *transaction) ListAll(objs []*schema.Object, path riposo.Path, opt storage.ListOptions) ([]*schema.Object, error) {
	if !path.IsNode() {
		return objs, storage.ErrInvalidPath
	}

	stmt := newQueryBuilder()
	defer stmt.Release()

	ns, _ := path.Split()
	stmt.AppendString(`SELECT id, last_modified, deleted, data FROM storage_objects`)
	stmt.Where(`path = `)
	stmt.AppendValue(ns)
	stmt.InclusionFilter(opt.Include)
	stmt.ConditionFilter(opt.Condition)
	stmt.PaginationFilter(opt.Pagination)
	stmt.OrderBy(opt.Sort)
	stmt.Limit(opt.Limit)

	rows, err := stmt.QueryContext(tx.ctx, tx)
	if err != nil {
		return objs, normErr(err)
	}
	defer rows.Close()

	for rows.Next() {
		var obj schema.Object
		if err := rows.Scan(&obj.ID, &obj.ModTime, &obj.Deleted, &obj.Extra); err != nil {
			return objs, err
		}
		objs = append(objs, &obj)
	}

	return objs, rows.Err()
}

// DeleteAll implements storage.Transaction interface.
func (tx *transaction) DeleteAll(paths []riposo.Path) (riposo.Epoch, []riposo.Path, error) {
	for _, path := range paths {
		if path.IsNode() {
			return 0, nil, storage.ErrInvalidPath
		}
	}
	if len(paths) == 0 {
		return 0, nil, nil
	}

	stmt := newQueryBuilder()
	defer stmt.Release()

	// collect paths to be deleted
	stmt.AppendString(`SELECT path, id FROM storage_objects WHERE NOT deleted AND (`)
	for i, path := range paths {
		if i != 0 {
			stmt.AppendString(" OR ")
		}
		appendPathConstraint(stmt, path, true)
	}
	stmt.AppendByte(')')

	rows, err := stmt.QueryContext(tx.ctx, tx)
	if err != nil {
		return 0, nil, normErr(err)
	}
	defer rows.Close()

	deleted := make([]riposo.Path, 0, len(paths))
	for rows.Next() {
		var ns, objID string
		if err := rows.Scan(&ns, &objID); err != nil {
			return 0, nil, err
		}
		deleted = append(deleted, riposo.JoinPath(ns, objID))
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}

	// exit early if nothing to delete
	if len(deleted) == 0 {
		return 0, nil, nil
	}

	// delete collected paths (recursive)
	stmt.Reset()
	stmt.AppendString(`UPDATE storage_objects SET deleted = TRUE, last_modified = NULL WHERE NOT deleted AND (`)
	for i, path := range deleted {
		if i != 0 {
			stmt.AppendString(" OR ")
		}
		appendPathConstraint(stmt, path, false)
	}
	stmt.AppendByte(')')
	if _, err := stmt.ExecContext(tx.ctx, tx); err != nil {
		return 0, nil, normErr(err)
	}

	// retrieve updated mod time
	stmt.Reset()
	stmt.AppendString(`SELECT COALESCE(MAX(last_modified), 0) FROM storage_objects WHERE deleted AND (`)
	for i, path := range paths {
		if i != 0 {
			stmt.AppendString(" OR ")
		}
		appendPathConstraint(stmt, path, false)
	}
	stmt.AppendByte(')')

	var modTime riposo.Epoch
	if err := stmt.
		QueryRowContext(tx.ctx, tx).
		Scan(&modTime); err != nil {
		return 0, nil, normErr(err)
	}
	return modTime, deleted, nil
}

// Purge implements storage.Transaction interface.
func (tx *transaction) Purge(olderThan riposo.Epoch) (int64, error) {
	stmt := tx.StmtContext(tx.ctx, tx.cn.stmt.purgeObjects)
	defer stmt.Close()

	res, err := stmt.ExecContext(tx.ctx, olderThan.IsZero(), olderThan)
	if err != nil {
		return 0, normErr(err)
	}
	return res.RowsAffected()
}

func (tx *transaction) get(ns, objID string, deleted bool) (*schema.Object, error) {
	stmt := tx.cn.stmt.getObject
	if deleted {
		stmt = tx.cn.stmt.getObjectDeleted
	}

	stmt = tx.StmtContext(tx.ctx, stmt)
	defer stmt.Close()

	var obj schema.Object
	if err := stmt.
		QueryRowContext(tx.ctx, ns, objID).
		Scan(&obj.ModTime, &obj.Extra); err != nil {
		return nil, normErr(err)
	}

	obj.ID = objID
	obj.Deleted = deleted
	return &obj, nil
}

func (tx *transaction) getObjectModTime(ns, objID string) (riposo.Epoch, error) {
	stmt := tx.StmtContext(tx.ctx, tx.cn.stmt.getObjectModTime)
	defer stmt.Close()

	var modTime riposo.Epoch
	if err := stmt.
		QueryRowContext(tx.ctx, ns, objID).
		Scan(&modTime); err != nil {
		return 0, normErr(err)
	}
	return modTime, nil
}

func (tx *transaction) writeLock() error {
	_, err := tx.ExecContext(tx.ctx, "PRAGMA user_version = 0")
	return normErr(err)
}

func normErr(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrNotFound
	} else if errors.Is(err, sql.ErrTxDone) {
		return storage.ErrTxDone
	}
	return err
}

func appendPathConstraint(stmt *queryBuilder, path riposo.Path, deep bool) {
	ns, objID := path.Split()
	stmt.AppendString("(path = ")
	stmt.AppendValue(ns)
	stmt.AppendString(" AND id = ")
	stmt.AppendValue(objID)
	if deep {
		stmt.AppendString(" OR path LIKE ")
		stmt.AppendValue(ns + "/" + objID + "/%")
	}
	stmt.AppendByte(')')
}
