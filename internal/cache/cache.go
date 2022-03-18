package cache

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"net/url"
	"time"

	"github.com/riposo/riposo/pkg/conn/cache"
	"github.com/riposo/riposo/pkg/riposo"
	"github.com/riposo/sqlite3/internal/common"
	"go.uber.org/multierr"
)

const schemaVersion = 1

//go:embed schema.sql
var embedFS embed.FS

func init() {
	cache.Register("sqlite3", func(ctx context.Context, uri *url.URL, _ riposo.Helpers) (cache.Backend, error) {
		return Connect(ctx, uri.String())
	})
}

// --------------------------------------------------------------------

type conn struct {
	db   *sql.DB
	stop context.CancelFunc
	stmt struct {
		getKey, setKey, delKey, prune *sql.Stmt
	}
}

// Connect connects to the DB.
func Connect(ctx context.Context, dsn string) (cache.Backend, error) {
	// Connect to the DB.
	db, err := common.Connect(ctx, dsn, "cache_schema_version", schemaVersion, embedFS)
	if err != nil {
		return nil, err
	}

	// Create connection struct, prepare statements.
	cn := &conn{db: db}
	if err := cn.prepare(ctx); err != nil {
		_ = cn.Close()
		return nil, err
	}

	// Setup periodic prunning in the background.
	pruneCtx, stop := context.WithCancel(ctx)
	cn.stop = stop
	go cn.pruneLoop(pruneCtx)

	return cn, nil
}

//nolint:sqlclosecheck
func (cn *conn) prepare(ctx context.Context) (err error) {
	if cn.stmt.getKey, err = cn.db.PrepareContext(ctx, sqlGetKey); err != nil {
		return
	}
	if cn.stmt.setKey, err = cn.db.PrepareContext(ctx, sqlSetKey); err != nil {
		return
	}
	if cn.stmt.delKey, err = cn.db.PrepareContext(ctx, sqlDelKey); err != nil {
		return
	}
	if cn.stmt.prune, err = cn.db.PrepareContext(ctx, sqlPrune); err != nil {
		return
	}

	// Prune expired.
	err = cn.pruneExpired(ctx, time.Now())
	return
}

// Ping implements cache.Backend interface.
func (cn *conn) Ping(ctx context.Context) error {
	return cn.db.PingContext(ctx)
}

// Begin implements cache.Backend interface.
func (cn *conn) Begin(ctx context.Context) (cache.Transaction, error) {
	tx, err := cn.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &transaction{Tx: tx, cn: cn, ctx: ctx}, nil
}

// Close implements cache.Backend.
func (cn *conn) Close() (err error) {
	if cn.stop != nil {
		cn.stop() // stop prune loop
	}

	if cn.stmt.getKey != nil {
		err = multierr.Append(err, cn.stmt.getKey.Close())
	}
	if cn.stmt.setKey != nil {
		err = multierr.Append(err, cn.stmt.setKey.Close())
	}
	if cn.stmt.delKey != nil {
		err = multierr.Append(err, cn.stmt.delKey.Close())
	}
	if cn.stmt.prune != nil {
		err = multierr.Append(err, cn.stmt.prune.Close())
	}
	return
}

func (cn *conn) pruneExpired(ctx context.Context, now time.Time) error {
	_, err := cn.stmt.prune.ExecContext(ctx, now.UTC())
	return normErr(err)
}

func (cn *conn) pruneLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			_ = cn.pruneExpired(ctx, now)
		}
	}
}

// --------------------------------------------------------------------

type transaction struct {
	*sql.Tx
	cn  *conn
	ctx context.Context
}

// Commit implements cache.Transaction interface.
func (tx *transaction) Commit() error {
	return normErr(tx.Tx.Commit())
}

// Rollback implements cache.Transaction interface.
func (tx *transaction) Rollback() error {
	return normErr(tx.Tx.Rollback())
}

// Flush implements cache.Transaction interface.
func (tx *transaction) Flush() error {
	_, err := tx.ExecContext(tx.ctx, `DELETE FROM cache_keys`)
	return normErr(err)
}

// Get implements cache.Transaction.
func (tx *transaction) Get(key string) ([]byte, error) {
	if err := cache.ValidateKey(key); err != nil {
		return nil, err
	}

	stmt := tx.StmtContext(tx.ctx, tx.cn.stmt.getKey)
	defer stmt.Close()

	var val []byte
	err := stmt.
		QueryRowContext(tx.ctx, key, time.Now().UTC()).
		Scan(&val)
	if err != nil {
		return nil, normErr(err)
	}
	return val, nil
}

// Set implements cache.Transaction.
func (tx *transaction) Set(key string, val []byte, exp time.Time) error {
	if err := cache.ValidateKey(key); err != nil {
		return err
	}

	stmt := tx.StmtContext(tx.ctx, tx.cn.stmt.setKey)
	defer stmt.Close()

	_, err := stmt.ExecContext(tx.ctx, key, val, exp.UTC())
	return normErr(err)
}

// Del implements cache.Transaction.
func (tx *transaction) Del(key string) error {
	if err := cache.ValidateKey(key); err != nil {
		return err
	}

	stmt := tx.StmtContext(tx.ctx, tx.cn.stmt.delKey)
	defer stmt.Close()

	res, err := stmt.ExecContext(tx.ctx, key, time.Now().UTC())
	if err != nil {
		return normErr(err)
	}

	num, err := res.RowsAffected()
	if err != nil {
		return err
	} else if num == 0 {
		return cache.ErrNotFound
	}
	return nil
}

func normErr(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return cache.ErrNotFound
	} else if errors.Is(err, sql.ErrTxDone) {
		return cache.ErrTxDone
	}
	return err
}
