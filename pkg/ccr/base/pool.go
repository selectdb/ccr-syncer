package base

import (
	"database/sql"
	"flag"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

const (
	DefaultMaxOpenConns    = 20
	DefaultMaxIdleConns    = 5
	DefaultMaxConnLifeTime = 0
)

var (
	MaxOpenConns    = DefaultMaxOpenConns
	MaxConnLifeTime = time.Duration(DefaultMaxConnLifeTime) * time.Second
)

func init() {
	flag.IntVar(&MaxOpenConns, "fe_max_open_conns", DefaultMaxOpenConns, "fe max open connections")
	flag.DurationVar(&MaxConnLifeTime, "fe_max_conn_lifetime", time.Duration(DefaultMaxConnLifeTime)*time.Second, "fe max connection lifetime")
}

type cachedMysqlDbPool struct {
	pool map[string]*sql.DB
	mu   sync.Mutex
}

var cachedSqlDbPool *cachedMysqlDbPool

func init() {
	cachedSqlDbPool = &cachedMysqlDbPool{
		pool: make(map[string]*sql.DB),
	}
}

func GetMysqlDB(dsn string) (*sql.DB, error) {
	cachedSqlDbPool.mu.Lock()
	defer cachedSqlDbPool.mu.Unlock()

	if db, ok := cachedSqlDbPool.pool[dsn]; ok {
		return db, nil
	}

	if db, err := sql.Open("mysql", dsn); err != nil {
		return nil, xerror.Wrapf(err, xerror.DB, "connect to mysql failed, dsn: %s", dsn)
	} else {
		db.SetMaxOpenConns(MaxOpenConns)
		if MaxOpenConns > 0 {
			db.SetMaxIdleConns(MaxOpenConns / 4)
		} else {
			db.SetMaxIdleConns(DefaultMaxIdleConns)
		}
		db.SetConnMaxLifetime(MaxConnLifeTime)

		cachedSqlDbPool.pool[dsn] = db
		return db, nil
	}
}
