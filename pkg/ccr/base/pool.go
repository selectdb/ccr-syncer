package base

import (
	"database/sql"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

const (
	MaxOpenConns    = 0
	MaxIdleConns    = 13
	MaxConnLifeTime = 0
)

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
		db.SetMaxIdleConns(MaxIdleConns)
		db.SetConnMaxLifetime(MaxConnLifeTime)

		cachedSqlDbPool.pool[dsn] = db
		return db, nil
	}
}

// TODO: 添加超时和Ping检测
