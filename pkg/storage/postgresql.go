package storage

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type PostgresqlDB struct {
	db *sql.DB
}

func NewPostgresqlDB(host string, port int, user string, password string) (DB, error) {
	url := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", user, password, host, port, "postgres")
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, xerror.Wrapf(err, xerror.DB, "postgresql: open %s:%d failed", host, port)
	}

	SetDBOptions(db)

	if _, err := db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", remoteDBName)); err != nil {
		return nil, xerror.Wrapf(err, xerror.DB, "postgresql: create schema %s failed", remoteDBName)
	}

	if _, err = db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.jobs (job_name VARCHAR(512) PRIMARY KEY, job_info TEXT, belong_to VARCHAR(96))", remoteDBName)); err != nil {
		return nil, xerror.Wrap(err, xerror.DB, "postgresql: create table jobs failed")
	}

	if _, err = db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.progresses (job_name VARCHAR(512) PRIMARY KEY, progress TEXT)", remoteDBName)); err != nil {
		return nil, xerror.Wrap(err, xerror.DB, "postgresql: create table progresses failed")
	}

	if _, err = db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.syncers (host_info VARCHAR(96) PRIMARY KEY, timestamp BIGINT)", remoteDBName)); err != nil {
		return nil, xerror.Wrap(err, xerror.DB, "postgresql: create table syncers failed")
	}

	return &PostgresqlDB{db: db}, nil
}

func (s *PostgresqlDB) AddJob(jobName string, jobInfo string, hostInfo string) error {
	// check job name exists, if exists, return error
	var count int
	if err := s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.jobs WHERE job_name = '%s'", remoteDBName, jobName)).Scan(&count); err != nil {
		return xerror.Wrapf(err, xerror.DB, "postgresql: query job name %s failed", jobName)
	}

	if count > 0 {
		return ErrJobExists
	}

	// insert job info
	insertSql := fmt.Sprintf("INSERT INTO %s.jobs (job_name, job_info, belong_to) VALUES ('%s', '%s', '%s')", remoteDBName, jobName, jobInfo, hostInfo)
	if _, err := s.db.Exec(insertSql); err != nil {
		return xerror.Wrapf(err, xerror.DB, "postgresql: insert job name %s failed", jobName)
	} else {
		return nil
	}
}

// Update Job
func (s *PostgresqlDB) UpdateJob(jobName string, jobInfo string) error {
	// check job name exists, if not exists, return error
	var count int
	if err := s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.jobs WHERE job_name = '%s'", remoteDBName, jobName)).Scan(&count); err != nil {
		return xerror.Wrapf(err, xerror.DB, "postgresql: query job name %s failed", jobName)
	}

	if count == 0 {
		return ErrJobNotExists
	}

	// update job info
	if _, err := s.db.Exec(fmt.Sprintf("UPDATE %s.jobs SET job_info = '%s' WHERE job_name = '%s'", remoteDBName, jobInfo, jobName)); err != nil {
		return xerror.Wrapf(err, xerror.DB, "postgresql: update job name %s failed", jobName)
	} else {
		return nil
	}
}

func (s *PostgresqlDB) RemoveJob(jobName string) error {
	txn, err := s.db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  false,
	})
	if err != nil {
		return xerror.Wrapf(err, xerror.DB, "postgresql: remove job begin transaction failed, name: %s", jobName)
	}

	if _, err := txn.Exec(fmt.Sprintf("DELETE FROM %s.jobs WHERE job_name = '%s'", remoteDBName, jobName)); err != nil {
		if err := txn.Rollback(); err != nil {
			return xerror.Wrapf(err, xerror.DB, "postgresql: remove job failed, name: %s, and rollback failed too", jobName)
		}
		return xerror.Wrapf(err, xerror.DB, "postgresql: remove job failed, name: %s", jobName)
	}

	if _, err := txn.Exec(fmt.Sprintf("DELETE FROM %s.progresses WHERE job_name = '%s'", remoteDBName, jobName)); err != nil {
		if err := txn.Rollback(); err != nil {
			return xerror.Wrapf(err, xerror.DB, "postgresql: remove progresses failed, name: %s, and rollback failed too", jobName)
		}
		return xerror.Wrapf(err, xerror.DB, "postgresql: remove progresses failed, name: %s", jobName)
	}

	if err := txn.Commit(); err != nil {
		return xerror.Wrapf(err, xerror.DB, "postgresql: remove job txn commit failed.")
	}

	return nil
}

func (s *PostgresqlDB) IsJobExist(jobName string) (bool, error) {
	var count int
	if err := s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.jobs WHERE job_name = '%s'", remoteDBName, jobName)).Scan(&count); err != nil {
		return false, xerror.Wrapf(err, xerror.DB, "postgresql: query job name %s failed", jobName)
	} else {
		return count > 0, nil
	}
}

func (s *PostgresqlDB) GetJobInfo(jobName string) (string, error) {
	var jobInfo string
	if err := s.db.QueryRow(fmt.Sprintf("SELECT job_info FROM %s.jobs WHERE job_name = '%s'", remoteDBName, jobName)).Scan(&jobInfo); err != nil {
		return "", xerror.Wrapf(err, xerror.DB, "postgresql: get job failed, name: %s", jobName)
	}
	return jobInfo, nil
}

func (s *PostgresqlDB) GetJobBelong(jobName string) (string, error) {
	var belong string
	if err := s.db.QueryRow(fmt.Sprintf("SELECT belong_to FROM %s.jobs WHERE job_name = '%s'", remoteDBName, jobName)).Scan(&belong); err != nil {
		return "", xerror.Wrapf(err, xerror.DB, "postgresql: get job belong failed, name: %s", jobName)
	}
	return belong, nil
}

func (s *PostgresqlDB) UpdateProgress(jobName string, progress string) error {
	// quoteProgress := strings.ReplaceAll(progress, "\"", "\\\"")
	encodeProgress := base64.StdEncoding.EncodeToString([]byte(progress))
	updateSql := fmt.Sprintf("INSERT INTO %s.progresses (job_name, progress) VALUES ('%s', '%s') ON CONFLICT (job_name) DO UPDATE SET progress = EXCLUDED.progress", remoteDBName, jobName, encodeProgress)
	if result, err := s.db.Exec(updateSql); err != nil {
		return xerror.Wrapf(err, xerror.DB, "postgresql: update progress failed")
	} else if rowNum, err := result.RowsAffected(); err != nil {
		return xerror.Wrapf(err, xerror.DB, "postgresql: update progress get affected rows failed")
	} else if rowNum != 1 {
		return xerror.Wrapf(err, xerror.DB, "postgresql: update progress affected rows error, rows: %d", rowNum)
	}

	return nil
}

func (s *PostgresqlDB) IsProgressExist(jobName string) (bool, error) {
	var count int
	if err := s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.progresses WHERE job_name = '%s'", remoteDBName, jobName)).Scan(&count); err != nil {
		return false, xerror.Wrapf(err, xerror.DB, "postgresql: query job name %s failed", jobName)
	}

	return count > 0, nil
}

func (s *PostgresqlDB) GetProgress(jobName string) (string, error) {
	var progress string
	if err := s.db.QueryRow(fmt.Sprintf("SELECT progress FROM %s.progresses WHERE job_name = '%s'", remoteDBName, jobName)).Scan(&progress); err != nil {
		return "", xerror.Wrapf(err, xerror.DB, "postgresql: query progress failed")
	}
	decodeProgress, err := base64.StdEncoding.DecodeString(progress)
	if err != nil {
		return "", xerror.Errorf(xerror.DB, "postgresql: base64 decode error")
	}

	return string(decodeProgress), nil
}

func (s *PostgresqlDB) AddSyncer(hostInfo string) error {
	timestamp := time.Now().UnixNano()
	addSql := fmt.Sprintf("INSERT INTO %s.syncers (host_info, timestamp) VALUES ('%s', %d) ON CONFLICT (host_info) DO UPDATE SET timestamp = EXCLUDED.timestamp", remoteDBName, hostInfo, timestamp)
	if result, err := s.db.Exec(addSql); err != nil {
		return xerror.Wrapf(err, xerror.DB, "postgresql: add syncer failed")
	} else if rowNum, err := result.RowsAffected(); err != nil {
		return xerror.Wrapf(err, xerror.DB, "postgresql: add syncer get affected rows failed")
	} else if rowNum != 1 {
		return xerror.Wrapf(err, xerror.DB, "postgresql: add syncer affected rows error, rows: %d", rowNum)
	}

	return nil
}

func (s *PostgresqlDB) RefreshSyncer(hostInfo string, lastStamp int64) (int64, error) {
	nowTime := time.Now().UnixNano()
	refreshSql := fmt.Sprintf("UPDATE %s.syncers SET timestamp = %d WHERE host_info = '%s' AND timestamp = %d", remoteDBName, nowTime, hostInfo, lastStamp)
	result, err := s.db.Exec(refreshSql)
	if err != nil {
		return -1, xerror.Wrapf(err, xerror.DB, "postgresql: refresh syncer failed.")
	}

	if rowNum, err := result.RowsAffected(); err != nil {
		return -1, xerror.Wrapf(err, xerror.DB, "postgresql: get RowsAffected failed.")
	} else if rowNum != 1 {
		return -1, nil
	} else {
		return nowTime, nil
	}
}

func (s *PostgresqlDB) GetStampAndJobs(hostInfo string) (int64, []string, error) {
	txn, err := s.db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return -1, nil, xerror.Wrapf(err, xerror.DB, "postgresql: begin IMMEDIATE transaction failed.")
	}

	var timestamp int64
	if err := txn.QueryRow(fmt.Sprintf("SELECT timestamp FROM %s.syncers WHERE host_info = '%s'", remoteDBName, hostInfo)).Scan(&timestamp); err != nil {
		return -1, nil, xerror.Wrapf(err, xerror.DB, "postgresql: get stamp failed.")
	}

	jobs := make([]string, 0)
	rows, err := s.db.Query(fmt.Sprintf("SELECT job_name FROM %s.jobs WHERE belong_to = '%s'", remoteDBName, hostInfo))
	if err != nil {
		return -1, nil, xerror.Wrapf(err, xerror.DB, "postgresql: get job_nums failed.")
	}
	defer rows.Close()

	for rows.Next() {
		var jobName string
		if err := rows.Scan(&jobName); err != nil {
			return -1, nil, xerror.Wrapf(err, xerror.DB, "postgresql: scan job_name failed.")
		}
		jobs = append(jobs, jobName)
	}

	if err := txn.Commit(); err != nil {
		return -1, nil, xerror.Wrapf(err, xerror.DB, "postgresql: get jobs & stamp txn commit failed.")
	}

	return timestamp, jobs, nil
}

func (s *PostgresqlDB) GetDeadSyncers(expiredTime int64) ([]string, error) {
	row, err := s.db.Query(fmt.Sprintf("SELECT host_info FROM %s.syncers WHERE timestamp < %d", remoteDBName, expiredTime))
	if err != nil {
		return nil, xerror.Wrapf(err, xerror.DB, "postgresql: get orphan job info failed.")
	}
	defer row.Close()
	deadSyncers := make([]string, 0)
	for row.Next() {
		var hostInfo string
		if err := row.Scan(&hostInfo); err != nil {
			return nil, xerror.Wrapf(err, xerror.DB, "postgresql: scan host_info and jobs failed")
		}
		deadSyncers = append(deadSyncers, hostInfo)
	}

	return deadSyncers, nil
}

func (s *PostgresqlDB) getOrphanJobs(txn *sql.Tx, syncers []string) ([]string, error) {
	orphanJobs := make([]string, 0)
	for _, deadSyncer := range syncers {
		rows, err := txn.Query(fmt.Sprintf("SELECT job_name FROM %s.jobs WHERE belong_to = '%s'", remoteDBName, deadSyncer))
		if err != nil {
			return nil, xerror.Wrapf(err, xerror.DB, "postgresql: get orphan jobs failed.")
		}

		for rows.Next() {
			var jobName string
			if err := rows.Scan(&jobName); err != nil {
				return nil, xerror.Wrapf(err, xerror.DB, "postgresql: scan orphan job name failed.")
			}
			orphanJobs = append(orphanJobs, jobName)
		}
		rows.Close()

		if _, err := txn.Exec(fmt.Sprintf("DELETE FROM %s.syncers WHERE host_info = '%s'", remoteDBName, deadSyncer)); err != nil {
			return nil, xerror.Wrapf(err, xerror.DB, "postgresql: delete dead syncer failed, name: %s", deadSyncer)
		}
	}

	return orphanJobs, nil
}

func (s *PostgresqlDB) getLoadInfo(txn *sql.Tx) (LoadSlice, int, error) {
	load := make(LoadSlice, 0)
	sumLoad := 0
	host_rows, err := txn.Query(fmt.Sprintf("SELECT host_info FROM %s.syncers", remoteDBName))
	if err != nil {
		return nil, -1, xerror.Wrapf(err, xerror.DB, "postgresql: get all syncers failed.")
	}
	for host_rows.Next() {
		loadInfo := LoadInfo{AddedLoad: 0}
		if err := host_rows.Scan(&loadInfo.HostInfo); err != nil {
			return nil, -1, xerror.Wrapf(err, xerror.DB, "postgresql: scan load info failed.")
		}
		load = append(load, loadInfo)
	}
	host_rows.Close()

	for i := range load {
		if err := txn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.jobs WHERE belong_to = '%s'", remoteDBName, load[i].HostInfo)).Scan(&load[i].NowLoad); err != nil {
			return nil, -1, xerror.Wrapf(err, xerror.DB, "postgresql: get syncer %s load failed.", load[i].HostInfo)
		}
		sumLoad += load[i].NowLoad
	}

	return load, sumLoad, nil
}

func (s *PostgresqlDB) dispatchJobs(txn *sql.Tx, hostInfo string, additionalJobs []string) error {
	for _, jobName := range additionalJobs {
		if _, err := txn.Exec(fmt.Sprintf("UPDATE %s.jobs SET belong_to = '%s' WHERE job_name = '%s'", remoteDBName, hostInfo, jobName)); err != nil {
			return xerror.Wrapf(err, xerror.DB, "postgresql: update job belong_to failed, name: %s", jobName)
		}
	}
	if _, err := txn.Exec(fmt.Sprintf("UPDATE %s.syncers SET timestamp = %d WHERE host_info = '%s'", remoteDBName, time.Now().UnixNano(), hostInfo)); err != nil {
		return xerror.Wrapf(err, xerror.DB, "postgresql: update syncer timestamp failed, host: %s", hostInfo)
	}

	return nil
}

func (s *PostgresqlDB) RebalanceLoadFromDeadSyncers(syncers []string) error {
	txn, err := s.db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  false,
	})
	if err != nil {
		return xerror.Wrap(err, xerror.DB, "postgresql: rebalance load begin txn failed")
	}

	orphanJobs, err := s.getOrphanJobs(txn, syncers)
	if err != nil {
		return err
	}

	additionalLoad := len(orphanJobs)
	loadList, currentLoad, err := s.getLoadInfo(txn)
	if err != nil {
		return err
	}

	loadList, err = RebalanceLoad(additionalLoad, currentLoad, loadList)
	if err != nil {
		return err
	}
	for i := range loadList {
		beginIdx := additionalLoad - loadList[i].AddedLoad
		if err := s.dispatchJobs(txn, loadList[i].HostInfo, orphanJobs[beginIdx:additionalLoad]); err != nil {
			if err := txn.Rollback(); err != nil {
				return xerror.Wrap(err, xerror.DB, "postgresql: rebalance rollback failed.")
			}
			return err
		}
		additionalLoad = beginIdx
	}

	if err := txn.Commit(); err != nil {
		return xerror.Wrap(err, xerror.DB, "postgresql: rebalance txn commit failed.")
	}

	return nil
}

func (s *PostgresqlDB) GetAllData() (map[string][]string, error) {
	ans := make(map[string][]string)

	jobRows, err := s.db.Query(fmt.Sprintf("SELECT job_name, belong_to FROM %s.jobs", remoteDBName))
	if err != nil {
		return nil, xerror.Wrap(err, xerror.DB, "postgresql: get jobs data failed.")
	}
	jobData := make([]string, 0)
	for jobRows.Next() {
		var jobName string
		var belongTo string
		if err := jobRows.Scan(&jobName, &belongTo); err != nil {
			return nil, xerror.Wrap(err, xerror.DB, "postgresql: scan jobs row failed.")
		}
		jobData = append(jobData, fmt.Sprintf("%s, %s", jobName, belongTo))
	}
	ans["jobs"] = jobData
	jobRows.Close()

	syncerRows, err := s.db.Query(fmt.Sprintf("SELECT * FROM %s.syncers", remoteDBName))
	if err != nil {
		return nil, xerror.Wrap(err, xerror.DB, "postgresql: get jobs data failed.")
	}

	syncerData := make([]string, 0)
	for syncerRows.Next() {
		var hostInfo string
		var timestamp int64
		if err := syncerRows.Scan(&hostInfo, &timestamp); err != nil {
			return nil, xerror.Wrap(err, xerror.DB, "postgresql: scan syncers row failed.")
		}
		syncerData = append(syncerData, fmt.Sprintf("%s, %d", hostInfo, timestamp))
	}
	ans["syncers"] = syncerData
	syncerRows.Close()

	return ans, nil
}
