package storage

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

const (
	defaultMaxAllowedPacket = 1024 * 1024 * 1024
)

type MysqlDB struct {
	db *sql.DB
}

func NewMysqlDB(host string, port int, user string, password string) (DB, error) {
	dbForDDL, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/?maxAllowedPacket=%d", user, password, host, port, maxAllowedPacket))
	if err != nil {
		return nil, xerror.Wrapf(err, xerror.DB, "mysql: open %s@tcp(%s:%s) failed", user, host, password)
	}

	if _, err := dbForDDL.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", remoteDBName)); err != nil {
		return nil, xerror.Wrapf(err, xerror.DB, "mysql: create database %s failed", remoteDBName)
	}
	dbForDDL.Close()

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?maxAllowedPacket=%d", user, password, host, port, remoteDBName, maxAllowedPacket))
	if err != nil {
		return nil, xerror.Wrapf(err, xerror.DB, "mysql: open mysql in db %s@tcp(%s:%d)/%s failed", user, host, port, remoteDBName)
	}

	SetDBOptions(db)

	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS jobs (`job_name` VARCHAR(512) PRIMARY KEY, `job_info` TEXT, `belong_to` VARCHAR(96))"); err != nil {
		return nil, xerror.Wrap(err, xerror.DB, "mysql: create table jobs failed")
	}

	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS progresses (`job_name` VARCHAR(512) PRIMARY KEY, `progress` LONGTEXT)"); err != nil {
		return nil, xerror.Wrap(err, xerror.DB, "mysql: create table progresses failed")
	}

	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS syncers (`host_info` VARCHAR(96) PRIMARY KEY, `timestamp` BIGINT)"); err != nil {
		return nil, xerror.Wrap(err, xerror.DB, "mysql: create table syncers failed")
	}

	return &MysqlDB{db: db}, nil
}

func (s *MysqlDB) AddJob(jobName string, jobInfo string, hostInfo string) error {
	// check job name exists, if exists, return error
	var count int
	if err := s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM jobs WHERE job_name = '%s'", jobName)).Scan(&count); err != nil {
		return xerror.Wrapf(err, xerror.DB, "mysql: query job name %s failed", jobName)
	}

	if count > 0 {
		return ErrJobExists
	}

	// insert job info
	insertSql := fmt.Sprintf("INSERT INTO jobs (job_name, job_info, belong_to) VALUES ('%s', '%s', '%s')", jobName, jobInfo, hostInfo)
	if _, err := s.db.Exec(insertSql); err != nil {
		return xerror.Wrapf(err, xerror.DB, "mysql: insert job name %s failed", jobName)
	} else {
		return nil
	}
}

// Update Job
func (s *MysqlDB) UpdateJob(jobName string, jobInfo string) error {
	// check job name exists, if not exists, return error
	var count int
	if err := s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM jobs WHERE job_name = '%s'", jobName)).Scan(&count); err != nil {
		return xerror.Wrapf(err, xerror.DB, "mysql: query job name %s failed", jobName)
	}

	if count == 0 {
		return ErrJobNotExists
	}

	// update job info
	if _, err := s.db.Exec(fmt.Sprintf("UPDATE jobs SET job_info = '%s' WHERE job_name = '%s'", jobInfo, jobName)); err != nil {
		return xerror.Wrapf(err, xerror.DB, "mysql: update job name %s failed", jobName)
	} else {
		return nil
	}
}

func (s *MysqlDB) RemoveJob(jobName string) error {
	txn, err := s.db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  false,
	})
	if err != nil {
		return xerror.Wrapf(err, xerror.DB, "mysql: remove job begin transaction failed, name: %s", jobName)
	}

	if _, err := txn.Exec(fmt.Sprintf("DELETE FROM jobs WHERE job_name = '%s'", jobName)); err != nil {
		if err := txn.Rollback(); err != nil {
			return xerror.Wrapf(err, xerror.DB, "mysql: remove job failed, name: %s, and rollback failed too", jobName)
		}
		return xerror.Wrapf(err, xerror.DB, "mysql: remove job failed, name: %s", jobName)
	}

	if _, err := txn.Exec(fmt.Sprintf("DELETE FROM progresses WHERE job_name = '%s'", jobName)); err != nil {
		if err := txn.Rollback(); err != nil {
			return xerror.Wrapf(err, xerror.DB, "mysql: remove progresses failed, name: %s, and rollback failed too", jobName)
		}
		return xerror.Wrapf(err, xerror.DB, "mysql: remove progresses failed, name: %s", jobName)
	}

	if err := txn.Commit(); err != nil {
		return xerror.Wrapf(err, xerror.DB, "mysql: remove job txn commit failed.")
	}

	return nil
}

func (s *MysqlDB) IsJobExist(jobName string) (bool, error) {
	var count int
	if err := s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM jobs WHERE job_name = '%s'", jobName)).Scan(&count); err != nil {
		return false, xerror.Wrapf(err, xerror.DB, "mysql: query job name %s failed", jobName)
	} else {
		return count > 0, nil
	}
}

func (s *MysqlDB) GetJobInfo(jobName string) (string, error) {
	var jobInfo string
	if err := s.db.QueryRow(fmt.Sprintf("SELECT job_info FROM jobs WHERE job_name = '%s'", jobName)).Scan(&jobInfo); err != nil {
		return "", xerror.Wrapf(err, xerror.DB, "mysql: get job failed, name: %s", jobName)
	}
	return jobInfo, nil
}

func (s *MysqlDB) GetJobBelong(jobName string) (string, error) {
	var belong string
	if err := s.db.QueryRow(fmt.Sprintf("SELECT belong_to FROM jobs WHERE job_name = '%s'", jobName)).Scan(&belong); err != nil {
		return "", xerror.Wrapf(err, xerror.DB, "mysql: get job belong failed, name: %s", jobName)
	}
	return belong, nil
}

func (s *MysqlDB) UpdateProgress(jobName string, progress string) error {
	// quoteProgress := strings.ReplaceAll(progress, "\"", "\\\"")
	encodeProgress := base64.StdEncoding.EncodeToString([]byte(progress))
	updateSql := fmt.Sprintf("INSERT INTO progresses (job_name, progress) VALUES ('%s', '%s') ON DUPLICATE KEY UPDATE progress = VALUES(progress)", jobName, encodeProgress)
	if result, err := s.db.Exec(updateSql); err != nil {
		return xerror.Wrapf(err, xerror.DB, "mysql: update progress failed")
	} else if rowNum, err := result.RowsAffected(); err != nil {
		return xerror.Wrapf(err, xerror.DB, "mysql: update progress get affected rows failed")
	} else if rowNum != 1 {
		return xerror.Wrapf(err, xerror.DB, "mysql: update progress affected rows error, rows: %d", rowNum)
	}

	return nil
}

func (s *MysqlDB) IsProgressExist(jobName string) (bool, error) {
	var count int
	if err := s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM progresses WHERE job_name = '%s'", jobName)).Scan(&count); err != nil {
		return false, xerror.Wrapf(err, xerror.DB, "mysql: query job name %s failed", jobName)
	}
	return count > 0, nil
}

func (s *MysqlDB) GetProgress(jobName string) (string, error) {
	var progress string
	if err := s.db.QueryRow(fmt.Sprintf("SELECT progress FROM progresses WHERE job_name = '%s'", jobName)).Scan(&progress); err != nil {
		return "", xerror.Wrapf(err, xerror.DB, "mysql: query progress failed")
	}
	decodeProgress, err := base64.StdEncoding.DecodeString(progress)
	if err != nil {
		return "", xerror.Errorf(xerror.DB, "mysql: base64 decode error")
	}
	return string(decodeProgress), nil
}

func (s *MysqlDB) AddSyncer(hostInfo string) error {
	timestamp := time.Now().UnixNano()
	addSql := fmt.Sprintf("INSERT INTO syncers (host_info, timestamp) VALUES ('%s', %d) ON DUPLICATE KEY UPDATE timestamp = VALUES(timestamp)", hostInfo, timestamp)
	if result, err := s.db.Exec(addSql); err != nil {
		return xerror.Wrapf(err, xerror.DB, "mysql: add syncer failed")
	} else if rowNum, err := result.RowsAffected(); err != nil {
		return xerror.Wrapf(err, xerror.DB, "mysql: add syncer get affected rows failed")
	} else if rowNum != 1 {
		return xerror.Wrapf(err, xerror.DB, "mysql: add syncer affected rows error, rows: %d", rowNum)
	}

	return nil
}

func (s *MysqlDB) RefreshSyncer(hostInfo string, lastStamp int64) (int64, error) {
	nowTime := time.Now().UnixNano()
	refreshSql := fmt.Sprintf("UPDATE syncers SET timestamp = %d WHERE host_info = '%s' AND timestamp = %d", nowTime, hostInfo, lastStamp)
	result, err := s.db.Exec(refreshSql)
	if err != nil {
		return -1, xerror.Wrapf(err, xerror.DB, "mysql: refresh syncer failed.")
	}

	if rowNum, err := result.RowsAffected(); err != nil {
		return -1, xerror.Wrapf(err, xerror.DB, "mysql: get RowsAffected failed.")
	} else if rowNum != 1 {
		return -1, nil
	} else {
		return nowTime, nil
	}
}

func (s *MysqlDB) GetStampAndJobs(hostInfo string) (int64, []string, error) {
	txn, err := s.db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return -1, nil, xerror.Wrapf(err, xerror.DB, "mysql: begin IMMEDIATE transaction failed.")
	}

	var timestamp int64
	if err := txn.QueryRow(fmt.Sprintf("SELECT timestamp FROM syncers WHERE host_info = '%s'", hostInfo)).Scan(&timestamp); err != nil {
		return -1, nil, xerror.Wrapf(err, xerror.DB, "mysql: get stamp failed.")
	}

	jobs := make([]string, 0)
	rows, err := s.db.Query(fmt.Sprintf("SELECT job_name FROM jobs WHERE belong_to = '%s'", hostInfo))
	if err != nil {
		return -1, nil, xerror.Wrapf(err, xerror.DB, "mysql: get job_nums failed.")
	}
	defer rows.Close()

	for rows.Next() {
		var jobName string
		if err := rows.Scan(&jobName); err != nil {
			return -1, nil, xerror.Wrapf(err, xerror.DB, "mysql: scan job_name failed.")
		}
		jobs = append(jobs, jobName)
	}

	if err := txn.Commit(); err != nil {
		return -1, nil, xerror.Wrapf(err, xerror.DB, "mysql: get jobs & stamp txn commit failed.")
	}

	return timestamp, jobs, nil
}

func (s *MysqlDB) GetDeadSyncers(expiredTime int64) ([]string, error) {
	row, err := s.db.Query(fmt.Sprintf("SELECT host_info FROM syncers WHERE timestamp < %d", expiredTime))
	if err != nil {
		return nil, xerror.Wrapf(err, xerror.DB, "mysql: get orphan job info failed.")
	}
	defer row.Close()
	deadSyncers := make([]string, 0)
	for row.Next() {
		var hostInfo string
		if err := row.Scan(&hostInfo); err != nil {
			return nil, xerror.Wrapf(err, xerror.DB, "mysql: scan host_info and jobs failed")
		}
		deadSyncers = append(deadSyncers, hostInfo)
	}
	return deadSyncers, nil
}

func (s *MysqlDB) getOrphanJobs(txn *sql.Tx, syncers []string) ([]string, error) {
	orphanJobs := make([]string, 0)
	for _, deadSyncer := range syncers {
		rows, err := txn.Query(fmt.Sprintf("SELECT job_name FROM jobs WHERE belong_to = '%s'", deadSyncer))
		if err != nil {
			return nil, xerror.Wrapf(err, xerror.DB, "mysql: get orphan jobs failed.")
		}

		for rows.Next() {
			var jobName string
			if err := rows.Scan(&jobName); err != nil {
				return nil, xerror.Wrapf(err, xerror.DB, "mysql: scan orphan job name failed.")
			}
			orphanJobs = append(orphanJobs, jobName)
		}
		rows.Close()

		if _, err := txn.Exec(fmt.Sprintf("DELETE FROM syncers WHERE host_info = '%s'", deadSyncer)); err != nil {
			return nil, xerror.Wrapf(err, xerror.DB, "mysql: delete dead syncer failed, name: %s", deadSyncer)
		}
	}
	return orphanJobs, nil
}

func (s *MysqlDB) getLoadInfo(txn *sql.Tx) (LoadSlice, int, error) {
	load := make(LoadSlice, 0)
	sumLoad := 0
	host_rows, err := txn.Query("SELECT host_info FROM syncers")
	if err != nil {
		return nil, -1, xerror.Wrapf(err, xerror.DB, "mysql: get all syncers failed.")
	}
	for host_rows.Next() {
		loadInfo := LoadInfo{AddedLoad: 0}
		if err := host_rows.Scan(&loadInfo.HostInfo); err != nil {
			return nil, -1, xerror.Wrapf(err, xerror.DB, "mysql: scan load info failed.")
		}
		load = append(load, loadInfo)
	}
	host_rows.Close()

	for i := range load {
		if err := txn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM jobs WHERE belong_to = '%s'", load[i].HostInfo)).Scan(&load[i].NowLoad); err != nil {
			return nil, -1, xerror.Wrapf(err, xerror.DB, "mysql: get syncer %s load failed.", load[i].HostInfo)
		}
		sumLoad += load[i].NowLoad
	}

	return load, sumLoad, nil
}

func (s *MysqlDB) dispatchJobs(txn *sql.Tx, hostInfo string, additionalJobs []string) error {
	for _, jobName := range additionalJobs {
		if _, err := txn.Exec(fmt.Sprintf("UPDATE jobs SET belong_to = '%s' WHERE job_name = '%s'", hostInfo, jobName)); err != nil {
			return xerror.Wrapf(err, xerror.DB, "mysql: update job belong_to failed, name: %s", jobName)
		}
	}
	if _, err := txn.Exec(fmt.Sprintf("UPDATE syncers SET timestamp = %d WHERE host_info = '%s'", time.Now().UnixNano(), hostInfo)); err != nil {
		return xerror.Wrapf(err, xerror.DB, "mysql: update syncer timestamp failed, host: %s", hostInfo)
	}
	return nil
}

func (s *MysqlDB) RebalanceLoadFromDeadSyncers(syncers []string) error {
	txn, err := s.db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  false,
	})
	if err != nil {
		return xerror.Wrap(err, xerror.DB, "mysql: rebalance load begin txn failed")
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
				return xerror.Wrap(err, xerror.DB, "mysql: rebalance rollback failed.")
			}
			return err
		}
		additionalLoad = beginIdx
	}

	if err := txn.Commit(); err != nil {
		return xerror.Wrap(err, xerror.DB, "mysql: rebalance txn commit failed.")
	}

	return nil
}

func (s *MysqlDB) GetAllData() (map[string][]string, error) {
	ans := make(map[string][]string)

	jobRows, err := s.db.Query("SELECT job_name, belong_to FROM jobs")
	if err != nil {
		return nil, xerror.Wrap(err, xerror.DB, "mysql: get jobs data failed.")
	}
	jobData := make([]string, 0)
	for jobRows.Next() {
		var jobName string
		var belongTo string
		if err := jobRows.Scan(&jobName, &belongTo); err != nil {
			return nil, xerror.Wrap(err, xerror.DB, "mysql: scan jobs row failed.")
		}
		jobData = append(jobData, fmt.Sprintf("%s, %s", jobName, belongTo))
	}
	ans["jobs"] = jobData
	jobRows.Close()

	syncerRows, err := s.db.Query("SELECT * FROM syncers")
	if err != nil {
		return nil, xerror.Wrap(err, xerror.DB, "mysql: get jobs data failed.")
	}

	syncerData := make([]string, 0)
	for syncerRows.Next() {
		var hostInfo string
		var timestamp int64
		if err := syncerRows.Scan(&hostInfo, &timestamp); err != nil {
			return nil, xerror.Wrap(err, xerror.DB, "mysql: scan syncers row failed.")
		}
		syncerData = append(syncerData, fmt.Sprintf("%s, %d", hostInfo, timestamp))
	}
	ans["syncers"] = syncerData
	syncerRows.Close()

	return ans, nil
}
