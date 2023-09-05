package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

type MysqlDB struct {
	db *sql.DB
}

func NewMysqlDB(host string, port int, user string, password string) (DB, error) {
	dbForDDL, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/", user, password, host, port))
	if err != nil {
		return nil, errors.Wrapf(err, "open mysql %s@tcp(%s:%s) failed", user, host, password)
	}

	if _, err := dbForDDL.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", remoteDBName)); err != nil {
		return nil, errors.Wrapf(err, "create database %s failed", remoteDBName)
	}
	dbForDDL.Close()

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, remoteDBName))
	if err != nil {
		return nil, errors.Wrapf(err, "open mysql in db %s@tcp(%s:%d)/%s failed", user, host, port, remoteDBName)
	}

	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS jobs (`job_name` VARCHAR(128) PRIMARY KEY, `job_info` VARCHAR(4096), `belong_to` VARCHAR(128))"); err != nil {
		return nil, errors.Wrapf(err, "create table jobs failed")
	}

	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS progresses (`job_name` VARCHAR(128) PRIMARY KEY, `progress` VARCHAR(4096))"); err != nil {
		return nil, errors.Wrapf(err, "create table progresses failed")
	}

	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS syncers (`host_info` VARCHAR(128) PRIMARY KEY, `timestamp` BIGINT)"); err != nil {
		return nil, errors.Wrapf(err, "create table syncers failed")
	}

	return &MysqlDB{db: db}, nil
}

func (s *MysqlDB) AddJob(jobName string, jobInfo string, hostInfo string) error {
	// check job name exists, if exists, return error
	var count int
	if err := s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM jobs WHERE job_name = '%s'", jobName)).Scan(&count); err != nil {
		return errors.Wrapf(err, "query job name %s failed", jobName)
	}

	if count > 0 {
		return ErrJobExists
	}

	// insert job info
	insertSql := fmt.Sprintf("INSERT INTO jobs (job_name, job_info, belong_to) VALUES ('%s', '%s', '%s')", jobName, jobInfo, hostInfo)
	if _, err := s.db.Exec(insertSql); err != nil {
		return errors.Wrapf(err, "insert job name %s failed", jobName)
	} else {
		return nil
	}
}

// Update Job
func (s *MysqlDB) UpdateJob(jobName string, jobInfo string) error {
	// check job name exists, if not exists, return error
	var count int
	if err := s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM jobs WHERE job_name = '%s'", jobName)).Scan(&count); err != nil {
		return errors.Wrapf(err, "query job name %s failed", jobName)
	}

	if count == 0 {
		return ErrJobNotExists
	}

	// update job info
	if _, err := s.db.Exec(fmt.Sprintf("UPDATE jobs SET job_info = %s WHERE job_name = '%s'", jobInfo, jobName)); err != nil {
		return errors.Wrapf(err, "update job name %s failed", jobName)
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
		return errors.Wrapf(err, "remove job begin transaction failed, name: %s", jobName)
	}

	if _, err := txn.Exec(fmt.Sprintf("DELETE FROM jobs WHERE job_name = '%s'", jobName)); err != nil {
		if err := txn.Rollback(); err != nil {
			return errors.Wrapf(err, "remove job failed, name: %s, and rollback failed too", jobName)
		}
		return errors.Wrapf(err, "remove job failed, name: %s", jobName)
	}

	if _, err := txn.Exec(fmt.Sprintf("DELETE FROM progresses WHERE job_name = '%s'", jobName)); err != nil {
		if err := txn.Rollback(); err != nil {
			return errors.Wrapf(err, "remove progresses failed, name: %s, and rollback failed too", jobName)
		}
		return errors.Wrapf(err, "remove progresses failed, name: %s", jobName)
	}

	if err := txn.Commit(); err != nil {
		return errors.Wrapf(err, "remove job txn commit failed.")
	}

	return nil
}

func (s *MysqlDB) IsJobExist(jobName string) (bool, error) {
	var count int
	if err := s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM jobs WHERE job_name = '%s'", jobName)).Scan(&count); err != nil {
		return false, errors.Wrapf(err, "query job name %s failed", jobName)
	} else {
		return count > 0, nil
	}
}

func (s *MysqlDB) GetJobInfo(jobName string) (string, error) {
	var jobInfo string
	if err := s.db.QueryRow(fmt.Sprintf("SELECT job_info FROM jobs WHERE job_name = '%s'", jobName)).Scan(&jobInfo); err != nil {
		return "", errors.Wrapf(err, "get job failed, name: %s", jobName)
	}
	return jobInfo, nil
}

func (s *MysqlDB) GetJobBelong(jobName string) (string, error) {
	var belong string
	if err := s.db.QueryRow(fmt.Sprintf("SELECT belong_to FROM jobs WHERE job_name = '%s'", jobName)).Scan(&belong); err != nil {
		return "", errors.Wrapf(err, "get job belong failed, name: %s", jobName)
	}
	return belong, nil
}

func (s *MysqlDB) UpdateProgress(jobName string, progress string) error {
	updateSql := fmt.Sprintf("INSERT INTO progresses (job_name, progress) VALUES ('%s', '%s') ON DUPLICATE KEY UPDATE progress = '%s'", jobName, progress, progress)
	if result, err := s.db.Exec(updateSql); err != nil {
		return errors.Wrapf(err, "update progress failed")
	} else if rowNum, err := result.RowsAffected(); err != nil {
		return errors.Wrapf(err, "update progress get affected rows failed")
	} else if rowNum != 1 {
		return errors.Wrapf(err, "update progress affected rows error, rows: %d", rowNum)
	}

	return nil
}

func (s *MysqlDB) IsProgressExist(jobName string) (bool, error) {
	var count int
	if err := s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM progresses WHERE job_name = '%s'", jobName)).Scan(&count); err != nil {
		return false, errors.Wrapf(err, "query job name %s failed", jobName)
	}
	return count > 0, nil
}

func (s *MysqlDB) GetProgress(jobName string) (string, error) {
	var progress string
	if err := s.db.QueryRow(fmt.Sprintf("SELECT progress FROM progresses WHERE job_name = '%s'", jobName)).Scan(&progress); err != nil {
		return "", errors.Wrapf(err, "query progress failed")
	}
	return progress, nil
}

func (s *MysqlDB) AddSyncer(hostInfo string) error {
	timestamp := time.Now().UnixNano()
	addSql := fmt.Sprintf("INSERT INTO syncers (host_info, timestamp) VALUES ('%s', %d) ON DUPLICATE KEY UPDATE timestamp = %d", hostInfo, timestamp, timestamp)
	if result, err := s.db.Exec(addSql); err != nil {
		return errors.Wrapf(err, "add syncer failed")
	} else if rowNum, err := result.RowsAffected(); err != nil {
		return errors.Wrapf(err, "add syncer get affected rows failed")
	} else if rowNum != 1 {
		return errors.Wrapf(err, "add syncer affected rows error, rows: %d", rowNum)
	}

	return nil
}

func (s *MysqlDB) RefreshSyncer(hostInfo string, lastStamp int64) (int64, error) {
	nowTime := time.Now().UnixNano()
	refreshSql := fmt.Sprintf("UPDATE syncers SET timestamp = %d WHERE host_info = '%s' AND timestamp = %d", nowTime, hostInfo, lastStamp)
	result, err := s.db.Exec(refreshSql)
	if err != nil {
		return -1, errors.Wrapf(err, "refresh syncer failed.")
	}

	if rowNum, err := result.RowsAffected(); err != nil {
		return -1, errors.Wrapf(err, "get RowsAffected failed.")
	} else if rowNum != 1 {
		return -1, nil
	} else {
		return nowTime, nil
	}
}

func (s *MysqlDB) GetStampAndJobs(hostInfo string) (int64, []string, error) {
	txn, err := s.db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return -1, nil, errors.Wrapf(err, "begin IMMEDIATE transaction failed.")
	}

	var timestamp int64
	if err := txn.QueryRow(fmt.Sprintf("SELECT timestamp FROM syncers WHERE host_info = '%s'", hostInfo)).Scan(&timestamp); err != nil {
		return -1, nil, errors.Wrapf(err, "get stamp failed.")
	}

	jobs := make([]string, 0)
	rows, err := s.db.Query(fmt.Sprintf("SELECT job_name FROM jobs WHERE belong_to = '%s'", hostInfo))
	if err != nil {
		return -1, nil, errors.Wrapf(err, "get job_nums failed.")
	}
	defer rows.Close()

	for rows.Next() {
		var jobName string
		if err := rows.Scan(&jobName); err != nil {
			return -1, nil, errors.Wrapf(err, "scan job_name failed.")
		}
		jobs = append(jobs, jobName)
	}

	if err := txn.Commit(); err != nil {
		return -1, nil, errors.Wrapf(err, "get jobs & stamp txn commit failed.")
	}

	return timestamp, jobs, nil
}

func (s *MysqlDB) GetDeadSyncers(expiredTime int64) ([]string, error) {
	row, err := s.db.Query(fmt.Sprintf("SELECT host_info FROM syncers WHERE timestamp < %d", expiredTime))
	if err != nil {
		return nil, errors.Wrapf(err, "get orphan job info failed.")
	}
	defer row.Close()
	deadSyncers := make([]string, 0)
	for row.Next() {
		var hostInfo string
		if err := row.Scan(&hostInfo); err != nil {
			return nil, errors.Wrapf(err, "scan host_info and jobs failed")
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
			return nil, errors.Wrapf(err, "get orphan jobs failed.")
		}

		for rows.Next() {
			var jobName string
			if err := rows.Scan(&jobName); err != nil {
				return nil, errors.Wrapf(err, "scan orphan job name failed.")
			}
			orphanJobs = append(orphanJobs, jobName)
		}
		rows.Close()

		if _, err := txn.Exec(fmt.Sprintf("DELETE FROM syncers WHERE host_info = '%s'", deadSyncer)); err != nil {
			return nil, errors.Wrapf(err, "delete dead syncer failed, name: %s", deadSyncer)
		}
	}
	return orphanJobs, nil
}

func (s *MysqlDB) getLoadInfo(txn *sql.Tx) (LoadSlice, int, error) {
	load := make(LoadSlice, 0)
	sumLoad := 0
	host_rows, err := txn.Query("SELECT host_info FROM syncers")
	if err != nil {
		return nil, -1, errors.Wrapf(err, "get all syncers failed.")
	}
	defer host_rows.Close()
	for host_rows.Next() {
		loadInfo := LoadInfo{AddedLoad: 0}
		if err := host_rows.Scan(&loadInfo.HostInfo); err != nil {
			return nil, -1, errors.Wrapf(err, "scan load info failed.")
		}
		if err := txn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM jobs WHERE belong_to = '%s'", loadInfo.HostInfo)).Scan(&loadInfo.NowLoad); err != nil {
			return nil, -1, errors.Wrapf(err, "get syncer %s load failed.", loadInfo.HostInfo)
		}
		sumLoad += loadInfo.NowLoad
		load = append(load, loadInfo)
	}

	return load, sumLoad, nil
}

func (s *MysqlDB) dispatchJobs(txn *sql.Tx, hostInfo string, additionalJobs []string) error {
	for _, jobName := range additionalJobs {
		if _, err := txn.Exec(fmt.Sprintf("UPDATE jobs SET belong_to = '%s' WHERE job_name = '%s'", hostInfo, jobName)); err != nil {
			return errors.Wrapf(err, "update job belong_to failed, name: %s", jobName)
		}
	}
	if _, err := txn.Exec(fmt.Sprintf("UPDATE syncers SET timestamp = %d WHERE host_info = '%s'", time.Now().UnixNano(), hostInfo)); err != nil {
		return errors.Wrapf(err, "update syncer timestamp failed, host: %s", hostInfo)
	}
	return nil
}

func (s *MysqlDB) RebalanceLoadFromDeadSyncers(syncers []string) error {
	txn, err := s.db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  false,
	})
	if err != nil {
		return errors.Wrapf(err, "rebalance load begin txn failed")
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

	loadList = RebalanceLoad(additionalLoad, currentLoad, loadList)
	for i := range loadList {
		beginIdx := additionalLoad - loadList[i].AddedLoad
		if err := s.dispatchJobs(txn, loadList[i].HostInfo, orphanJobs[beginIdx:additionalLoad]); err != nil {
			if err := txn.Rollback(); err != nil {
				return errors.Wrapf(err, "rebalance rollback failed.")
			}
			return err
		}
		additionalLoad = beginIdx
	}

	if err := txn.Commit(); err != nil {
		return errors.Wrapf(err, "rebalance txn commit failed.")
	}

	return nil
}

func (s *MysqlDB) GetAllData() (map[string][]string, error) {
	ans := make(map[string][]string)

	jobRows, err := s.db.Query("SELECT job_name, belong_to FROM jobs")
	if err != nil {
		return nil, errors.Wrapf(err, "get jobs data failed.")
	}
	jobData := make([]string, 0)
	for jobRows.Next() {
		var jobName string
		var belong_to string
		if err := jobRows.Scan(&jobName, &belong_to); err != nil {
			return nil, errors.Wrapf(err, "scan jobs row failed.")
		}
		jobData = append(jobData, fmt.Sprintf("%s, %s", jobName, belong_to))
	}
	ans["jobs"] = jobData
	jobRows.Close()

	syncerRows, err := s.db.Query("SELECT * FROM syncers")
	if err != nil {
		return nil, errors.Wrapf(err, "get jobs data failed.")
	}
	syncerData := make([]string, 0)
	for syncerRows.Next() {
		var hostInfo string
		var timestamp int64
		if err := syncerRows.Scan(&hostInfo, &timestamp); err != nil {
			return nil, errors.Wrapf(err, "scan syncers row failed.")
		}
		syncerData = append(syncerData, fmt.Sprintf("%s, %d", hostInfo, timestamp))
	}
	ans["syncers"] = syncerData
	syncerRows.Close()

	return ans, nil
}
