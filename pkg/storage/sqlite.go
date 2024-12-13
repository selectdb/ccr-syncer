package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

type SQLiteDB struct {
	db *sql.DB
}

func NewSQLiteDB(dbPath string) (DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, xerror.Wrapf(err, xerror.DB, "sqlite: open sqlite3 path %s failed", dbPath)
	}

	SetDBOptions(db)

	// create table info && progress, if not exists
	// all is tuple (string, string)
	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS jobs (job_name TEXT PRIMARY KEY, job_info TEXT, belong_to TEXT)"); err != nil {
		return nil, xerror.Wrap(err, xerror.DB, "sqlite: create table jobs failed")
	}

	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS progresses (job_name TEXT PRIMARY KEY, progress TEXT)"); err != nil {
		return nil, xerror.Wrap(err, xerror.DB, "sqlite: create table progresses failed")
	}

	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS syncers (host_info TEXT PRIMARY KEY, timestamp INTEGER)"); err != nil {
		return nil, xerror.Wrap(err, xerror.DB, "sqlite: create table syncers failed")
	}

	return &SQLiteDB{db: db}, nil
}

func (s *SQLiteDB) AddJob(jobName string, jobInfo string, hostInfo string) error {
	// check job name exists, if exists, return error
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE job_name = ?", jobName).Scan(&count); err != nil {
		return xerror.Wrapf(err, xerror.DB, "sqlite: query job name %s failed", jobName)
	}

	if count > 0 {
		return ErrJobExists
	}

	// insert job info
	if _, err := s.db.Exec("INSERT INTO jobs (job_name, job_info, belong_to) VALUES (?, ?, ?)", jobName, jobInfo, hostInfo); err != nil {
		return xerror.Wrapf(err, xerror.DB, "sqlite: insert job name %s failed", jobName)
	} else {
		return nil
	}
}

// Update Job
func (s *SQLiteDB) UpdateJob(jobName string, jobInfo string) error {
	// check job name exists, if not exists, return error
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE job_name = ?", jobName).Scan(&count); err != nil {
		return xerror.Wrapf(err, xerror.DB, "sqlite: query job name %s failed", jobName)
	}

	if count == 0 {
		return ErrJobNotExists
	}

	// update job info
	if _, err := s.db.Exec("UPDATE jobs SET job_info = ? WHERE job_name = ?", jobInfo, jobName); err != nil {
		return xerror.Wrapf(err, xerror.DB, "sqlite: update job name %s failed", jobName)
	} else {
		return nil
	}
}

func (s *SQLiteDB) RemoveJob(jobName string) error {
	txn, err := s.db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  false,
	})
	if err != nil {
		return xerror.Wrapf(err, xerror.DB, "sqlite: remove job begin transaction failed, name: %s", jobName)
	}

	if _, err := txn.Exec("DELETE FROM jobs WHERE job_name = ?", jobName); err != nil {
		if err := txn.Rollback(); err != nil {
			return xerror.Wrapf(err, xerror.DB, "sqlite: remove job failed, name: %s, and rollback failed too", jobName)
		}
		return xerror.Wrapf(err, xerror.DB, "sqlite: remove job failed, name: %s", jobName)
	}

	if _, err := txn.Exec("DELETE FROM progresses WHERE job_name = ?", jobName); err != nil {
		if err := txn.Rollback(); err != nil {
			return xerror.Wrapf(err, xerror.DB, "sqlite: remove progresses failed, name: %s, and rollback failed too", jobName)
		}
		return xerror.Wrapf(err, xerror.DB, "sqlite: remove progresses failed, name: %s", jobName)
	}

	if err := txn.Commit(); err != nil {
		return xerror.Wrap(err, xerror.DB, "sqlite: remove job txn commit failed.")
	}

	return nil
}

func (s *SQLiteDB) IsJobExist(jobName string) (bool, error) {
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE job_name = ?", jobName).Scan(&count); err != nil {
		return false, xerror.Wrapf(err, xerror.DB, "sqlite: query job name %s failed", jobName)
	} else {
		return count > 0, nil
	}
}

func (s *SQLiteDB) GetJobInfo(jobName string) (string, error) {
	var jobInfo string
	if err := s.db.QueryRow("SELECT job_info FROM jobs WHERE job_name = ?", jobName).Scan(&jobInfo); err != nil {
		return "", xerror.Wrapf(err, xerror.DB, "sqlite: get job failed, name: %s", jobName)
	}
	return jobInfo, nil
}

func (s *SQLiteDB) GetJobBelong(jobName string) (string, error) {
	var belong string
	if err := s.db.QueryRow("SELECT belong_to FROM jobs WHERE job_name = ?", jobName).Scan(&belong); err != nil {
		return "", xerror.Wrapf(err, xerror.DB, "sqlite: get job belong failed, name: %s", jobName)
	}
	return belong, nil
}

func (s *SQLiteDB) UpdateProgress(jobName string, progress string) error {
	if result, err := s.db.Exec("INSERT INTO progresses VALUES (?, ?) ON CONFLICT (job_name) DO UPDATE SET progress = ?", jobName, progress, progress); err != nil {
		return xerror.Wrap(err, xerror.DB, "sqlite: update progress failed")
	} else if rowNum, err := result.RowsAffected(); err != nil {
		return xerror.Wrap(err, xerror.DB, "sqlite: update progress get affected rows failed")
	} else if rowNum != 1 {
		return xerror.Wrapf(err, xerror.DB, "sqlite: update progress affected rows error, rows: %d", rowNum)
	}

	return nil
}

func (s *SQLiteDB) IsProgressExist(jobName string) (bool, error) {
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM progresses WHERE job_name = ?", jobName).Scan(&count); err != nil {
		return false, xerror.Wrapf(err, xerror.DB, "sqlite: query job name %s failed", jobName)
	}
	return count > 0, nil
}

func (s *SQLiteDB) GetProgress(jobName string) (string, error) {
	var progress string
	if err := s.db.QueryRow("SELECT progress FROM progresses WHERE job_name = ?", jobName).Scan(&progress); err != nil {
		return "", xerror.Wrap(err, xerror.DB, "sqlite: query progress failed")
	}
	return progress, nil
}

func (s *SQLiteDB) AddSyncer(hostInfo string) error {
	timestamp := time.Now().UnixNano()
	if result, err := s.db.Exec("INSERT INTO syncers VALUES (?, ?) ON CONFLICT (host_info) DO UPDATE SET timestamp = ?", hostInfo, timestamp, timestamp); err != nil {
		return xerror.Wrap(err, xerror.DB, "sqlite: add syncer failed")
	} else if rowNum, err := result.RowsAffected(); err != nil {
		return xerror.Wrap(err, xerror.DB, "sqlite: add syncer get affected rows failed")
	} else if rowNum != 1 {
		return xerror.Wrapf(err, xerror.DB, "sqlite: add syncer affected rows error, rows: %d", rowNum)
	}

	return nil
}

func (s *SQLiteDB) RefreshSyncer(hostInfo string, lastStamp int64) (int64, error) {
	nowTime := time.Now().UnixNano()
	result, err := s.db.Exec("UPDATE syncers SET timestamp = ? WHERE host_info = ? AND timestamp = ?", nowTime, hostInfo, lastStamp)
	if err != nil {
		return -1, xerror.Wrap(err, xerror.DB, "sqlite: refresh syncer failed.")
	}

	if rowNum, err := result.RowsAffected(); err != nil {
		return -1, xerror.Wrap(err, xerror.DB, "sqlite: get RowsAffected failed.")
	} else if rowNum != 1 {
		return -1, nil
	} else {
		return nowTime, nil
	}
}

func (s *SQLiteDB) GetStampAndJobs(hostInfo string) (int64, []string, error) {
	txn, err := s.db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return -1, nil, xerror.Wrap(err, xerror.DB, "sqlite: begin IMMEDIATE transaction failed.")
	}

	var timestamp int64
	if err := txn.QueryRow("SELECT timestamp FROM syncers WHERE host_info = ?", hostInfo).Scan(&timestamp); err != nil {
		return -1, nil, xerror.Wrap(err, xerror.DB, "sqlite: get stamp failed.")
	}

	jobs := make([]string, 0)
	rows, err := s.db.Query("SELECT job_name FROM jobs WHERE belong_to = ?", hostInfo)
	if err != nil {
		return -1, nil, xerror.Wrap(err, xerror.DB, "sqlite: get job_nums failed.")
	}
	defer rows.Close()

	for rows.Next() {
		var jobName string
		if err := rows.Scan(&jobName); err != nil {
			return -1, nil, xerror.Wrap(err, xerror.DB, "sqlite: scan job_name failed.")
		}
		jobs = append(jobs, jobName)
	}

	if err := txn.Commit(); err != nil {
		return -1, nil, xerror.Wrap(err, xerror.DB, "sqlite: get jobs & stamp txn commit failed.")
	}

	return timestamp, jobs, nil
}

func (s *SQLiteDB) GetDeadSyncers(expiredTime int64) ([]string, error) {
	row, err := s.db.Query("SELECT host_info FROM syncers WHERE timestamp < ?", expiredTime)
	if err != nil {
		return nil, xerror.Wrap(err, xerror.DB, "sqlite: get orphan job info failed.")
	}
	defer row.Close()
	deadSyncers := make([]string, 0)
	for row.Next() {
		var hostInfo string
		if err := row.Scan(&hostInfo); err != nil {
			return nil, xerror.Wrap(err, xerror.DB, "sqlite: scan host_info and jobs failed")
		}
		deadSyncers = append(deadSyncers, hostInfo)
	}
	return deadSyncers, nil
}

func (s *SQLiteDB) getOrphanJobs(txn *sql.Tx, syncers []string) ([]string, error) {
	orphanJobs := make([]string, 0)
	for _, deadSyncer := range syncers {
		rows, err := txn.Query("SELECT job_name FROM jobs WHERE belong_to = ?", deadSyncer)
		if err != nil {
			return nil, xerror.Wrap(err, xerror.DB, "sqlite: get orphan jobs failed.")
		}

		for rows.Next() {
			var jobName string
			if err := rows.Scan(&jobName); err != nil {
				return nil, xerror.Wrap(err, xerror.DB, "sqlite: scan orphan job name failed.")
			}
			orphanJobs = append(orphanJobs, jobName)
		}
		rows.Close()

		if _, err := txn.Exec("DELETE FROM syncers WHERE host_info = ?", deadSyncer); err != nil {
			return nil, xerror.Wrapf(err, xerror.DB, "sqlite: delete dead syncer failed, name: %s", deadSyncer)
		}
	}
	return orphanJobs, nil
}

func (s *SQLiteDB) getLoadInfo(txn *sql.Tx) (LoadSlice, int, error) {
	load := make(LoadSlice, 0)
	sumLoad := 0
	host_rows, err := txn.Query("SELECT host_info FROM syncers")
	if err != nil {
		return nil, -1, xerror.Wrap(err, xerror.DB, "sqlite: get all syncers failed.")
	}
	defer host_rows.Close()
	for host_rows.Next() {
		loadInfo := LoadInfo{AddedLoad: 0}
		if err := host_rows.Scan(&loadInfo.HostInfo); err != nil {
			return nil, -1, xerror.Wrap(err, xerror.DB, "sqlite: scan load info failed.")
		}
		if err := txn.QueryRow("SELECT COUNT(*) FROM jobs WHERE belong_to = ?", loadInfo.HostInfo).Scan(&loadInfo.NowLoad); err != nil {
			return nil, -1, xerror.Wrapf(err, xerror.DB, "sqlite: get syncer %s load failed.", loadInfo.HostInfo)
		}
		sumLoad += loadInfo.NowLoad
		load = append(load, loadInfo)
	}

	return load, sumLoad, nil
}

func (s *SQLiteDB) dispatchJobs(txn *sql.Tx, hostInfo string, additionalJobs []string) error {
	for _, jobName := range additionalJobs {
		if _, err := txn.Exec("UPDATE jobs SET belong_to = ? WHERE job_name = ?", hostInfo, jobName); err != nil {
			return xerror.Wrapf(err, xerror.DB, "sqlite: update job belong_to failed, name: %s", jobName)
		}
	}
	if _, err := txn.Exec("UPDATE syncers SET timestamp = ? WHERE host_info = ?", time.Now().UnixNano(), hostInfo); err != nil {
		return xerror.Wrapf(err, xerror.DB, "sqlite: update syncer timestamp failed, host: %s", hostInfo)
	}
	return nil
}

func (s *SQLiteDB) RebalanceLoadFromDeadSyncers(syncers []string) error {
	txn, err := s.db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  false,
	})
	if err != nil {
		return xerror.Wrap(err, xerror.DB, "sqlite: rebalance load begin txn failed")
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
				return xerror.Wrap(err, xerror.DB, "sqlite: rebalance rollback failed.")
			}
			return err
		}
		additionalLoad = beginIdx
	}

	if err := txn.Commit(); err != nil {
		return xerror.Wrap(err, xerror.DB, "sqlite: rebalance txn commit failed.")
	}

	return nil
}

func (s *SQLiteDB) GetAllData() (map[string][]string, error) {
	ans := make(map[string][]string)

	jobRows, err := s.db.Query("SELECT job_name, belong_to FROM jobs")
	if err != nil {
		return nil, xerror.Wrap(err, xerror.DB, "sqlite: get jobs data failed.")
	}
	jobData := make([]string, 0)
	for jobRows.Next() {
		var jobName string
		var belong_to string
		if err := jobRows.Scan(&jobName, &belong_to); err != nil {
			return nil, xerror.Wrap(err, xerror.DB, "sqlite: scan jobs row failed.")
		}
		jobData = append(jobData, fmt.Sprintf("%s, %s", jobName, belong_to))
	}
	ans["jobs"] = jobData
	jobRows.Close()

	syncerRows, err := s.db.Query("SELECT * FROM syncers")
	if err != nil {
		return nil, xerror.Wrap(err, xerror.DB, "sqlite: get jobs data failed.")
	}
	syncerData := make([]string, 0)
	for syncerRows.Next() {
		var hostInfo string
		var timestamp int64
		if err := syncerRows.Scan(&hostInfo, &timestamp); err != nil {
			return nil, xerror.Wrap(err, xerror.DB, "sqlite: scan syncers row failed.")
		}
		syncerData = append(syncerData, fmt.Sprintf("%s, %d", hostInfo, timestamp))
	}
	ans["syncers"] = syncerData
	syncerRows.Close()

	return ans, nil
}
