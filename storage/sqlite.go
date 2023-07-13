package storage

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
)

type SQLiteDB struct {
	db *sql.DB
}

func NewSQLiteDB(dbPath string) (DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, errors.Wrapf(err, "open sqlite3 path %s failed", dbPath)
	}

	// create table info && progress, if not exists
	// all is tuple (string, string)
	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS jobs (job_name TEXT PRIMARY KEY, job_info TEXT)"); err != nil {
		return nil, errors.Wrapf(err, "create table jobs failed")
	}

	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS progresses (job_name TEXT PRIMARY KEY, progress TEXT)"); err != nil {
		return nil, errors.Wrapf(err, "create table progresses failed")
	}

	return &SQLiteDB{db: db}, nil
}

func (s *SQLiteDB) AddJob(jobName string, jobInfo string) error {
	// check job name exists, if exists, return error
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE job_name = ?", jobName).Scan(&count); err != nil {
		return errors.Wrapf(err, "query job name %s failed", jobName)
	}

	if count > 0 {
		return ErrJobExists
	}

	// insert job info
	if _, err := s.db.Exec("INSERT INTO jobs (job_name, job_info) VALUES (?, ?)", jobName, jobInfo); err != nil {
		return errors.Wrapf(err, "insert job name %s failed", jobName)
	} else {
		return nil
	}
}

// Update Job
func (s *SQLiteDB) UpdateJob(jobName string, jobInfo string) error {
	// check job name exists, if not exists, return error
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE job_name = ?", jobName).Scan(&count); err != nil {
		return errors.Wrapf(err, "query job name %s failed", jobName)
	}

	if count == 0 {
		return ErrJobNotExists
	}

	// update job info
	if _, err := s.db.Exec("UPDATE jobs SET job_info = ? WHERE job_name = ?", jobInfo, jobName); err != nil {
		return errors.Wrapf(err, "update job name %s failed", jobName)
	} else {
		return nil
	}
}

func (s *SQLiteDB) IsJobExist(jobName string) (bool, error) {
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE job_name = ?", jobName).Scan(&count); err != nil {
		return false, errors.Wrapf(err, "query job name %s failed", jobName)
	} else {
		return count > 0, nil
	}
}

func (s *SQLiteDB) GetAllJobs() (map[string]string, error) {
	rows, err := s.db.Query("SELECT job_name, job_info FROM jobs")
	if err != nil {
		return nil, errors.Wrapf(err, "query all jobs failed")
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var jobName, jobInfo string
		err = rows.Scan(&jobName, &jobInfo)
		if err != nil {
			return nil, errors.Wrapf(err, "scan job info failed")
		}
		result[jobName] = jobInfo
	}
	return result, nil
}

func (s *SQLiteDB) UpdateProgress(jobName string, progress string) error {
	// check job name exists, if not exists, return error
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE job_name = ?", jobName).Scan(&count); err != nil {
		return errors.Wrapf(err, "query job name %s failed", jobName)
	}

	if count == 0 {
		return ErrJobNotExists
	}

	// update progress
	if _, err := s.db.Exec("UPDATE progresses SET progress = ? WHERE job_name = ?", progress, jobName); err != nil {
		return errors.Wrapf(err, "update progress failed")
	} else {
		return nil
	}
}

func (s *SQLiteDB) IsProgressExist(jobName string) (bool, error) {
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM progresses WHERE job_name = ?", jobName).Scan(&count); err != nil {
		return false, errors.Wrapf(err, "query job name %s failed", jobName)
	}
	return count > 0, nil
}

func (s *SQLiteDB) GetProgress(jobName string) (string, error) {
	var progress string
	if err := s.db.QueryRow("SELECT progress FROM progresses WHERE job_name = ?", jobName).Scan(&progress); err != nil {
		return "", errors.Wrapf(err, "query progress failed")
	}
	return progress, nil
}
