package storage

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteDB struct {
	db *sql.DB
}

func NewSQLiteDB(dbPath string) (DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	// create table info && progress, if not exists
	// all is tuple (string, string)
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS info (job_name TEXT PRIMARY KEY, job_info TEXT)")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS progress (job_name TEXT PRIMARY KEY, progress TEXT)")
	if err != nil {
		return nil, err
	}

	return &SQLiteDB{db: db}, nil
}

func (s *SQLiteDB) AddJobInfo(jobName string, jobInfo string) error {
	// check job name exists, if exists, return error
	var count int
	err := s.db.QueryRow("SELECT COUNT(*) FROM info WHERE job_name = ?", jobName).Scan(&count)
	if err != nil {
		return err
	}
	if count > 0 {
		return ErrJobExists
	}

	// insert job info
	_, err = s.db.Exec("INSERT INTO info (job_name, job_info) VALUES (?, ?)", jobName, jobInfo)
	return err
}

func (s *SQLiteDB) IsJobExist(jobName string) (bool, error) {
	var count int
	err := s.db.QueryRow("SELECT COUNT(*) FROM info WHERE job_name = ?", jobName).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *SQLiteDB) GetAllJobInfo() (map[string]string, error) {
	rows, err := s.db.Query("SELECT job_name, job_info FROM info")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var jobName, jobInfo string
		err = rows.Scan(&jobName, &jobInfo)
		if err != nil {
			return nil, err
		}
		result[jobName] = jobInfo
	}
	return result, nil
}

func (s *SQLiteDB) UpdateProgress(jobName string, progress string) error {
	// check job name exists, if not exists, return error
	var count int
	err := s.db.QueryRow("SELECT COUNT(*) FROM info WHERE job_name = ?", jobName).Scan(&count)
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrJobNotExists
	}

	// update progress
	_, err = s.db.Exec("UPDATE progress SET progress = ? WHERE job_name = ?", progress, jobName)
	return err
}

func (s *SQLiteDB) IsProgressExist(jobName string) (bool, error) {
	var count int
	err := s.db.QueryRow("SELECT COUNT(*) FROM progress WHERE job_name = ?", jobName).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *SQLiteDB) GetProgress(jobName string) (string, error) {
	var progress string
	err := s.db.QueryRow("SELECT progress FROM progress WHERE job_name = ?", jobName).Scan(&progress)
	if err != nil {
		return "", err
	}
	return progress, nil
}
