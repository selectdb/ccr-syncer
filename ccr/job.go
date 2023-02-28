package ccr

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/selectdb/ccr_syncer/ccr/base"
	"github.com/selectdb/ccr_syncer/rpc"
	"github.com/selectdb/ccr_syncer/storage"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"

	_ "github.com/go-sql-driver/mysql"
)

type SyncType int

const (
	DBSync    SyncType = 0
	TableSync SyncType = 1

	SYNC_DURATION = time.Second * 5
)

type Job struct {
	SyncType    SyncType      `json:"sync_type"`
	Name        string        `json:"name"`
	Src         base.Spec     `json:"src"`
	srcMeta     *Meta         `json:"-"`
	Dest        base.Spec     `json:"dest"`
	destMeta    *Meta         `json:"-"`
	isFirstSync bool          `json:"-"`
	progress    *JobProgress  `json:"-"`
	db          storage.DB    `json:"-"`
	stop        chan struct{} `json:"-"`
}

// new job
func NewJobFromService(name string, src, dest base.Spec, db storage.DB) (*Job, error) {
	job := &Job{
		Name:        name,
		Src:         src,
		srcMeta:     NewMeta(&src),
		Dest:        dest,
		destMeta:    NewMeta(&dest),
		isFirstSync: true,
		progress:    NewJobProgress(),
		db:          db,
		stop:        make(chan struct{}),
	}

	if err := job.valid(); err != nil {
		return nil, errors.Wrap(err, "job is invalid")
	}
	return job, nil
}

func NewJobFromJson(jsonData string, db storage.DB) (*Job, error) {
	var job Job
	err := json.Unmarshal([]byte(jsonData), &job)
	if err != nil {
		return nil, err
	}
	job.srcMeta = NewMeta(&job.Src)
	job.destMeta = NewMeta(&job.Dest)

	// retry 3 times to check IsProgressExist
	var isProgressExist bool
	for i := 0; i < 3; i++ {
		isProgressExist, err = db.IsProgressExist(job.Name)
		if err != nil {
			log.Error("check progress exist failed", zap.Error(err))
			continue
		}
		break
	}

	if err != nil {
		return nil, err
	}

	if isProgressExist {
		job.isFirstSync = false
	} else {
		job.isFirstSync = true
		job.progress = NewJobProgress()
	}
	job.db = db
	job.stop = make(chan struct{})
	return &job, nil
}

// valid
func (j *Job) valid() error {
	var err error
	if exist, err := j.db.IsJobExist(j.Name); err != nil {
		return errors.Wrap(err, "check job exist failed")
	} else if exist {
		return fmt.Errorf("job %s already exist", j.Name)
	}

	if j.Name == "" {
		return errors.New("name is empty")
	}

	err = j.Src.Valid()
	if err != nil {
		return errors.Wrap(err, "src spec is invalid")
	}

	err = j.Dest.Valid()
	if err != nil {
		return errors.Wrap(err, "dest spec is invalid")
	}

	if (j.Src.Table == "" && j.Dest.Table != "") || (j.Src.Table != "" && j.Dest.Table == "") {
		return errors.New("src/dest are not both db or table sync")
	}

	return nil
}

// create database by dest table spec
func (j *Job) DestCreateDatabase() error {
	log.Trace("create database by dest table spec")

	db, err := j.Dest.Connect()
	if err != nil {
		return nil
	}
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + j.Dest.Database)
	return err
}

// create table by replace remote create table stmt
func (j *Job) DestCreateTable(stmt string) error {
	db, err := j.Dest.Connect()
	if err != nil {
		return nil
	}
	defer db.Close()

	_, err = db.Exec(stmt)
	return err
}

// check database exist by spec
func checkDatabaseExists(spec *base.Spec) (bool, error) {
	log.Trace("check database exist by spec", zap.String("spec", spec.String()))
	db, err := spec.Connect()
	if err != nil {
		return false, err
	}
	defer db.Close()

	rows, err := db.Query("SHOW DATABASES LIKE '" + spec.Database + "'")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	var database string
	for rows.Next() {
		if err := rows.Scan(&database); err != nil {
			return false, err
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}

	return database != "", nil
}

// check table exits in database dir by spec
func checkTableExists(spec *base.Spec) (bool, error) {
	log.Trace("check table exists", zap.String("table", spec.Table))

	db, err := spec.Connect()
	if err != nil {
		return false, err
	}
	defer db.Close()

	rows, err := db.Query("SHOW TABLES FROM " + spec.Database + " LIKE '" + spec.Table + "'")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	var table string
	for rows.Next() {
		if err := rows.Scan(&table); err != nil {
			return false, err
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}

	return table != "", nil
}

// check dest table exits in database dir
func (c *Job) CheckDestTableExists() (bool, error) {
	return checkTableExists(&c.Dest)
}

// check src database && table exists
func (c *Job) CheckSrcTableExists() (bool, error) {
	return checkTableExists(&c.Src)
}

// check dest database exists
func (j *Job) CheckDestDatabaseExists() (bool, error) {
	return checkDatabaseExists(&j.Dest)
}

// check src database exists
func (j *Job) CheckSrcDatabaseExists() (bool, error) {
	return checkDatabaseExists(&j.Src)
}

func (j *Job) RecoverDatabaseSync() error {
	// TODO(Drogon): impl
	return nil
}

// database old data sync
func (j *Job) DatabaseOldDataSync() error {
	// TODO(Drogon): impl
	// Step 1: drop all tables
	err := j.Dest.ClearDB()
	if err != nil {
		return err
	}

	// Step 2: make snapshot

	return nil
}

// database sync
func (j *Job) DatabaseSync() error {
	// TODO(Drogon): impl
	return nil
}

func (j *Job) genExtraInfo() (*base.ExtraInfo, error) {
	meta := j.srcMeta
	masterToken, err := meta.GetMasterToken()
	if err != nil {
		log.Errorf("get master token failed: %v\n", err)
		return nil, err
	}

	backends, err := meta.GetBackends()
	if err != nil {
		log.Errorf("get backends failed: %v\n", err)
		return nil, err
	} else {
		log.Infof("found backends: %v\n", backends)
	}

	beNetworkMap := make(map[int64]base.NetworkAddr)
	for _, backend := range backends {
		log.Infof("backend: %v\n", backend)
		addr := base.NetworkAddr{
			Ip:   backend.Host,
			Port: backend.HttpPort,
		}
		beNetworkMap[backend.Id] = addr
	}

	return &base.ExtraInfo{
		BeNetworkMap: beNetworkMap,
		Token:        masterToken,
	}, nil
}

func (j *Job) firstSync() error {
	// Step 1: Create snapshot
	snapshotName, err := j.Src.CreateSnapshotAndWaitForDone()
	if err != nil {
		return err
	}

	// Step 2: Get snapshot info
	src := &j.Src
	srcRpc, err := rpc.NewThriftRpc(src)
	if err != nil {
		log.Errorf("new thrift rpc failed, err: %v", err)
		return nil
	}

	snapshotResp, err := srcRpc.GetSnapshot(src, snapshotName)
	if err != nil {
		log.Errorf("get snapshot failed, err: %v", err)
		return err
	}
	log.Infof("resp: %v\n", snapshotResp)
	log.Infof("job: %s\n", string(snapshotResp.GetJobInfo()))

	// Step 2: restore snapshot
	var jobInfo map[string]interface{}
	// json umarshal jobInfo
	err = json.Unmarshal(snapshotResp.GetJobInfo(), &jobInfo)
	if err != nil {
		log.Errorf("unmarshal jobInfo failed, err: %v", err)
		return err
	}
	log.Infof("jobInfo: %v\n", jobInfo)

	extraInfo, err := j.genExtraInfo()
	if err != nil {
		return err
	}
	log.Infof("extraInfo: %v\n", extraInfo)

	jobInfo["extra_info"] = extraInfo

	// marshal jobInfo
	jobInfoBytes, err := json.Marshal(jobInfo)
	if err != nil {
		log.Errorf("marshal jobInfo failed, err: %v", err)
		return err
	}
	log.Infof("jobInfoBytes: %s\n", string(jobInfoBytes))
	snapshotResp.SetJobInfo(jobInfoBytes)

	// Restore snapshot to det
	dest := &j.Dest
	destRpc, err := rpc.NewThriftRpc(dest)
	if err != nil {
		log.Errorf("new thrift rpc failed, err: %v", err)
		return nil
	}
	restoreResp, err := destRpc.RestoreSnapshot(dest, snapshotName, snapshotResp)
	if err != nil {
		log.Errorf("restore snapshot failed, err: %v", err)
		return nil
	}
	log.Infof("resp: %v\n", restoreResp)

	return nil
}

func new_label(t *base.Spec, commitSeq int64) string {
	// label "ccr_sync_job:${db}:${table}:${commit_seq}"
	return fmt.Sprintf("ccr_sync_job:%s:%s:%d", t.Database, t.Table, commitSeq)
}

func (j *Job) contineSync() error {
	return nil
}

// func (j *Job) contineSync() error {
// 	src := &j.Src
// 	commitSeq := j.progress.CommitSeq

// 	// Step 1: get binlog
// 	srcRpc, err := rpc.NewThriftRpc(src)
// 	if err != nil {
// 		panic(err)
// 	}
// 	getBinlogResp, err := srcRpc.GetBinlog(src, commitSeq)
// 	if err != nil {
// 		log.Errorf("get binlog failed, err: %v", err)
// 		return nil
// 	}

// 	// Step 2: begin txn
// 	label := new_label(src, commitSeq)
// 	beginResp, err := srcRpc.BeginTransaction(src, label)
// 	if err != nil {
// 		panic(err)
// 	}
// 	fmt.Printf("resp: %v\n", beginResp)
// 	log.Infof("TxnId: %d, DbId: %d\n", beginResp.GetTxnId(), beginResp.GetDbId())

// 	// Step 3: ingest be

// 	// Step 4: commit txn

// 	// Step 5: save to db

// 	return nil
// }

func (j *Job) recoverJobProgress() error {
	// get progress from db, retry 3 times
	var err error
	var progressJson string
	for i := 0; i < 3; i++ {
		progressJson, err = j.db.GetProgress(j.Name)
		if err != nil {
			log.Error("get job progress failed", zap.String("job", j.Name), zap.Error(err))
			continue
		}
		break
	}
	if err != nil {
		return err
	}

	// parse progress
	if progress, err := NewJobProgressFromJson(progressJson); err != nil {
		log.Error("parse job progress failed", zap.String("job", j.Name), zap.Error(err))
		return err
	} else {
		j.progress = progress
		return nil
	}
}

func (j *Job) Sync() error {
	if j.isFirstSync {
		return j.firstSync()
	}

	/// Continue sync
	if j.progress == nil {
		err := j.recoverJobProgress()
		if err != nil {
			return err
		}
	}
	return j.contineSync()
}

// run job
func (j *Job) Loop() error {
	// 5s check once
	ticker := time.NewTicker(SYNC_DURATION)
	defer ticker.Stop()

	for {
		select {
		case <-j.stop:
			return nil
		case <-ticker.C:
			if err := j.Sync(); err != nil {
				log.Error("job sync failed", zap.String("job", j.Name), zap.Error(err))
			}
		}
	}
}

// stop job
func (j *Job) Stop() error {
	close(j.stop)
	return nil
}

func (j *Job) FirstRunCheck() error {
	log.Info("first run check job", zap.String("src", j.Src.String()), zap.String("dest", j.Dest.String()))

	// Step 1: check src database
	if src_db_exists, err := j.CheckSrcDatabaseExists(); err != nil {
		return err
	} else if !src_db_exists {
		return fmt.Errorf("src database %s not exists", j.Src.Database)
	}
	if enable, err := j.Src.IsDatabaseEnableBinlog(); err != nil {
		return err
	} else if !enable {
		return fmt.Errorf("src database %s not enable binlog", j.Src.Database)
	}

	// Step 2: check src table exists, if not exists, return err
	if j.SyncType == TableSync {
		if src_table_exists, err := j.CheckSrcTableExists(); err != nil {
			return err
		} else if !src_table_exists {
			return fmt.Errorf("src table %s.%s not exists", j.Src.Database, j.Src.Table)
		}
		if enable, err := j.Src.IsTableEnableBinlog(); err != nil {
			return err
		} else if !enable {
			return fmt.Errorf("src table %s.%s not enable binlog", j.Src.Database, j.Src.Table)
		}
	}

	// Step 3: check dest database && table exists
	// if dest database && table exists, return err
	if dest_db_exists, err := j.CheckDestDatabaseExists(); err != nil {
		return err
	} else if !dest_db_exists {
		return nil
	} else if j.SyncType == TableSync {
		if dest_table_exists, err := j.CheckDestTableExists(); err != nil {
			return err
		} else if dest_table_exists {
			return fmt.Errorf("dest table %s.%s already exists", j.Dest.Database, j.Dest.Table)
		}
	}

	return nil
}

func (j *Job) Create() error {
	log.Info("create job", zap.String("src", j.Src.String()), zap.String("dest", j.Dest.String()))

	// Step 1: check dest database exists
	var err error
	dest_db_exists := false
	if dest_db_exists, err = j.CheckDestDatabaseExists(); err != nil {
		return err
	}

	// Step 2: create table by dest table spec
	if !dest_db_exists {
		if err := j.DestCreateDatabase(); err != nil {
			return err
		}
	}

	return nil
}
