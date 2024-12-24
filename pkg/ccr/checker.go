// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License
package ccr

import (
	"fmt"
	"time"

	"github.com/selectdb/ccr_syncer/pkg/storage"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
	log "github.com/sirupsen/logrus"
)

const (
	CHECK_DURATION = time.Second * 5
	CHECK_TIMEOUT  = CHECK_DURATION*2 + time.Second*2
)

type CheckerState int

const (
	checkerStateRefresh   CheckerState = 0
	checkerStateUpdate    CheckerState = 1
	checkerStateCheck     CheckerState = 2
	checkerStateRebalance CheckerState = 3

	checkerStateFinish CheckerState = 200
	checkerStateError  CheckerState = 201
)

func (cs CheckerState) String() string {
	switch cs {
	case checkerStateRefresh:
		return "checkerStateRefresh"
	case checkerStateUpdate:
		return "checkerStateUpdate"
	case checkerStateCheck:
		return "checkerStateCheck"
	case checkerStateRebalance:
		return "checkerStateRebalance"
	case checkerStateFinish:
		return "checkerStateFinish"
	case checkerStateError:
		return "checkerStateError"
	default:
		return fmt.Sprintf("Unknown checker state: %d", cs)
	}
}

type Checker struct {
	lastStamp   int64
	hostInfo    string
	db          storage.DB
	jobManager  *JobManager
	state       CheckerState
	firstCheck  bool
	deadSyncers []string
	err         error
	stop        chan struct{}
}

func NewChecker(hostInfo string, db storage.DB, jm *JobManager) *Checker {
	return &Checker{
		lastStamp:  -1,
		hostInfo:   hostInfo,
		db:         db,
		jobManager: jm,
		stop:       make(chan struct{}),
	}
}

func (s *Checker) reset() {
	s.state = checkerStateRefresh
	s.firstCheck = true
	s.deadSyncers = nil
	s.err = nil
}

func (c *Checker) next() {
	if c.err != nil {
		c.state = checkerStateError
		return
	}

	switch c.state {
	case checkerStateRefresh:
		if c.lastStamp == storage.InvalidCheckTimestamp {
			c.state = checkerStateUpdate
		} else if c.firstCheck {
			c.state = checkerStateCheck
		} else {
			c.state = checkerStateFinish
		}
		c.firstCheck = false
	case checkerStateUpdate:
		c.state = checkerStateRefresh
	case checkerStateCheck:
		if c.deadSyncers != nil && len(c.deadSyncers) != 0 {
			c.state = checkerStateRebalance
		} else {
			c.state = checkerStateFinish
		}
	case checkerStateRebalance:
		c.state = checkerStateRefresh
	default:
		c.err = xerror.Errorf(xerror.Normal, "Unknown checker state %d", c.state)
		c.state = checkerStateError
	}
}

func (c *Checker) handleRefresh() {
	c.lastStamp, c.err = c.db.RefreshSyncer(c.hostInfo, c.lastStamp)
}

func (c *Checker) handleUpdate() {
	var jobs []string
	c.lastStamp, jobs, c.err = c.db.GetStampAndJobs(c.hostInfo)
	if len(jobs) != 0 {
		c.err = c.jobManager.Recover(jobs)
	}
	log.Infof("update jobs %v", jobs)
}

func (c *Checker) handleCheck() {
	c.deadSyncers, c.err = c.db.GetDeadSyncers(c.lastStamp - int64(CHECK_TIMEOUT.Nanoseconds()))
}

func (c *Checker) handleRebalance() {
	log.Infof("rebalance dead syncers: %v", c.deadSyncers)
	c.err = c.db.RebalanceLoadFromDeadSyncers(c.deadSyncers)
}

func (c *Checker) check() error {
	c.reset()

	for {
		log.Tracef("checker state: %s", c.state)
		switch c.state {
		case checkerStateRefresh:
			c.handleRefresh()
		case checkerStateUpdate:
			c.handleUpdate()
		case checkerStateCheck:
			c.handleCheck()
		case checkerStateRebalance:
			c.handleRebalance()
		case checkerStateFinish:
			// if log.GetLevel() >= log.DebugLevel {
			// 	if ans, err := c.db.GetAllData(); err != nil {
			// 		log.Error(err)
			// 	} else {
			// 		log.Debugf("table data: %v", ans)
			// 	}
			// }
			return nil
		case checkerStateError:
			return c.err
		default:
			return xerror.Errorf(xerror.Normal, "Unknown checker state %d", c.state)
		}
		c.next()
	}
}

func (c *Checker) Start() error {
	if err := c.db.AddSyncer(c.hostInfo); err != nil {
		log.Errorf("add failed, host info: %s, err: %+v", c.hostInfo, err)
		return err
	}
	if err := c.check(); err != nil {
		log.Errorf("checker first failed, host info: %s, err: %+v", c.hostInfo, err)
		return err
	}
	return c.run()
}

func (c *Checker) Stop() {
	log.Info("checker stopping")
	close(c.stop)
}

func (c *Checker) run() error {
	ticker := time.NewTicker(CHECK_DURATION)
	defer ticker.Stop()

	for {
		select {
		case <-c.stop:
			log.Info("checker stopped")
			return nil
		case <-ticker.C:
			if err := c.check(); err != nil {
				log.Errorf("checker failed, host info: %s, err: %+v", c.hostInfo, err)
			}
		}
	}
}
