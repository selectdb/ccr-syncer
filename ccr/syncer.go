package ccr

import (
	base "github.com/selectdb/ccr_syncer/ccr/base"
)

type Syncer struct {
	Src           base.Spec
	Dest          base.Spec
	NextCommitSeq uint64
}

// TODO(Drogon): impl
func (s *Syncer) sync(commit_seq uint64) {
	// step 1: fetch all commit_seq version from src
	// step 2: begin transaction
	// step 3: distribute data to dest bes
	// step 4: check bes status && progress
	// step 5: commit transaction or rollback
	// step 6: update next_commit_seq
}

// full sync from src to dest
// func (s *Syncer)

// sync data from src to dest
// TODO(Drogon): impl
func (s *Syncer) Sync() error {
	return nil
}
