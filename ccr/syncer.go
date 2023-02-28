package ccr

type Syncer struct {
	Src  TableSpec
	Dest TableSpec
}

func (s *Syncer) sync(commit_seq uint64) {
	// TODO(Drogon): impl
}

// full sync from src to dest

// sync data from src to dest
// TODO(Drogon): impl
func (s *Syncer) Sync() error {
	return nil
}
