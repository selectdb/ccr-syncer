package ccr

import "github.com/selectdb/ccr_syncer/pkg/xerror"

var errBackendNotFound = xerror.NewWithoutStack(xerror.Meta, "backend not found")
