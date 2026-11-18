package rpc

import (
	"errors"

	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/selectdb/ccr_syncer/pkg/xerror"
)

func IsUnknownMethod(err error) bool {
	if err == nil {
		return false
	}

	var xerr *xerror.XError
	if errors.As(err, &xerr) {
		return IsUnknownMethod(xerr.Unwrap())
	}

	var de *kerrors.DetailedError
	if errors.As(err, &de) {
		return IsUnknownMethod(de.Unwrap())
	}

	var te *remote.TransError
	if errors.As(err, &te) && te.TypeID() == remote.UnknownMethod {
		return true
	}

	return false
}
