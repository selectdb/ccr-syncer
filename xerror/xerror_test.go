package xerror

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrorf(t *testing.T) {
	err := Errorf(Normal, "test error")
	t.Logf("err: %+v", err)
	assert.NotNil(t, err)

	var xerr *XError
	assert.True(t, errors.As(err, &xerr))
	assert.Equal(t, xerr.ErrType, Normal)
	assert.Equal(t, xerr.Err.Error(), "test error")
}

func TestWrap(t *testing.T) {
	err := errors.New("db open error")
	wrappedErr := Wrap(err, DB, "wrapped error")
	t.Logf("wrappedErr: %+v", wrappedErr)
	assert.NotNil(t, wrappedErr)

	var xerr *XError
	assert.True(t, errors.As(wrappedErr, &xerr))
	assert.Equal(t, xerr.ErrType, DB)
	assert.Equal(t, xerr.Err.Error(), "db open error")
}

// func TestWrapf(t *testing.T) {
// 	err := errors.New("test error")
// 	wrappedErr := xerror.Wrapf(err, xerror.FE, "wrapped error: %s", "foo")
// 	if wrappedErr == nil {
// 		t.Errorf("expected error, got nil")
// 	}
// 	if xerr, ok := wrappedErr.(*xerror.XError); ok {
// 		if xerr.ErrType != xerror.FE {
// 			t.Errorf("expected error type %v, got %v", xerror.FE, xerr.ErrType)
// 		}
// 		if xerr.Err.Error() != "test error" {
// 			t.Errorf("expected error message %q, got %q", "test error", xerr.Err.Error())
// 		}
// 	} else {
// 		t.Errorf("expected *xerror.XError, got %T", wrappedErr)
// 	}
// }
