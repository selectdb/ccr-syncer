package xerror

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TODO(Drogon): Add more unittests

// UnitTest for xCategory
func TestXCategory(t *testing.T) {
	assert.Equal(t, Normal.Name(), "normal")
	assert.Equal(t, RPC.Name(), "rpc")
	assert.Equal(t, DB.Name(), "db")
	assert.Equal(t, FE.Name(), "fe")
	assert.Equal(t, BE.Name(), "be")
	assert.Equal(t, Meta.Name(), "meta")
}

// UnitTest for XError
func TestErrorf(t *testing.T) {
	err := Errorf(Normal, "test error")
	assert.NotNil(t, err)
	// t.Logf("err: %+v", err)

	var xerr *XError
	assert.True(t, errors.As(err, &xerr))
	assert.Equal(t, xerr.Category(), Normal)
	assert.Equal(t, xerr.err.Error(), "test error")
}

func TestWrap(t *testing.T) {
	err := errors.New("db open error")
	wrappedErr := Wrap(err, DB, "wrapped error")
	assert.NotNil(t, wrappedErr)
	// t.Logf("wrappedErr: %+v", wrappedErr)

	var xerr *XError
	assert.True(t, errors.As(wrappedErr, &xerr))
	assert.Equal(t, xerr.Category(), DB)
	assert.Equal(t, xerr.err.Error(), "db open error")
}

func TestWrapf(t *testing.T) {
	err := errors.New("fe test error")
	wrappedErr := Wrapf(err, FE, "wrapped error: %s", "foo")
	assert.NotNil(t, wrappedErr)
	// t.Logf("wrappedErr: %+v", wrappedErr)

	var xerr *XError
	assert.True(t, errors.As(wrappedErr, &xerr))
	assert.Equal(t, xerr.Category(), FE)
	assert.Equal(t, xerr.err.Error(), "fe test error")
}

func TestIs(t *testing.T) {
	errBackendNotFound := XNew(Meta, "backend not found")
	wrappedErr := XWrapf(errBackendNotFound, "backend id: %d", 33415)
	assert.NotNil(t, wrappedErr)
	// t.Logf("wrappedErr: %+v", wrappedErr)

	assert.True(t, errors.Is(wrappedErr, errBackendNotFound))

	var xerr *XError
	assert.True(t, errors.As(wrappedErr, &xerr))
	assert.Equal(t, xerr.Category(), Meta)
	// t.Logf("xerr: %s", xerr.Error())
	assert.Equal(t, errBackendNotFound.Error(), errBackendNotFound.Error())
}
