package xerror

import (
	"errors"
	"fmt"
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

func TestXError_Error(t *testing.T) {
	errMsg := "test error"
	err := Errorf(Normal, errMsg)
	assert.NotNil(t, err)

	var xerr *XError
	assert.True(t, errors.As(err, &xerr))
	assert.Equal(t, xerr.Error(), fmt.Sprintf("[%s] %s", Normal.Name(), errMsg))

	err = Wrap(err, DB, "wrapped error")
	// t.Logf("err: %+v", err)
	assert.NotNil(t, err)

	assert.True(t, errors.As(err, &xerr))
	assert.Equal(t, xerr.Error(), fmt.Sprintf("[%s] %s", Normal.Name(), errMsg))
}

// UnitTest for XError
func TestErrorf(t *testing.T) {
	errMsg := "test error"
	err := Errorf(Normal, errMsg)
	assert.NotNil(t, err)
	// t.Logf("err: %+v", err)

	var xerr *XError
	assert.True(t, errors.As(err, &xerr))
	assert.True(t, xerr.IsRecoverable())
	assert.Equal(t, xerr.Category(), Normal)
	assert.Equal(t, xerr.err.Error(), errMsg)
}

func TestWrap(t *testing.T) {
	errMsg := "db open error"
	err := errors.New(errMsg)
	wrappedErr := Wrap(err, DB, "wrapped error")
	assert.NotNil(t, wrappedErr)
	// t.Logf("wrappedErr: %+v", wrappedErr)

	var xerr *XError
	assert.True(t, errors.As(wrappedErr, &xerr))
	assert.True(t, xerr.IsRecoverable())
	assert.Equal(t, xerr.Category(), DB)
	assert.Equal(t, xerr.err.Error(), errMsg)
}

func TestWrapf(t *testing.T) {
	errMsg := "fe test error"
	err := errors.New(errMsg)
	wrappedErr := Wrapf(err, FE, "wrapped error: %s", "foo")
	assert.NotNil(t, wrappedErr)
	// t.Logf("wrappedErr: %+v", wrappedErr)

	var xerr *XError
	assert.True(t, errors.As(wrappedErr, &xerr))
	assert.True(t, xerr.IsRecoverable())
	assert.Equal(t, xerr.Category(), FE)
	assert.Equal(t, xerr.err.Error(), errMsg)
}

func TestIs(t *testing.T) {
	errBackendNotFound := NewWithoutStack(Meta, "backend not found")
	wrappedErr := XWrapf(errBackendNotFound, "backend id: %d", 33415)
	assert.NotNil(t, wrappedErr)
	// t.Logf("wrappedErr: %+v", wrappedErr)

	assert.True(t, errors.Is(wrappedErr, errBackendNotFound))

	var xerr *XError
	assert.True(t, errors.As(wrappedErr, &xerr))
	assert.True(t, xerr.IsRecoverable())
	assert.Equal(t, xerr.Category(), Meta)
	// t.Logf("xerr: %s", xerr.Error())
	assert.Equal(t, errBackendNotFound.Error(), errBackendNotFound.Error())
}

func TestPanic(t *testing.T) {
	errMsg := "test panic"
	err := Panic(Normal, errMsg)
	// t.Logf("err: %+v", err)
	assert.NotNil(t, err)

	var xerr *XError
	assert.True(t, errors.As(err, &xerr))
	assert.True(t, xerr.IsPanic())
	assert.Equal(t, xerr.Category(), Normal)
	assert.Equal(t, xerr.err.Error(), errMsg)
}

func TestPanicf(t *testing.T) {
	errMsg := "test panicf"
	err := Panicf(Normal, errMsg)
	// t.Logf("err: %+v", err)
	assert.NotNil(t, err)

	var xerr *XError
	assert.True(t, errors.As(err, &xerr))
	assert.True(t, xerr.IsPanic())
	assert.Equal(t, xerr.Category(), Normal)
	assert.Equal(t, xerr.err.Error(), errMsg)
}

func TestIsCategory(t *testing.T) {
	errMsg := "test error"
	err := Errorf(Normal, errMsg)
	assert.NotNil(t, err)

	assert.True(t, IsCategory(err, Normal))
	assert.False(t, IsCategory(err, DB))

	err = Wrap(err, Meta, "wrapped error")
	assert.True(t, IsCategory(err, Meta))
	assert.False(t, IsCategory(err, Normal))

	err = fmt.Errorf("invalid error")
	assert.False(t, IsCategory(err, Meta))
	assert.False(t, IsCategory(err, Normal))
}
