package xerror

import (
	"fmt"

	"github.com/pkg/errors"
)

type ErrType int

const (
	Normal ErrType = iota
	DB
	FE
	BE
)

func (e ErrType) String() string {
	switch e {
	case Normal:
		return "normal"
	case DB:
		return "db"
	case FE:
		return "fe"
	case BE:
		return "be"
	default:
		return "unknown"
	}
}

// this will add one stack msg in the error msg

// a wrapped error with error type
type XError struct {
	ErrType ErrType
	Err     error
}

func (e *XError) Error() string {
	return e.Err.Error()
}

func Errorf(errType ErrType, format string, args ...interface{}) error {
	err := &XError{
		ErrType: errType,
		Err:     fmt.Errorf(format, args...),
	}
	return errors.Wrap(err, "")
}

func Wrap(err error, errType ErrType, message string) error {
	if err == nil {
		return nil
	}
	err = &XError{
		ErrType: errType,
		Err:     err,
	}
	return errors.Wrap(err, message)
}

func Wrapf(err error, errType ErrType, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	err = &XError{
		ErrType: errType,
		Err:     err,
	}
	return errors.Wrapf(err, format, args...)
}
