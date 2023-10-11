package xerror

import (
	stderrors "errors"
	"fmt"

	"github.com/hashicorp/go-metrics"
	"github.com/pkg/errors"
)

type ErrType int

const (
	Normal ErrType = iota
	RPC
	DB
	FE
	BE
	Meta
)

func (e ErrType) String() string {
	switch e {
	case Normal:
		return "normal"
	case RPC:
		return "rpc"
	case DB:
		return "db"
	case FE:
		return "fe"
	case BE:
		return "be"
	case Meta:
		return "meta"
	default:
		return "unknown"
	}
}

type ErrLevel int

const (
	xrecoverable ErrLevel = iota
	xpanic
)

func (e ErrLevel) String() string {
	switch e {
	case xrecoverable:
		return "Recoverable"
	case xpanic:
		return "panic"
	default:
		panic("unknown error level")
	}
}

// this will add one stack msg in the error msg

// a wrapped error with error type
type XError struct {
	ErrType  ErrType
	errLevel ErrLevel
	err      error
}

func (e *XError) Error() string {
	if xerr, ok := e.err.(*XError); ok {
		return xerr.Error()
	}

	return fmt.Sprintf("%s: %s", e.ErrType.String(), e.err.Error())
}

func (e *XError) Unwrap() error {
	return e.err
}

func (e *XError) IsRecoverable() bool {
	return e.errLevel == xrecoverable
}

func (e *XError) IsPanic() bool {
	return e.errLevel == xpanic
}

func New(errType ErrType, message string) error {
	err := &XError{
		ErrType:  errType,
		errLevel: xrecoverable,
		err:      stderrors.New(message),
	}
	return errors.WithStack(err)
}

func XNew(errType ErrType, message string) *XError {
	err := &XError{
		ErrType:  errType,
		errLevel: xrecoverable,
		err:      stderrors.New(message),
	}
	return err
}

func Panic(errType ErrType, message string) error {
	err := &XError{
		ErrType:  errType,
		errLevel: xpanic,
		err:      stderrors.New(message),
	}
	metrics.IncrCounter([]string{fmt.Sprintf("Error.%s", errType.String())}, 1)
	return errors.WithStack(err)
}

func Errorf(errType ErrType, format string, args ...interface{}) error {
	err := &XError{
		ErrType:  errType,
		errLevel: xrecoverable,
		err:      fmt.Errorf(format, args...),
	}
	metrics.IncrCounter([]string{fmt.Sprintf("Error.%s", errType.String())}, 1)
	return errors.WithStack(err)
}

func Panicf(errType ErrType, format string, args ...interface{}) error {
	err := &XError{
		ErrType:  errType,
		errLevel: xpanic,
		err:      fmt.Errorf(format, args...),
	}
	metrics.IncrCounter([]string{fmt.Sprintf("Error.%s", errType.String())}, 1)
	return errors.WithStack(err)
}

func wrap(err error, errType ErrType, errLevel ErrLevel, message string) error {
	if err == nil {
		return nil
	}

	err = &XError{
		ErrType:  errType,
		errLevel: errLevel,
		err:      err,
	}
	return errors.Wrap(err, message)
}

func Wrap(err error, errType ErrType, message string) error {
	return wrap(err, errType, xrecoverable, message)
}

func PanicWrap(err error, errType ErrType, message string) error {
	return wrap(err, errType, xpanic, message)
}

func wrapf(err error, errType ErrType, errLevel ErrLevel, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}

	err = &XError{
		ErrType:  errType,
		errLevel: errLevel,
		err:      err,
	}
	return errors.Wrapf(err, format, args...)
}

func Wrapf(err error, errType ErrType, format string, args ...interface{}) error {
	return wrapf(err, errType, xrecoverable, format, args...)
}

func XWrapf(xerr *XError, format string, args ...interface{}) error {
	return wrapf(xerr, xerr.ErrType, xrecoverable, format, args...)
}

func PanicWrapf(err error, errType ErrType, format string, args ...interface{}) error {
	return wrapf(err, errType, xpanic, format, args...)
}

func WithStack(err error) error {
	if err == nil {
		return nil
	}

	err = &XError{
		ErrType:  Normal,
		errLevel: xrecoverable,
		err:      err,
	}

	return errors.WithStack(err)
}
