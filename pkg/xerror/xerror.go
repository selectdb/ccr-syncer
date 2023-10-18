package xerror

import (
	stderrors "errors"
	"fmt"

	"github.com/pkg/errors"
)

// type ErrorCategory int

type ErrorCategory interface {
	Name() string
}

var (
	Normal = newErrorCategory("normal")
	RPC    = newErrorCategory("rpc")
	DB     = newErrorCategory("db")
	FE     = newErrorCategory("fe")
	BE     = newErrorCategory("be")
	Meta   = newErrorCategory("meta")
)

type xErrorCategory struct {
	name string
}

func (e xErrorCategory) Name() string {
	return e.name
}

func newErrorCategory(name string) ErrorCategory {
	return &xErrorCategory{
		name: name,
	}
}

type errType int

const (
	xrecoverable errType = iota
	xpanic
)

func (e errType) String() string {
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
	category ErrorCategory
	errType  errType
	err      error
}

func (e *XError) Category() ErrorCategory {
	return e.category
}

func (e *XError) Error() string {
	if xerr, ok := e.err.(*XError); ok {
		return xerr.Error()
	}

	return fmt.Sprintf("[%s] %s", e.category.Name(), e.err.Error())
}

func (e *XError) Unwrap() error {
	return e.err
}

func (e *XError) IsRecoverable() bool {
	return e.errType == xrecoverable
}

func (e *XError) IsPanic() bool {
	return e.errType == xpanic
}

func New(errCategory ErrorCategory, message string) error {
	err := &XError{
		category: errCategory,
		errType:  xrecoverable,
		err:      stderrors.New(message),
	}
	return errors.WithStack(err)
}

func XNew(errCategory ErrorCategory, message string) *XError {
	err := &XError{
		category: errCategory,
		errType:  xrecoverable,
		err:      stderrors.New(message),
	}
	return err
}

func Panic(errCategory ErrorCategory, message string) error {
	err := &XError{
		category: errCategory,
		errType:  xpanic,
		err:      stderrors.New(message),
	}
	return errors.WithStack(err)
}

func Errorf(errCategory ErrorCategory, format string, args ...interface{}) error {
	err := &XError{
		category: errCategory,
		errType:  xrecoverable,
		err:      fmt.Errorf(format, args...),
	}
	return errors.WithStack(err)
}

func Panicf(errCategory ErrorCategory, format string, args ...interface{}) error {
	err := &XError{
		category: errCategory,
		errType:  xpanic,
		err:      fmt.Errorf(format, args...),
	}
	return errors.WithStack(err)
}

func wrap(err error, errCategory ErrorCategory, errLevel errType, message string) error {
	if err == nil {
		return nil
	}

	err = &XError{
		category: errCategory,
		errType:  errLevel,
		err:      err,
	}
	return errors.Wrap(err, message)
}

func Wrap(err error, errCategory ErrorCategory, message string) error {
	return wrap(err, errCategory, xrecoverable, message)
}

func PanicWrap(err error, errCategory ErrorCategory, message string) error {
	return wrap(err, errCategory, xpanic, message)
}

func wrapf(err error, errCategory ErrorCategory, errLevel errType, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}

	err = &XError{
		category: errCategory,
		errType:  errLevel,
		err:      err,
	}
	return errors.Wrapf(err, format, args...)
}

func Wrapf(err error, errCategory ErrorCategory, format string, args ...interface{}) error {
	return wrapf(err, errCategory, xrecoverable, format, args...)
}

func XWrapf(xerr *XError, format string, args ...interface{}) error {
	return wrapf(xerr, xerr.category, xrecoverable, format, args...)
}

func PanicWrapf(err error, errCategory ErrorCategory, format string, args ...interface{}) error {
	return wrapf(err, errCategory, xpanic, format, args...)
}

func WithStack(err error) error {
	if err == nil {
		return nil
	}

	err = &XError{
		category: Normal,
		errType:  xrecoverable,
		err:      err,
	}

	return errors.WithStack(err)
}
