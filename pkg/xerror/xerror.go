package xerror

import (
	stderrors "errors"
	"fmt"
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
	Meta   = newErrorCategory("meta") // The error is related to meta, so a new snapshot will be created.
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
		return "Panic"
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

// return the innerest xerror, unwrap stack && xerror
func (e *XError) Error() string {
	if err, ok := e.err.(*withStack); ok {
		return err.error.Error()
	}

	// If the error is an XError, recursively call Error() on the inner error
	if xerr, ok := e.err.(*XError); ok {
		return xerr.Error()
	}

	// Otherwise, format the error message with the category name and error message
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

func NewWithoutStack(errCategory ErrorCategory, message string) *XError {
	err := &XError{
		category: errCategory,
		errType:  xrecoverable,
		err:      stderrors.New(message),
	}
	return err
}

func New(errCategory ErrorCategory, message string) error {
	err := NewWithoutStack(errCategory, message)
	return newWithStack(err)
}

func PanicWithoutStack(errCategory ErrorCategory, message string) error {
	err := &XError{
		category: errCategory,
		errType:  xpanic,
		err:      stderrors.New(message),
	}
	return err
}

func Panic(errCategory ErrorCategory, message string) error {
	err := PanicWithoutStack(errCategory, message)
	return newWithStack(err)
}

func errorf(errCategory ErrorCategory, errtype errType, format string, args ...interface{}) *XError {
	err := &XError{
		category: errCategory,
		errType:  errtype,
		err:      fmt.Errorf(format, args...),
	}
	return err
}

func Errorf(errCategory ErrorCategory, format string, args ...interface{}) error {
	err := errorf(errCategory, xrecoverable, format, args...)
	return newWithStack(err)
}

func Panicf(errCategory ErrorCategory, format string, args ...interface{}) error {
	err := errorf(errCategory, xpanic, format, args...)
	return newWithStack(err)
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
	err = &withMessage{
		cause: err,
		msg:   message,
	}
	return &withStack{
		err,
		callers(4),
	}
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

	err = &withMessage{
		cause: err,
		msg:   fmt.Sprintf(format, args...),
	}
	return &withStack{
		err,
		callers(4),
	}
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

func XPanicWrapf(xerr *XError, format string, args ...interface{}) error {
	return wrapf(xerr, xerr.category, xpanic, format, args...)
}

func newWithStack(err error) error {
	if err == nil {
		return nil
	}

	return &withStack{
		err,
		callers(4),
	}
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

	return &withStack{
		err,
		callers(4),
	}
}

func IsCategory(err error, category ErrorCategory) bool {
	if err == nil {
		return false
	}

	if xerr, ok := err.(*XError); ok {
		return xerr.category == category
	}

	return false
}
