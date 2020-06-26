package util

import (
	"log"
)

// ErrorLogger may be used to report errors. Implementations may decide
// to log, mutate, redirect and discard them. This interface is used in
// places where errors are generated asynchronously, meaning they cannot
// be returned to the caller directly.
type ErrorLogger interface {
	Log(err error)
}

type defaultErrorLogger struct{}

func (l defaultErrorLogger) Log(err error) {
	log.Print(err)
}

// DefaultErrorLogger writes errors using Go's standard logging package.
var DefaultErrorLogger ErrorLogger = defaultErrorLogger{}
