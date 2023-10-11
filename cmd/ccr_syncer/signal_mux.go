package main

import (
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"
)

type SignalMux struct {
	sigChan chan os.Signal
	handler func(os.Signal) bool
}

func NewSignalMux(handler func(os.Signal) bool) *SignalMux {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)

	if handler == nil {
		log.Panic("signal handler is nil")
	}

	return &SignalMux{
		sigChan: sigChan,
		handler: handler,
	}
}

func (s *SignalMux) Serve() {
	for {
		signal := <-s.sigChan
		log.Infof("receive signal: %s", signal.String())

		if s.handler(signal) {
			return
		}
	}
}
