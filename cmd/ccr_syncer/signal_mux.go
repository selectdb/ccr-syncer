// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License
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
