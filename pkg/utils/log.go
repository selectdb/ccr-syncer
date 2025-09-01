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
package utils

import (
	"flag"
	"fmt"
	"io"
	"os"

	filename "github.com/keepeye/logrus-filename"
	log "github.com/sirupsen/logrus"
	prefixed "github.com/t-tomalak/logrus-prefixed-formatter"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	logLevel        string
	logFilename     string
	logAlsoToStderr bool
	logRetainNum    int
	logRetainDays   int
)

func init() {
	flag.StringVar(&logLevel, "log_level", "debug", "log level")
	flag.StringVar(&logFilename, "log_filename", "", "log filename")
	flag.BoolVar(&logAlsoToStderr, "log_also_to_stderr", false, "log also to stderr")
	flag.IntVar(&logRetainNum, "log_retain_num", 30, "log retain number")
	flag.IntVar(&logRetainDays, "log_retain_days", 7, "log retain days")
}

func InitLog() {
	level, err := log.ParseLevel(logLevel)
	if err != nil {
		fmt.Printf("parse log level %v failed: %v\n", logLevel, err)
		os.Exit(1)
	}
	log.SetLevel(level)
	log.SetFormatter(&prefixed.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05.000",
		ForceFormatting: true,
	})

	syncHook := NewHook()
	log.AddHook(syncHook)

	// log.SetReportCaller(true), caller by filename
	filenameHook := filename.NewHook()
	filenameHook.Field = "line"
	log.AddHook(filenameHook)

	if logFilename == "" {
		log.SetOutput(os.Stdout)
		return
	}

	// TODO: Add write permission check
	output := &lumberjack.Logger{
		Filename:   logFilename,
		MaxSize:    1024, // 1GB
		MaxAge:     logRetainDays,
		MaxBackups: logRetainNum,
		LocalTime:  true,
		Compress:   false,
	}
	if logAlsoToStderr {
		writer := io.MultiWriter(output, os.Stderr)
		log.SetOutput(writer)
	} else {
		log.SetOutput(output)
	}
}
