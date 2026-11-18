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
	"log"
	"net/http"
	"time"

	"github.com/hashicorp/go-metrics"
	prometheussink "github.com/hashicorp/go-metrics/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func promHttp() {
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func main() {
	go promHttp()
	sink, _ := prometheussink.NewPrometheusSink()
	metrics.NewGlobal(metrics.DefaultConfig("service-name"), sink)
	metrics.SetGauge([]string{"foo"}, 42)
	metrics.EmitKey([]string{"bar"}, 30)
	metrics.IncrCounter([]string{"baz"}, 42)
	metrics.IncrCounter([]string{"baz"}, 1)
	metrics.IncrCounter([]string{"baz"}, 80)
	metrics.AddSample([]string{"method", "wow"}, 42)
	metrics.AddSample([]string{"method", "wow"}, 100)
	metrics.AddSample([]string{"method", "wow"}, 22)
	time.Sleep(10000000 * time.Second)
}
