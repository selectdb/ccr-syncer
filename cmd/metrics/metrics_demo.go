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
