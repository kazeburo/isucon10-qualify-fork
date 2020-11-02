package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/profiler"
	"contrib.go.opencensus.io/exporter/stackdriver"
	"contrib.go.opencensus.io/integrations/ocsql"
	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/trace"
)

var use_profiler = false

func initProfiler() {
	if !use_profiler {
		return
	}

	/*hostname, _ := os.Hostname()
	if hostname != "isu01" {
		return
	}*/
	serviceVersion := time.Now().Format("2006.01.02.15.04")
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		projectID = "kouzoh-p-kazeburo"
	}
	if err := profiler.Start(profiler.Config{
		Service:        "isucon10q",
		ServiceVersion: serviceVersion,
		ProjectID:      projectID,
	}); err != nil {
		log.Fatal(err)
	}
}

func initTrace() {
	if !use_profiler {
		return
	}

	/*hostname, _ := os.Hostname()
	if hostname != "isu01" {
		return
	}*/
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		projectID = "kouzoh-p-kazeburo"
	}

	exporter, err := stackdriver.NewExporter(stackdriver.Options{
		ProjectID:                projectID,
		TraceSpansBufferMaxBytes: 32 * 1024 * 1024,
	})
	if err != nil {
		log.Fatal(err)
	}
	trace.RegisterExporter(exporter)

	trace.ApplyConfig(trace.Config{DefaultSampler: trace.ProbabilitySampler(0.05)})
}

func withTrace(h http.Handler) http.Handler {
	if !use_profiler {
		return h
	}
	return &ochttp.Handler{Handler: h}
}

func tracedDriver(driverName string) string {
	if !use_profiler {
		return driverName
	}
	driverName, err := ocsql.Register(driverName, ocsql.WithQuery(true), ocsql.WithQueryParams(true))
	if err != nil {
		log.Fatal(err)
	}
	return driverName
}

func traceClient() http.Client {
	trs := &http.Transport{
		MaxIdleConnsPerHost: 100,
		MaxConnsPerHost:     100,
	}

	if !use_profiler {
		return http.Client{Transport: trs}
	}
	return http.Client{Transport: &ochttp.Transport{Base: trs}}
}

func traceCtx(ctx context.Context) context.Context {
	if !use_profiler {
		return context.Background()
	}
	return ctx
}
