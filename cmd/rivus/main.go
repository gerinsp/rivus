package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gerinsp/rivus/pkg/api"
	"github.com/gerinsp/rivus/pkg/connector"
	"github.com/gerinsp/rivus/pkg/connectors/doris"
	"github.com/gerinsp/rivus/pkg/connectors/iceberg"
	"github.com/gerinsp/rivus/pkg/connectors/mysql"
	"github.com/gerinsp/rivus/pkg/core"
	"github.com/gerinsp/rivus/pkg/meta"
)

const defaultGracefulShutdownTimeout = 90 * time.Second

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	uiDir := flag.String("ui-dir", "./ui", "UI directory")
	flag.Parse()

	logCloser, err := setupLogging()
	if err != nil {
		log.Fatalf("logging setup error: %v", err)
	}
	if logCloser != nil {
		defer logCloser.Close()
	}

	reg := connector.NewRegistry()
	mysql.Register(reg)
	doris.Register(reg)
	iceberg.Register(reg)

	jobManagerOpts := make([]core.JobManagerOption, 0, 3)
	jobManagerOpts = append(jobManagerOpts, core.WithAutoResume(envBool("RIVUS_AUTO_RESUME", false)))
	if dsn := strings.TrimSpace(os.Getenv("RIVUS_META_MYSQL_DSN")); dsn != "" {
		jobStore, err := meta.NewMySQLJobStore(dsn)
		if err != nil {
			log.Fatalf("job store error: %v", err)
		}
		jobManagerOpts = append(jobManagerOpts,
			core.WithJobStore(jobStore),
			core.WithDefaultMetaMySQLDSN(dsn),
		)
	}
	jobManager := core.NewJobManager(reg, jobManagerOpts...)
	restoreCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := jobManager.RestorePersistedJobs(restoreCtx); err != nil {
		log.Fatalf("restore persisted jobs failed: %v", err)
	}
	authConfig, err := api.LoadAuthConfigFromEnv()
	if err != nil {
		log.Fatalf("auth config error: %v", err)
	}

	apiServer := api.NewServer(jobManager, *uiDir, authConfig)
	mux := apiServer.Router()
	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 15 * time.Second,
	}

	log.Printf("Starting rivus on %s ...", *addr)
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- httpServer.ListenAndServe()
	}()

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	select {
	case err := <-serverErr:
		if errors.Is(err, http.ErrServerClosed) {
			return
		}
		log.Fatalf("server error: %v", err)
	case <-signalCtx.Done():
		timeout := gracefulShutdownTimeoutFromEnv()
		log.Printf("Shutdown signal received; draining jobs for up to %s", timeout)

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), timeout)
		drainErr := jobManager.Shutdown(shutdownCtx)
		shutdownCancel()
		if drainErr != nil {
			log.Printf("job drain error: %v", drainErr)
		}

		httpCtx, httpCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := httpServer.Shutdown(httpCtx); err != nil {
			log.Printf("HTTP shutdown error: %v", err)
			_ = httpServer.Close()
		}
		httpCancel()
		log.Printf("Rivus shutdown complete")
	}
}

func gracefulShutdownTimeoutFromEnv() time.Duration {
	raw := strings.TrimSpace(os.Getenv("RIVUS_SHUTDOWN_TIMEOUT_SECONDS"))
	if raw == "" {
		return defaultGracefulShutdownTimeout
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		log.Printf("invalid RIVUS_SHUTDOWN_TIMEOUT_SECONDS=%q; using %s", raw, defaultGracefulShutdownTimeout)
		return defaultGracefulShutdownTimeout
	}
	return time.Duration(seconds) * time.Second
}

func setupLogging() (io.Closer, error) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg := logConfigFromEnv()
	if !cfg.enabled {
		return nil, nil
	}

	w, err := newRotatingLogWriter(cfg)
	if err != nil {
		return nil, err
	}

	output := io.Writer(w)
	if cfg.stderrEnabled {
		output = io.MultiWriter(os.Stderr, w)
	}
	log.SetOutput(output)
	log.Printf("[logging] writing logs dir=%s prefix=%s retention_days=%d max_size_mb=%d max_total_size_mb=%d stderr=%t",
		cfg.dir, cfg.prefix, cfg.retentionDays, cfg.maxSizeMB, cfg.maxTotalSizeMB, cfg.stderrEnabled)
	return w, nil
}
