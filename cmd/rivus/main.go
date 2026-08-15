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
	logCloser, err := setupLogging()
	if err != nil {
		log.Fatalf("logging setup error: %v", err)
	}
	if logCloser != nil {
		defer logCloser.Close()
	}

	if len(os.Args) > 1 && os.Args[1] == "maintenance-worker" {
		if err := runMaintenanceWorkerCommand(os.Args[2:]); err != nil {
			log.Fatalf("maintenance worker error: %v", err)
		}
		return
	}

	if err := runServer(os.Args[1:]); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func runMaintenanceWorkerCommand(args []string) error {
	fs := flag.NewFlagSet("maintenance-worker", flag.ContinueOnError)
	queue := fs.Bool("queue", false, "continue polling the durable maintenance queue")
	pollSeconds := fs.Int("poll-interval-seconds", 0, "worker poll interval in seconds (0 uses env/default)")
	leaseSeconds := fs.Int("lease-seconds", 0, "task lease duration in seconds (0 uses env/default)")
	taskPageSize := fs.Int("task-page-size", 0, "maximum tasks claimed per parent run")
	duePageSize := fs.Int("due-page-size", 0, "maximum due table states read per operation")
	workerID := fs.String("worker-id", "", "stable worker identity (defaults to hostname-pid)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var pollInterval, leaseDuration time.Duration
	if *pollSeconds > 0 {
		pollInterval = time.Duration(*pollSeconds) * time.Second
	}
	if *leaseSeconds > 0 {
		leaseDuration = time.Duration(*leaseSeconds) * time.Second
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return iceberg.RunMaintenanceWorker(ctx, strings.TrimSpace(os.Getenv("RIVUS_META_MYSQL_DSN")), iceberg.MaintenanceWorkerOptions{
		Queue:         *queue,
		PollInterval:  pollInterval,
		LeaseDuration: leaseDuration,
		TaskPageSize:  *taskPageSize,
		DuePageSize:   *duePageSize,
		WorkerID:      strings.TrimSpace(*workerID),
	})
}

func runServer(args []string) error {
	fs := flag.NewFlagSet("rivus", flag.ContinueOnError)
	addr := fs.String("addr", ":8080", "HTTP listen address")
	uiDir := fs.String("ui-dir", "./ui", "UI directory")
	if err := fs.Parse(args); err != nil {
		return err
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
			return err
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
		return err
	}
	authConfig, err := api.LoadAuthConfigFromEnv()
	if err != nil {
		return err
	}

	apiServer := api.NewServer(jobManager, *uiDir, authConfig)
	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           apiServer.Router(),
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
			return nil
		}
		return err
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
		return nil
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
