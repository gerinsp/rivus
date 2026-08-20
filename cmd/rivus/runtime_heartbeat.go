package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gerinsp/rivus/pkg/meta"
	"github.com/gerinsp/rivus/pkg/version"
)

const runtimeHeartbeatInterval = 30 * time.Second

func startRuntimeHeartbeat(parent context.Context, dsn, role, configuredID string) (func(), error) {
	store, err := meta.NewRuntimeInstanceStore(dsn)
	if err != nil {
		return nil, err
	}
	initCtx, initCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer initCancel()
	if err := store.Init(initCtx); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("initialize runtime version registry: %w", err)
	}

	build := version.Current()
	instance := meta.RuntimeInstance{
		Role:       strings.TrimSpace(role),
		InstanceID: runtimeInstanceID(role, configuredID),
		Version:    build.Version,
		ImageTag:   build.ImageTag,
		Commit:     build.Commit,
		BuildDate:  build.BuildDate,
		StartedAt:  time.Now().UTC(),
	}
	if err := store.RegisterRuntimeInstance(initCtx, instance); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("register runtime version: %w", err)
	}

	ctx, cancel := context.WithCancel(parent)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(runtimeHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				instance.HeartbeatAt = now.UTC()
				heartbeatCtx, heartbeatCancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := store.Heartbeat(heartbeatCtx, instance)
				heartbeatCancel()
				if err != nil {
					log.Printf("[runtime-version] heartbeat role=%s instance=%s error=%v", instance.Role, instance.InstanceID, err)
				}
			}
		}
	}()

	var stopOnce sync.Once
	return func() {
		stopOnce.Do(func() {
			cancel()
			wg.Wait()
			_ = store.Close()
		})
	}, nil
}

func runtimeInstanceID(role, configured string) string {
	for _, candidate := range []string{
		strings.TrimSpace(os.Getenv("RIVUS_INSTANCE_ID")),
		strings.TrimSpace(configured),
	} {
		if candidate != "" {
			return truncateRuntimeInstanceID(candidate)
		}
	}
	if hostname, err := os.Hostname(); err == nil && strings.TrimSpace(hostname) != "" {
		return truncateRuntimeInstanceID(hostname)
	}
	return truncateRuntimeInstanceID(fmt.Sprintf("%s-%d", strings.TrimSpace(role), os.Getpid()))
}

func truncateRuntimeInstanceID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 255 {
		return value[:255]
	}
	return value
}
