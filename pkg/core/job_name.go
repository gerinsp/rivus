package core

import (
	"context"
	"errors"
	"github.com/gerinsp/rivus/pkg/config"
	"github.com/gerinsp/rivus/pkg/meta"
	"time"
)

var ErrJobRenameUnsupported = errors.New("job store does not support rename")

func (j *Job) Name() string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	if j.displayName != "" {
		return j.displayName
	}
	if j.Config != nil {
		return j.Config.Name
	}
	return ""
}
func (j *Job) DisplayConfig() *config.JobConfig {
	j.mu.RLock()
	defer j.mu.RUnlock()
	if j.Config == nil {
		return nil
	}
	cfg := *j.Config
	if j.displayConfig != nil {
		cfg = *j.displayConfig
	}
	if j.displayName != "" {
		cfg.Name = j.displayName
	}
	return &cfg
}
func (m *JobManager) RequestRename(id, name string) error {
	name, err := meta.ValidateJobName(name)
	if err != nil {
		return err
	}
	m.mu.RLock()
	job := m.jobs[id]
	m.mu.RUnlock()
	if job == nil {
		return ErrJobNotFound
	}
	release, err := m.beginDurableLifecycle(job)
	if err != nil {
		return err
	}
	defer release()
	if m.jobStore != nil {
		if err := m.ensureJobStoreReady(context.Background()); err != nil {
			return err
		}
		store, ok := m.jobStore.(meta.JobNameStore)
		if !ok {
			return ErrJobRenameUnsupported
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		changed, err := store.RenameJob(ctx, id, name)
		if err != nil {
			return err
		}
		if !changed {
			return ErrJobNotFound
		}
	}
	job.mu.Lock()
	job.displayName = name
	job.Updated = time.Now()
	job.mu.Unlock()
	return nil
}
func (j *Job) CreatedTime() time.Time { j.mu.RLock(); defer j.mu.RUnlock(); return j.Created }
func (j *Job) UpdatedTime() time.Time { j.mu.RLock(); defer j.mu.RUnlock(); return j.Updated }
