package connector

import (
	"fmt"
	"sync"
)

type SourceFactory func(jctx JobContext, cfg any) (Source, error)
type SinkFactory func(jctx JobContext, cfg any) (Sink, error)

type Registry struct {
	mu      sync.RWMutex
	sources map[string]SourceFactory
	sinks   map[string]SinkFactory
}

func NewRegistry() *Registry {
	return &Registry{
		sources: make(map[string]SourceFactory),
		sinks:   make(map[string]SinkFactory),
	}
}

func (r *Registry) RegisterSource(typ string, f SourceFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sources[typ] = f
}

func (r *Registry) RegisterSink(typ string, f SinkFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sinks[typ] = f
}

func (r *Registry) NewSource(typ string, jctx JobContext, cfg any) (Source, error) {
	r.mu.RLock()
	f, ok := r.sources[typ]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown source connector type: %q", typ)
	}
	return f(jctx, cfg)
}

func (r *Registry) NewSink(typ string, jctx JobContext, cfg any) (Sink, error) {
	r.mu.RLock()
	f, ok := r.sinks[typ]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown sink connector type: %q", typ)
	}
	return f(jctx, cfg)
}
