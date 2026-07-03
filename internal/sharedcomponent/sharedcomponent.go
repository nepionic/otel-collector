// Package sharedcomponent lets multiple OTel receiver signal wrappers (e.g. one
// per pipeline) share a single underlying component instance keyed by
// component.ID, so Start/Shutdown of the real resource happen at most once no
// matter how many pipelines reference the same receiver configuration.
package sharedcomponent

import (
	"context"
	"sync"

	"go.opentelemetry.io/collector/component"
)

// Map caches component instances keyed by K (component.ID in practice).
type Map[K comparable, V component.Component] struct {
	mu         sync.Mutex
	components map[K]*Component[V]
}

// NewMap creates an empty Map.
func NewMap[K comparable, V component.Component]() *Map[K, V] {
	return &Map[K, V]{components: map[K]*Component[V]{}}
}

// LoadOrStore returns the existing *Component[V] for key, or creates one via
// create() and stores it. create() is only invoked when key is not already
// present.
func (m *Map[K, V]) LoadOrStore(key K, create func() (V, error)) (*Component[V], error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if c, ok := m.components[key]; ok {
		return c, nil
	}

	v, err := create()
	if err != nil {
		return nil, err
	}

	c := &Component[V]{
		comp: v,
		removeFunc: func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			delete(m.components, key)
		},
	}
	m.components[key] = c
	return c, nil
}

// Component wraps V so that Start/Shutdown run at most once across every
// caller that shares this instance (e.g. one per signal-specific
// receiver.Logs/receiver.Metrics wrapper referencing the same receiver ID).
type Component[V component.Component] struct {
	comp V

	startOnce sync.Once
	startErr  error

	stopOnce sync.Once
	stopErr  error

	removeFunc func()
}

// Unwrap returns the underlying instance so callers can invoke
// domain-specific methods not part of component.Component.
func (c *Component[V]) Unwrap() V { return c.comp }

// Start implements component.Component. The underlying comp.Start runs at
// most once; subsequent calls return the same result.
func (c *Component[V]) Start(ctx context.Context, host component.Host) error {
	c.startOnce.Do(func() { c.startErr = c.comp.Start(ctx, host) })
	return c.startErr
}

// Shutdown implements component.Component. The underlying comp.Shutdown runs
// at most once; subsequent calls return the same result. After the first
// call, the instance is removed from its owning Map so a later LoadOrStore
// with the same key creates a fresh instance.
func (c *Component[V]) Shutdown(ctx context.Context) error {
	c.stopOnce.Do(func() {
		c.stopErr = c.comp.Shutdown(ctx)
		c.removeFunc()
	})
	return c.stopErr
}
