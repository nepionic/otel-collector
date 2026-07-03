package sharedcomponent

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
)

type fakeComp struct {
	startCount atomic.Int32
	stopCount  atomic.Int32
	startErr   error
	stopErr    error
}

func (f *fakeComp) Start(context.Context, component.Host) error {
	f.startCount.Add(1)
	return f.startErr
}

func (f *fakeComp) Shutdown(context.Context) error {
	f.stopCount.Add(1)
	return f.stopErr
}

func TestLoadOrStore_SameKeyReturnsSameInstance(t *testing.T) {
	m := NewMap[string, *fakeComp]()
	key := "ads"
	created := 0

	create := func() (*fakeComp, error) {
		created++
		require.LessOrEqual(t, created, 1, "create must only be called once for the same key")
		return &fakeComp{}, nil
	}

	c1, err := m.LoadOrStore(key, create)
	require.NoError(t, err)
	c2, err := m.LoadOrStore(key, create)
	require.NoError(t, err)

	assert.Same(t, c1, c2)
	assert.Same(t, c1.Unwrap(), c2.Unwrap())
}

func TestStart_OnlyRunsOnce(t *testing.T) {
	m := NewMap[string, *fakeComp]()
	create := func() (*fakeComp, error) { return &fakeComp{}, nil }

	c1, err := m.LoadOrStore("ads", create)
	require.NoError(t, err)
	c2, err := m.LoadOrStore("ads", create)
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); errs[0] = c1.Start(context.Background(), nil) }()
	go func() { defer wg.Done(); errs[1] = c2.Start(context.Background(), nil) }()
	wg.Wait()

	assert.NoError(t, errs[0])
	assert.NoError(t, errs[1])
	assert.EqualValues(t, 1, c1.Unwrap().startCount.Load())
}

func TestShutdown_OnlyRunsOnce(t *testing.T) {
	m := NewMap[string, *fakeComp]()
	create := func() (*fakeComp, error) { return &fakeComp{}, nil }

	c1, err := m.LoadOrStore("ads", create)
	require.NoError(t, err)
	c2, err := m.LoadOrStore("ads", create)
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); errs[0] = c1.Shutdown(context.Background()) }()
	go func() { defer wg.Done(); errs[1] = c2.Shutdown(context.Background()) }()
	wg.Wait()

	assert.NoError(t, errs[0])
	assert.NoError(t, errs[1])
	assert.EqualValues(t, 1, c1.Unwrap().stopCount.Load())

	// The key was removed after Shutdown, so a fresh LoadOrStore creates a new instance.
	c3, err := m.LoadOrStore("ads", create)
	require.NoError(t, err)
	assert.NotSame(t, c1.Unwrap(), c3.Unwrap())
}

func TestLoadOrStore_CreateErrorNotCached(t *testing.T) {
	m := NewMap[string, *fakeComp]()
	calls := 0

	_, err := m.LoadOrStore("ads", func() (*fakeComp, error) {
		calls++
		return nil, assert.AnError
	})
	require.Error(t, err)

	_, err = m.LoadOrStore("ads", func() (*fakeComp, error) {
		calls++
		return &fakeComp{}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, calls, "second LoadOrStore must retry create since the first attempt failed")
}
