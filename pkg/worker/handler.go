package worker

import (
	"context"
	"errors"
	"sync"

	"github.com/abdul-hamid-achik/job-queue/pkg/job"
)

var (
	ErrHandlerNotFound = errors.New("handler not found for job type")
	ErrHandlerExists   = errors.New("handler already registered for job type")
)

type HandlerFunc func(ctx context.Context, j *job.Job) error

type Middleware func(next HandlerFunc) HandlerFunc

type Registry struct {
	mu         sync.RWMutex
	handlers   map[string]HandlerFunc
	middleware []Middleware
}

func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]HandlerFunc),
	}
}

func (r *Registry) Register(jobType string, handler HandlerFunc) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[jobType]; exists {
		return ErrHandlerExists
	}

	r.handlers[jobType] = handler
	return nil
}

func (r *Registry) MustRegister(jobType string, handler HandlerFunc) {
	if err := r.Register(jobType, handler); err != nil {
		panic(err)
	}
}

func (r *Registry) Get(jobType string) (HandlerFunc, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	handler, exists := r.handlers[jobType]
	if !exists {
		return nil, ErrHandlerNotFound
	}

	wrapped := handler
	for i := len(r.middleware) - 1; i >= 0; i-- {
		wrapped = r.middleware[i](wrapped)
	}

	return wrapped, nil
}

func (r *Registry) Use(mw ...Middleware) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.middleware = append(r.middleware, mw...)
}

func (r *Registry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	return types
}

func (r *Registry) Has(jobType string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.handlers[jobType]
	return exists
}

func (r *Registry) Unregister(jobType string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.handlers, jobType)
}

func (r *Registry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers = make(map[string]HandlerFunc)
	r.middleware = nil
}

func Chain(middleware ...Middleware) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		for i := len(middleware) - 1; i >= 0; i-- {
			next = middleware[i](next)
		}
		return next
	}
}
