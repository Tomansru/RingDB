package pool

import (
	"sync"
)

type Pool[T any] struct {
	pool sync.Pool
}

func New[T any](fn func() T) *Pool[T] {
	return &Pool[T]{
		pool: sync.Pool{
			New: func() any {
				return fn()
			},
		},
	}
}

func NewNil[T any]() *Pool[T] {
	return &Pool[T]{
		pool: sync.Pool{},
	}
}

func (p *Pool[T]) Get() T {
	if p.pool.New == nil {
		var zero T
		return zero
	}
	return p.pool.Get().(T)
}

func (p *Pool[T]) Put(x T) {
	p.pool.Put(x)
}
