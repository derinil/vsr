package main

import (
	"context"

	"github.com/derinil/vsr"
)

const maxIdleClients = 256

type pool struct {
	opts vsr.ClientOptions
	idle chan *vsr.Client
}

func newPool(opts vsr.ClientOptions) (*pool, error) {
	c, err := vsr.NewClient(opts)
	if err != nil {
		return nil, err
	}
	p := &pool{opts: opts, idle: make(chan *vsr.Client, maxIdleClients)}
	p.idle <- c
	return p, nil
}

func (p *pool) invoke(ctx context.Context, op []byte) ([]byte, error) {
	c, err := p.get()
	if err != nil {
		return nil, err
	}
	defer p.put(c)
	return c.Invoke(ctx, op)
}

func (p *pool) get() (*vsr.Client, error) {
	select {
	case c := <-p.idle:
		return c, nil
	default:
		return vsr.NewClient(p.opts)
	}
}

func (p *pool) put(c *vsr.Client) {
	select {
	case p.idle <- c:
	default:
		c.Close()
	}
}

func (p *pool) close() {
	for {
		select {
		case c := <-p.idle:
			c.Close()
		default:
			return
		}
	}
}
