package main

import (
	"context"

	"github.com/derinil/vsr/vsrclient"
)

const maxIdleClients = 256

type pool struct {
	opts vsrclient.Options
	idle chan *vsrclient.Client
}

func newPool(opts vsrclient.Options) (*pool, error) {
	c, err := vsrclient.New(opts)
	if err != nil {
		return nil, err
	}
	p := &pool{opts: opts, idle: make(chan *vsrclient.Client, maxIdleClients)}
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

func (p *pool) get() (*vsrclient.Client, error) {
	select {
	case c := <-p.idle:
		return c, nil
	default:
		return vsrclient.New(p.opts)
	}
}

func (p *pool) put(c *vsrclient.Client) {
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
