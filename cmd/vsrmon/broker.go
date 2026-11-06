package main

import "sync"

type broker struct {
	mu   sync.Mutex
	subs map[chan []byte]struct{}
}

func (b *broker) subscribe() chan []byte {
	ch := make(chan []byte, 1)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subs == nil {
		b.subs = make(map[chan []byte]struct{})
	}
	b.subs[ch] = struct{}{}
	return ch
}

func (b *broker) unsubscribe(ch chan []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.subs, ch)
}

func (b *broker) publish(data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs {
		select {
		case <-ch:
		default:
		}
		select {
		case ch <- data:
		default:
		}
	}
}

func (b *broker) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subs)
}
