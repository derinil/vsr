package vsr

import (
	"runtime"
	"runtime/metrics"
	"sync"
	"time"

	"github.com/derinil/vsr/internal/vsrstatus"
)

const cpuSampleInterval = 250 * time.Millisecond

type processSampler struct {
	mu      sync.Mutex
	at      time.Time
	cpu     time.Duration
	percent float64
}

func (p *processSampler) sample() vsrstatus.Process {
	now, cpu := time.Now(), cpuTime()
	p.mu.Lock()
	if wall := now.Sub(p.at); wall >= cpuSampleInterval {
		if !p.at.IsZero() {
			p.percent = 100 * float64(cpu-p.cpu) / float64(wall)
		}
		p.at, p.cpu = now, cpu
	}
	percent := p.percent
	p.mu.Unlock()

	samples := []metrics.Sample{
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
		{Name: "/memory/classes/heap/objects:bytes"},
	}
	metrics.Read(samples)
	return vsrstatus.Process{
		CPUPercent: percent,
		MemBytes:   samples[0].Value.Uint64() - samples[1].Value.Uint64(),
		HeapBytes:  samples[2].Value.Uint64(),
		Goroutines: runtime.NumGoroutine(),
	}
}
