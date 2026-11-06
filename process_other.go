//go:build !unix

package vsr

import "time"

func cpuTime() time.Duration { return 0 }
