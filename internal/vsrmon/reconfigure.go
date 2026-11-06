package vsrmon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/derinil/vsr"
)

const (
	maxReconfigureBody = 64 << 10
	minConfigSize      = 3
)

type reconfigureRequest struct {
	Config []string `json:"config"`
}

type reconfigureResponse struct {
	Config []string `json:"config,omitempty"`
	Epoch  uint64   `json:"epoch,omitempty"`
	Error  string   `json:"error,omitempty"`
}

type httpError struct {
	code int
	msg  string
}

func (e *httpError) Error() string { return e.msg }

func errorf(code int, format string, args ...any) error {
	return &httpError{code: code, msg: fmt.Sprintf(format, args...)}
}

func (m *Monitor) serveReconfigure(w http.ResponseWriter, r *http.Request) {
	var req reconfigureRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxReconfigureBody)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, reconfigureResponse{Error: "invalid request body"})
		return
	}
	epoch, err := m.reconfigure(r.Context(), req.Config)
	if err != nil {
		code := http.StatusBadGateway
		if he, ok := errors.AsType[*httpError](err); ok {
			code = he.code
		} else if errors.Is(err, context.DeadlineExceeded) {
			code = http.StatusGatewayTimeout
		}
		writeJSON(w, code, reconfigureResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, reconfigureResponse{Config: req.Config, Epoch: epoch})
}

func (m *Monitor) reconfigure(ctx context.Context, config []string) (uint64, error) {
	m.mu.Lock()
	current := slices.Clone(m.config)
	m.mu.Unlock()
	if len(current) == 0 {
		return 0, errorf(http.StatusServiceUnavailable, "current configuration unknown: no replica reachable")
	}
	if err := checkConfig(current, config); err != nil {
		return 0, err
	}
	if !m.reconfiguring.CompareAndSwap(false, true) {
		return 0, errorf(http.StatusConflict, "a reconfiguration is already in progress")
	}
	defer m.reconfiguring.Store(false)
	if err := m.checkNewMembers(ctx, current, config); err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(ctx, m.opts.ReconfigureTimeout)
	defer cancel()
	cl, err := vsr.NewClient(vsr.ClientOptions{Config: current})
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	m.logger.Info("reconfiguring", "from", current, "to", config)
	if err := cl.Reconfigure(ctx, config); err != nil {
		m.logger.Warn("reconfiguration failed", "to", config, "err", err)
		return 0, fmt.Errorf("reconfiguration failed: %w", err)
	}
	epoch := cl.Epoch()
	m.logger.Info("reconfigured", "epoch", epoch, "config", config)
	m.mu.Lock()
	m.config, m.epoch, m.reconfigured = slices.Clone(config), epoch, time.Now()
	m.mu.Unlock()
	return epoch, nil
}

func checkConfig(current, config []string) error {
	if len(config) < minConfigSize {
		return errorf(http.StatusBadRequest, "a configuration needs at least %d replicas, got %d", minConfigSize, len(config))
	}
	seen := make(map[string]bool, len(config))
	for _, addr := range config {
		switch {
		case strings.TrimSpace(addr) != addr || addr == "":
			return errorf(http.StatusBadRequest, "invalid replica address %q", addr)
		case seen[addr]:
			return errorf(http.StatusBadRequest, "duplicate replica address %q", addr)
		}
		seen[addr] = true
	}
	if slices.Equal(current, config) {
		return errorf(http.StatusBadRequest, "configuration unchanged")
	}
	return nil
}

func (m *Monitor) checkNewMembers(ctx context.Context, current, config []string) error {
	if m.opts.StatusURL == nil {
		return nil
	}
	for _, addr := range config {
		if slices.Contains(current, addr) {
			continue
		}
		url := m.opts.StatusURL(addr)
		if url == "" {
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, m.opts.Timeout)
		n, err := m.fetchStatus(pctx, url)
		cancel()
		switch {
		case err != nil:
			return errorf(http.StatusBadRequest, "%s is not reachable at %s; start it as a joining replica first", addr, url)
		case n.Status == "shutdown":
			return errorf(http.StatusBadRequest, "%s has shut down; restart it as a joining replica first", addr)
		}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
