package vsrmon

import (
	"context"
	"embed"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"time"
)

//go:embed web
var webFS embed.FS

func newHandler(m *Monitor) (http.Handler, error) {
	site, err := fs.Sub(webFS, "web")
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/cluster", m.serveCluster)
	mux.HandleFunc("GET /api/stream", m.serveStream)
	mux.HandleFunc("POST /api/reconfigure", m.serveReconfigure)
	mux.Handle("GET /", http.FileServerFS(site))
	return mux, nil
}

func (m *Monitor) serveCluster(w http.ResponseWriter, _ *http.Request) {
	data := m.latest.Load()
	if data == nil {
		http.Error(w, "no poll completed", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(*data)
}

func (m *Monitor) serveStream(w http.ResponseWriter, r *http.Request) {
	ch := m.broker.subscribe()
	defer m.broker.unsubscribe(ch)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	rc := http.NewResponseController(w)
	if err := rc.Flush(); err != nil {
		return
	}
	if data := m.latest.Load(); data != nil && writeEvent(w, rc, *data) != nil {
		return
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case data := <-ch:
			if writeEvent(w, rc, data) != nil {
				return
			}
		}
	}
}

func writeEvent(w io.Writer, rc *http.ResponseController, data []byte) error {
	if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
		return err
	}
	return rc.Flush()
}

const shutdownTimeout = 5 * time.Second

// Serve runs m and serves its dashboard on listen until ctx is done.
func Serve(ctx context.Context, m *Monitor, listen string, logger *slog.Logger) error {
	handler, err := newHandler(m)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}

	go m.Run(ctx)
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	logger.Info("dashboard listening", "addr", ln.Addr().String(), "nodes", len(m.opts.Nodes))

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
