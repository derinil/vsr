package main

import (
	"embed"
	"fmt"
	"io"
	"io/fs"
	"net/http"
)

//go:embed web
var webFS embed.FS

func newHandler(m *monitor) (http.Handler, error) {
	site, err := fs.Sub(webFS, "web")
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/cluster", m.serveCluster)
	mux.HandleFunc("GET /api/stream", m.serveStream)
	mux.Handle("GET /", http.FileServerFS(site))
	return mux, nil
}

func (m *monitor) serveCluster(w http.ResponseWriter, _ *http.Request) {
	data := m.latest.Load()
	if data == nil {
		http.Error(w, "no poll completed", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(*data)
}

func (m *monitor) serveStream(w http.ResponseWriter, r *http.Request) {
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
