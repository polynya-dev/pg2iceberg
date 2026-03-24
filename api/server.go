package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/hasyimibhar/pg2iceberg/config"
	"github.com/hasyimibhar/pg2iceberg/pipeline"
)

// Server is the multi-tenant HTTP API server.
type Server struct {
	mgr    *pipeline.Manager
	addr   string
	server *http.Server
}

func NewServer(mgr *pipeline.Manager, addr string) *Server {
	s := &Server{mgr: mgr, addr: addr}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/pipelines", s.handlePipelines)
	mux.HandleFunc("/api/v1/pipelines/", s.handlePipeline)

	s.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	return s
}

// Run starts the HTTP server and blocks until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s.server.Shutdown(shutdownCtx)
	}()

	log.Printf("[api] listening on %s", s.addr)
	if err := s.server.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

// handlePipelines handles POST /api/v1/pipelines and GET /api/v1/pipelines.
func (s *Server) handlePipelines(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listPipelines(w, r)
	case http.MethodPost:
		s.createPipeline(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handlePipeline routes /api/v1/pipelines/{id}[/tables[/{name}]].
func (s *Server) handlePipeline(w http.ResponseWriter, r *http.Request) {
	// Parse path: /api/v1/pipelines/{id}[/tables[/{name}]]
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/pipelines/")
	parts := strings.SplitN(path, "/", 3)

	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "pipeline ID required", http.StatusBadRequest)
		return
	}

	id := parts[0]

	// /api/v1/pipelines/{id}/tables or /api/v1/pipelines/{id}/tables/{name}
	if len(parts) >= 2 && parts[1] == "tables" {
		tableName := ""
		if len(parts) == 3 {
			tableName = parts[2]
		}
		s.handleTables(w, r, id, tableName)
		return
	}

	// /api/v1/pipelines/{id}
	switch r.Method {
	case http.MethodGet:
		s.getPipeline(w, r, id)
	case http.MethodDelete:
		s.deletePipeline(w, r, id)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleTables(w http.ResponseWriter, r *http.Request, pipelineID, tableName string) {
	switch r.Method {
	case http.MethodPost:
		s.addTable(w, r, pipelineID)
	case http.MethodDelete:
		if tableName == "" {
			http.Error(w, "table name required", http.StatusBadRequest)
			return
		}
		s.removeTable(w, r, pipelineID, tableName)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// --- Handler implementations ---

type createPipelineRequest struct {
	ID     string         `json:"id"`
	Config *config.Config `json:"config"`
}

func (s *Server) createPipeline(w http.ResponseWriter, r *http.Request) {
	var req createPipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.ID == "" {
		jsonError(w, "id is required", http.StatusBadRequest)
		return
	}
	if req.Config == nil {
		jsonError(w, "config is required", http.StatusBadRequest)
		return
	}
	req.Config.ApplyDefaults()

	if err := s.mgr.Create(r.Context(), req.ID, req.Config); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	info, _ := s.mgr.Get(req.ID)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(info)
}

func (s *Server) listPipelines(w http.ResponseWriter, r *http.Request) {
	infos := s.mgr.List()
	json.NewEncoder(w).Encode(infos)
}

func (s *Server) getPipeline(w http.ResponseWriter, r *http.Request, id string) {
	info, err := s.mgr.Get(id)
	if err != nil {
		jsonError(w, err.Error(), http.StatusNotFound)
		return
	}
	json.NewEncoder(w).Encode(info)
}

func (s *Server) deletePipeline(w http.ResponseWriter, r *http.Request, id string) {
	if err := s.mgr.Delete(r.Context(), id); err != nil {
		jsonError(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type addTableRequest struct {
	Table string `json:"table"`
}

func (s *Server) addTable(w http.ResponseWriter, r *http.Request, pipelineID string) {
	var req addTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Table == "" {
		jsonError(w, "table is required", http.StatusBadRequest)
		return
	}

	if err := s.mgr.AddTable(r.Context(), pipelineID, req.Table); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "table": req.Table})
}

func (s *Server) removeTable(w http.ResponseWriter, r *http.Request, pipelineID, tableName string) {
	if err := s.mgr.RemoveTable(r.Context(), pipelineID, tableName); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
