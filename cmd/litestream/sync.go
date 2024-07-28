package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/benbjohnson/litestream"
)

type SyncRequest struct {
	DatabasePath   string `json:"database-path"`
	ReplicaName    string `json:"replica-name"`
	Checkpoint     bool   `json:"checkpoint"`
	CheckpointMode string `json:"checkpoint-mode"`
}

type SyncResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
}

type SyncHandler struct {
	// Context to execute checkpoints in
	ctx context.Context

	// The command running the replication process
	c *ReplicateCommand

	// Where to send log messages, defaults to log.Default()
	Logger *slog.Logger
}

func NewSyncHandler(ctx context.Context, c *ReplicateCommand) *SyncHandler {
	return &SyncHandler{
		ctx:    ctx,
		c:      c,
		Logger: slog.Default(),
	}
}

func (h *SyncHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.c == nil {
		w.WriteHeader(500)
		res := SyncResponse{Status: "error", Error: "sync handler has not been initialized properly (ReplicateCommand is nil)"}
		json.NewEncoder(w).Encode(res)
		return
	}

	// Check if the request is a POST
	if r.Method != "POST" {
		w.WriteHeader(405)
		res := SyncResponse{Status: "error", Error: "method not allowed"}
		json.NewEncoder(w).Encode(res)
		return
	}

	// Parse request
	var req SyncRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(400)
		res := SyncResponse{Status: "error", Error: "invalid request"}
		json.NewEncoder(w).Encode(res)
		return
	}

	// Check if the requested database is being replicated
	var db *litestream.DB
	for _, cdb := range h.c.DBs {
		if cdb.Path() == req.DatabasePath {
			db = cdb
			break
		}
	}

	if db == nil {
		h.Logger.Info(fmt.Sprintf("database %s not found", req.DatabasePath))
		w.WriteHeader(404)
		res := SyncResponse{Status: "error", Error: fmt.Sprintf("database %s not found", req.DatabasePath)}
		json.NewEncoder(w).Encode(res)
		return
	}

	// Check if the requested database replica exists
	var rep *litestream.Replica
	for _, crep := range db.Replicas {
		if crep.Name() == req.ReplicaName {
			rep = crep
			break
		}
	}

	if rep == nil {
		h.Logger.Info(fmt.Sprintf("replica %s for database %s not found", req.ReplicaName, req.DatabasePath))
		w.WriteHeader(404)
		res := SyncResponse{Status: "error", Error: fmt.Sprintf("replica %s for database %s not found", req.ReplicaName, req.DatabasePath)}
		json.NewEncoder(w).Encode(res)
		return
	}

	// Check the requested checkpoint mode
	mode := litestream.CheckpointModePassive
	if req.CheckpointMode == "FULL" {
		mode = litestream.CheckpointModeFull
	} else if req.CheckpointMode == "RESTART" {
		mode = litestream.CheckpointModeRestart
	} else if req.CheckpointMode == "TRUNCATE" {
		mode = litestream.CheckpointModeTruncate
	}

	h.Logger.Info(fmt.Sprintf("issuing sync on database %s", req.DatabasePath))
	if err := db.Sync(h.ctx); err != nil {
		h.Logger.Info(fmt.Sprintf("error issuing sync on database %s: %s", req.DatabasePath, err))
		w.WriteHeader(500)
		res := SyncResponse{Status: "error", Error: fmt.Sprintf("error issuing sync on database %s: %s", req.DatabasePath, err)}
		json.NewEncoder(w).Encode(res)
		return
	}

	// Issue sync
	h.Logger.Info(fmt.Sprintf("issuing sync on replica %s for database %s", req.ReplicaName, req.DatabasePath))
	if err := rep.Sync(h.ctx); err != nil {
		h.Logger.Info(fmt.Sprintf("error issuing sync on replica %s for database %s: %s", req.ReplicaName, req.DatabasePath, err))
		w.WriteHeader(500)
		res := SyncResponse{Status: "error", Error: fmt.Sprintf("error issuing sync on replica %s for database %s: %s", req.ReplicaName, req.DatabasePath, err)}
		json.NewEncoder(w).Encode(res)
		return
	}

	// Issue checkpoint
	if req.Checkpoint {
		h.Logger.Info(fmt.Sprintf("issuing checkpoint on database %s", req.DatabasePath))
		if err := db.Checkpoint(h.ctx, mode); err != nil {
			h.Logger.Info(fmt.Sprintf("error issuing checkpoint on database %s: %s", req.DatabasePath, err))
			w.WriteHeader(500)
			res := SyncResponse{Status: "error", Error: fmt.Sprintf("error issuing checkpoint on database %s: %s", req.DatabasePath, err)}
			json.NewEncoder(w).Encode(res)
			return
		}
	}

	w.WriteHeader(200)
	res := SyncResponse{Status: "ok"}
	json.NewEncoder(w).Encode(res)
}
