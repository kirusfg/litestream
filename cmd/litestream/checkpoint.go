package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/benbjohnson/litestream"
)

type CheckpointRequest struct {
	DatabasePath string `json:"database-path"`
	ReplicaName  string `json:"replica-name"`
	Mode         string `json:"mode"`
	Sync         bool   `json:"sync"`
}

type CheckpointResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
}

type CheckpointHandler struct {
	// Context to execute checkpoints in
	ctx context.Context

	// The command running the replication process
	c *ReplicateCommand

	// Where to send log messages, defaults to log.Default()
	Logger *slog.Logger
}

func NewCheckpointHandler(ctx context.Context, c *ReplicateCommand) *CheckpointHandler {
	return &CheckpointHandler{
		ctx:    ctx,
		c:      c,
		Logger: slog.Default(),
	}
}

func (h *CheckpointHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.c == nil {
		w.WriteHeader(500)
		res := CheckpointResponse{Status: "error", Error: "checkpoint handler has not been initialized properly (ReplicateCommand is nil)"}
		json.NewEncoder(w).Encode(res)
		return
	}

	// Check if the request is a POST
	if r.Method != "POST" {
		w.WriteHeader(405)
		res := CheckpointResponse{Status: "error", Error: "method not allowed"}
		json.NewEncoder(w).Encode(res)
		return
	}

	// Parse request
	var req CheckpointRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(400)
		res := CheckpointResponse{Status: "error", Error: "invalid request"}
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
		res := CheckpointResponse{Status: "error", Error: fmt.Sprintf("database %s not found", req.DatabasePath)}
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
		res := CheckpointResponse{Status: "error", Error: fmt.Sprintf("replica %s for database %s not found", req.ReplicaName, req.DatabasePath)}
		json.NewEncoder(w).Encode(res)
		return
	}

	// Check if the requested mode is valid
	mode := litestream.CheckpointModePassive
	if req.Mode == "FULL" {
		mode = litestream.CheckpointModeFull
	} else if req.Mode == "RESTART" {
		mode = litestream.CheckpointModeRestart
	} else if req.Mode == "TRUNCATE" {
		mode = litestream.CheckpointModeTruncate
	}

	// Issue checkpoint
	h.Logger.Info(fmt.Sprintf("issuing checkpoint on database %s", req.DatabasePath))
	if err := db.Checkpoint(h.ctx, mode); err != nil {
		h.Logger.Info(fmt.Sprintf("error issuing checkpoint on database %s: %s", req.DatabasePath, err))
		w.WriteHeader(500)
		res := CheckpointResponse{Status: "error", Error: fmt.Sprintf("error issuing checkpoint on database %s: %s", req.DatabasePath, err)}
		json.NewEncoder(w).Encode(res)
		return
	}

	// Issue sync
	if req.Sync {
		h.Logger.Info(fmt.Sprintf("issuing sync on replica %s for database %s", req.ReplicaName, req.DatabasePath))
		if err := rep.Sync(h.ctx); err != nil {
			h.Logger.Info(fmt.Sprintf("error issuing sync on replica %s for database %s: %s", req.ReplicaName, req.DatabasePath, err))
			w.WriteHeader(500)
			res := CheckpointResponse{Status: "error", Error: fmt.Sprintf("error issuing sync on replica %s for database %s: %s", req.ReplicaName, req.DatabasePath, err)}
			json.NewEncoder(w).Encode(res)
			return
		}
	}

	w.WriteHeader(200)
	res := CheckpointResponse{Status: "ok"}
	json.NewEncoder(w).Encode(res)
}
