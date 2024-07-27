package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/benbjohnson/litestream"
)

type SnapshotRequest struct {
	DatabasePath string `json:"database-path"`
	ReplicaName  string `json:"replica-name"`
	Cleanup      bool   `json:"cleanup"`
}

type SnapshotResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
}

type SnapshotHandler struct {
	// Context to execute checkpoints in
	ctx context.Context

	// The command running the replication process
	c *ReplicateCommand

	// Where to send log messages, defaults to log.Default()
	Logger *slog.Logger
}

func NewSnapshotHandler(ctx context.Context, c *ReplicateCommand) *SnapshotHandler {
	return &SnapshotHandler{
		ctx:    ctx,
		c:      c,
		Logger: slog.Default(),
	}
}

func (h *SnapshotHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.c == nil {
		w.WriteHeader(500)
		res := SnapshotResponse{Status: "error", Error: "snapshot handler has not been initialized properly (ReplicateCommand is nil)"}
		json.NewEncoder(w).Encode(res)
		return
	}

	// Check if the request is a POST
	if r.Method != "POST" {
		w.WriteHeader(405)
		res := SnapshotResponse{Status: "error", Error: "method not allowed"}
		json.NewEncoder(w).Encode(res)
		return
	}

	// Parse request
	var req SnapshotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(400)
		res := SnapshotResponse{Status: "error", Error: "invalid request"}
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
		res := SnapshotResponse{Status: "error", Error: fmt.Sprintf("database %s not found", req.DatabasePath)}
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
		res := SnapshotResponse{Status: "error", Error: fmt.Sprintf("replica %s for database %s not found", req.ReplicaName, req.DatabasePath)}
		json.NewEncoder(w).Encode(res)
		return
	}

	// Issue snapshot
	h.Logger.Info(fmt.Sprintf("creating snapshot of replica %s for database %s", req.ReplicaName, req.DatabasePath))
	if _, err := rep.Snapshot(h.ctx); err != nil {
		h.Logger.Info(fmt.Sprintf("error creating snapshot of replica %s for database %s: %s", req.ReplicaName, req.DatabasePath, err))
		w.WriteHeader(500)
		res := SnapshotResponse{Status: "error", Error: fmt.Sprintf("error issuing snapshot on replica %s for database %s: %s", req.ReplicaName, req.DatabasePath, err)}
		json.NewEncoder(w).Encode(res)
		return
	}

	if req.Cleanup {
		h.Logger.Info(fmt.Sprintf("retaining snapshots of replica %s for database %s", req.ReplicaName, req.DatabasePath))
		if err := rep.EnforceRetention(h.ctx); err != nil {
			h.Logger.Info(fmt.Sprintf("error retaining snapshots of replica %s for database %s: %s", req.ReplicaName, req.DatabasePath, err))
			w.WriteHeader(500)
			res := SnapshotResponse{Status: "error", Error: fmt.Sprintf("error issuing retain on replica %s for database %s: %s", req.ReplicaName, req.DatabasePath, err)}
			json.NewEncoder(w).Encode(res)
			return
		}
	}

	w.WriteHeader(200)
	res := SnapshotResponse{Status: "ok"}
	json.NewEncoder(w).Encode(res)
}
