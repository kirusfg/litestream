package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
)

type ConfigHandler struct {
	c *ReplicateCommand

	// Where to send log messages, defaults to log.Default()
	Logger *slog.Logger
}

func NewConfigHandler(c *ReplicateCommand) *ConfigHandler {
	return &ConfigHandler{
		c:      c,
		Logger: slog.Default(),
	}
}

func (h *ConfigHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.c == nil {
		w.WriteHeader(500)
		w.Write([]byte("The config handler has not been initialized properly (ReplicateCommand is nil)"))
		return
	}

	fs := flag.NewFlagSet("litestream-replicate", flag.ContinueOnError)
	configPath, noExpandEnv := registerConfigFlag(fs)
	if *configPath == "" {
		*configPath = DefaultConfigPath()
	}

	var newConfig Config
	var err error
	if newConfig, err = ReadConfigFile(*configPath, !*noExpandEnv); err != nil {
		w.WriteHeader(500)
		w.Write([]byte(fmt.Sprintf("Error reading config file: %s", err)))
		return
	}

	type Action int
	const (
		Keep   Action = 0
		Add    Action = 1
		Remove Action = 2
	)

	// Update the config
	h.c.Config = newConfig

	// Take action on each of the databases in the new config
	// h.Logger.Info(fmt.Sprintf("Checking databases %#v\n", newConfig.DBs))
	for _, newDBConfig := range h.c.Config.DBs {
		// Keep track of existing or add new databases
		h.Logger.Info(fmt.Sprintf("Checking database %s\n", newDBConfig.Path))
		action := Add
		for _, oldDB := range h.c.DBs {
			if newDBConfig.Path == oldDB.Path() {
				action = Keep
				break
			}
		}
		if action == Add {
			h.Logger.Info(fmt.Sprintf("Adding database %s", newDBConfig.Path))
			db, err := NewDBFromConfig(newDBConfig)
			if err != nil {
				w.WriteHeader(500)
				w.Write([]byte(fmt.Sprintf("Error opening database %s for replication: %s", newDBConfig.Path, err)))
				return
			}

			// Open database & attach to program
			if err := db.Open(); err != nil {
				w.WriteHeader(500)
				w.Write([]byte(fmt.Sprintf("Error opening database %s for replication: %s", newDBConfig.Path, err)))
				return
			}
			h.c.DBs = append(h.c.DBs, db)
			h.Logger.Info(fmt.Sprintf("Opened database %s for replication", db.Path()))
		} else if action == Keep {
			h.Logger.Info(fmt.Sprintf("Keeping database %s\n", newDBConfig.Path))
		}
	}

	// Close databases that are no longer being tracked
	for _, oldDB := range h.c.DBs {
		action := Remove
		for _, newDB := range h.c.Config.DBs {
			if oldDB.Path() == newDB.Path {
				action = Keep
				break
			}
		}
		if action == Remove {
			h.Logger.Info(fmt.Sprintf("Removing database %s\n", oldDB.Path()))
			if err := oldDB.Close(context.Background()); err != nil {
				w.WriteHeader(500)
				w.Write([]byte(fmt.Sprintf("Error closing database %s: %s", oldDB.Path(), err)))
				return
			}
			index := 0
			for _, db := range h.c.DBs {
				if db != oldDB {
					h.c.DBs[index] = db
					index++
				}
			}
			h.c.DBs = h.c.DBs[:index]
			h.Logger.Info(fmt.Sprintf("Closed database %s\n", oldDB.Path()))
		}
	}

	w.WriteHeader(200)
	// w.Write([]byte(fmt.Sprintf("Success! Replicating databases: %#v", h.c.DBs)))
}
