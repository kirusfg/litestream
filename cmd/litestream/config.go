package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
)

type ConfigHandler struct {
	c *ReplicateCommand

	// Where to send log messages, defaults to log.Default()
	Logger *log.Logger
}

func NewConfigHandler(c *ReplicateCommand) *ConfigHandler {
	return &ConfigHandler{
		c:      c,
		Logger: log.Default(),
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
	h.Logger.Printf("Checking databases %#v\n", newConfig.DBs)
	for _, newDBConfig := range h.c.Config.DBs {
		// Keep track of existing or add new databases
		h.Logger.Printf("Checking database %s\n", newDBConfig.Path)
		action := Add
		for _, oldDB := range h.c.DBs {
			if newDBConfig.Path == oldDB.Path() {
				// Leave as is, keep replicating
				action = Keep
				break
			}
		}
		if action == Add {
			db, err := NewDBFromConfig(newDBConfig)
			h.Logger.Printf("Adding database %s with sync-interval of %s\n", newDBConfig.Path, newDBConfig.MonitorInterval)
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
			h.Logger.Printf("Opened database %s for replication\n", db.Path())
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
			if err := oldDB.Close(); err != nil {
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
			h.Logger.Printf("Closed database %s\n", oldDB.Path())
		}
	}

	w.WriteHeader(200)
	w.Write([]byte(fmt.Sprintf("Success! Replicating databases: %#v", h.c.DBs)))
}
