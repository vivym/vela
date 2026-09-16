// A disposable process-progress probe, never a model backend or Worker.
package main

import (
	"encoding/json"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	path := os.Getenv("VELA_DIAGNOSTIC_PROGRESS_FILE")
	if path == "" {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
		<-stop
		return
	}
	signals := make(chan os.Signal, 8)
	signal.Notify(signals, syscall.SIGUSR1)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	state := struct {
		Ticks   int `json:"ticks"`
		Signals int `json:"signals"`
	}{}
	for {
		select {
		case <-signals:
			state.Signals++
		case <-ticker.C:
			state.Ticks++
		}
		wire, err := json.Marshal(state)
		if err != nil {
			panic(err)
		}
		if err := os.WriteFile(path+".tmp", wire, 0600); err != nil {
			panic(err)
		}
		if err := os.Rename(path+".tmp", path); err != nil {
			panic(err)
		}
	}
}
