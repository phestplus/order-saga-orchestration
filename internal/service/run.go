// Package service holds the boot sequence shared by every participant,
// so each cmd/ main stays down to its business rules.
package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/phestplus/order-saga-orchestration/internal/participant"
	"github.com/phestplus/order-saga-orchestration/internal/transport"
)

// Env reads an environment variable with a fallback.
func Env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// RunParticipant connects to NATS, subscribes the service to its subject
// pattern, serves a health endpoint, and blocks until terminated.
func RunParticipant(name, pattern string, register func(*participant.Service)) error {
	svc := participant.New(name)
	register(svc)

	conn, err := transport.Connect(Env("NATS_URL", "nats://127.0.0.1:4222"), 60*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	sub, err := transport.ServeParticipant(conn, svc, pattern)
	if err != nil {
		return err
	}
	defer func() { _ = sub.Drain() }()

	srv := healthServer(Env("PORT", "8080"), name)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("%s health server: %v", name, err)
		}
	}()

	log.Printf("%s listening on NATS pattern %q", name, pattern)
	waitForSignal()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
}

func healthServer(port, name string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "%s ok\n", name)
	})
	return &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

func waitForSignal() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}
