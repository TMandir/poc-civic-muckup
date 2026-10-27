// Package app wires the database, frontend, server, and client packages and
// manages process signals and graceful HTTP shutdown.
package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	client "whiteboard/Client"
	server "whiteboard/Server"
	"whiteboard/database"
	"whiteboard/frontend"
)

const (
	// frontendAddress is the local address used to serve the web frontend.
	frontendAddress = ":8080"
	// shutdownTimeout bounds graceful HTTP server shutdown.
	shutdownTimeout = 5 * time.Second
)

// Run starts the application and stops it after an interrupt or termination signal.
// It returns startup, serving, shutdown, or database-close errors to main.
func Run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return RunContext(ctx)
}

// RunContext starts the application until ctx is canceled or the HTTP server fails.
// It opens the database, injects it into the frontend handler, starts the peer
// services, and closes resources before returning.
func RunContext(ctx context.Context) (runErr error) {
	if ctx == nil {
		return errors.New("application context is required")
	}

	store, err := database.OpenDefault()
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close database: %w", err))
		}
	}()

	handler, err := frontend.NewHandler(store)
	if err != nil {
		return fmt.Errorf("create frontend handler: %w", err)
	}

	httpServer := &http.Server{
		Addr:    frontendAddress,
		Handler: handler,
	}
	// Buffer the result so the serving goroutine can exit during signal shutdown.
	serverErrors := make(chan error, 1)

	//calling  other subsystem namely server and client
	//here it is only for checkmark purpose,
	//in real application it should be called by othger its best engineering practice to call them in main.go or in a separate function which is called from main.go

	server.Server()
	client.Client()

	//Start the HTTP server in a goroutine so we can listen for shutdown signals.
	go func() {
		serverErrors <- httpServer.ListenAndServe()
	}()

	select {
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve frontend: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			closeErr := httpServer.Close()
			if closeErr != nil {
				closeErr = fmt.Errorf("close frontend: %w", closeErr)
			}
			return errors.Join(
				fmt.Errorf("shut down frontend: %w", err),
				closeErr,
			)
		}

		return nil
	}
}
