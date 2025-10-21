package service

import (
	"fmt"
	"log"
	"net/http"

	"github.com/knightfall22/distributed_KV/internal/store"
)

type Service struct {
	//inmemory datastore
	store *store.Store

	//port the service runs on
	port string

	// srv is the HTTP server exposed by the service to the external world.
	srv *http.Server
}

func NewService(store *store.Store, port string) *Service {
	return &Service{
		store: store,
		port:  port,
	}
}

func (s *Service) ServeHTTP() {
	if s.srv != nil {
		panic("server already running")
	}

	mux := http.NewServeMux()
	//Todo insert functions
	s.srv = &http.Server{
		Addr:    fmt.Sprintf(":%q", s.port),
		Handler: mux,
	}

	go func() {
		if err := s.srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
		s.srv = nil
	}()
}
