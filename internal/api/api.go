package api

import "github.com/knightfall22/distributed_KV/internal/store"

type Command struct {
	Kind    ComandKind `json:"kind"`
	Key     string     `json:"key"`
	Value   string     `json:"value"`
	Compare string     `json:"compare"`
}

// Response is the response to a command. Previous is set for CAS, PUT, and APPEND. Value is set for GET
type Response struct {
	Status        string `json:"status"`
	Key           string `json:"key"`
	PreviousValue string `json:"previous,omitempty"`
	Value         string `json:"value,omitempty"`
}

type ComandKind int

const (
	CommandPut ComandKind = iota
	CommandGet
	CommandCAS
	CommandAppend
)

func (c ComandKind) String() string {
	return [...]string{"PUT", "GET", "CAS", "APPEND"}[c]
}

type ApiService struct {
	store *store.Store
	port  string
}
