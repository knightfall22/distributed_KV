package store_test

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/knightfall22/distributed_KV/api"
	"github.com/knightfall22/distributed_KV/internal/store"
)

func TestInitialization(t *testing.T) {
	dir, err := os.MkdirTemp("", "store_test")
	if err != nil {
		t.Fatalf("could not create temporary directory %v\n", err)
	}

	// defer os.RemoveAll(dir)
	config := store.Config{
		RaftDir:   dir,
		Raddr:     "127.0.0.1:8080",
		Bootstrap: true,
		NodeId:    "node0",
	}

	var st *store.Store
	if st, err = store.NewStore(config); err != nil {
		t.Fatalf("could not create a store %v\n", err)
	}

	st.WaitForLeader(10 * time.Second)

	res, err := st.Write(api.Command{
		Kind:  api.CommandPut,
		Key:   "prince",
		Value: "andrew",
	})

	if err != nil {
		t.Fatal(err)
	}
	res = st.Get(api.Command{
		Kind: api.CommandGet,
		Key:  "prince",
	})

	if err != nil {
		t.Fatal(err)
	}

	log.Println(res)
}
