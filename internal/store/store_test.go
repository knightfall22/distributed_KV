package store_test

import (
	"testing"
	"time"

	"github.com/knightfall22/distributed_KV/internal/store"
)

func checkGet(t *testing.T, store *store.Store, key, value string, exists bool) {
	val, present := store.Get(key)

	if value != val || present != exists {
		t.Fatalf("failed to get value of %q; value: %q, present: %t", key, val, present)
	}
}

func checkPut(t *testing.T, store *store.Store, key, value, preVal string, exists bool) {
	val, present := store.Put(key, value)

	if preVal != val || present != exists {
		t.Fatalf("put value mismatch for key %q, expected (%q, %t) got (%q, %t)", key, val, present, preVal, exists)
	}
}

func checkCAS(t *testing.T, store *store.Store, key, value, compare string, exists bool) {
	val, present := store.CAS(key, compare, value)

	if compare != val || present != exists {
		t.Fatalf("value mismatch for key %q, expected (%q, %t) got (%q, %t)", key, val, present, compare, exists)
	}
}

func checkAppend(t *testing.T, store *store.Store, key, value, preVal string, exists bool) {
	val, present := store.Append(key, value)

	if preVal != val || present != exists {
		t.Fatalf("value mismatch for appending key %q, expected (%q, %t) got (%q, %t)", key, val, present, preVal, exists)
	}
}

func TestPut(t *testing.T) {
	store := store.NewStore()

	checkPut(t, store, "prince", "andrew", "", false)
	checkPut(t, store, "prince", "nicholas", "andrew", true)
	checkPut(t, store, "prince", "nikolai", "nicholas", true)
}

func TestGet(t *testing.T) {
	store := store.NewStore()

	checkPut(t, store, "prince", "andrew", "", false)
	checkGet(t, store, "prince", "andrew", true)
	checkGet(t, store, "princess", "", false)
}

func TestCAS(t *testing.T) {
	store := store.NewStore()

	checkPut(t, store, "prince", "andrew", "", false)
	checkCAS(t, store, "prince", "nikolai", "andrew", true)
	checkGet(t, store, "prince", "nikolai", true)

	checkCAS(t, store, "princess", "anna", "", false)
}

func TestAppend(t *testing.T) {
	store := store.NewStore()

	checkPut(t, store, "prince", "andrew", "", false)
	checkAppend(t, store, "prince", " bolonski", "andrew", true)
	checkGet(t, store, "prince", "andrew bolonski", true)
}

func TestConcurrent(t *testing.T) {
	store := store.NewStore()

	go func() {
		for range 200 {
			store.Put("prince", "andrew")
		}
	}()

	go func() {
		for range 200 {
			store.Put("prince", "nikolai")
		}
	}()

	time.Sleep(50 * time.Millisecond)

	val, _ := store.Get("prince")
	if val != "andrew" && val != "nikolai" {
		t.Fatalf("failed to get value of %q; value: %q", "prince", val)
	}
}
