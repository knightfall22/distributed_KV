package store

import "sync"

type Store struct {
	sync.RWMutex
	store map[string]string
}

//Simple Thread Safe Datasore
func NewStore() *Store {
	return &Store{
		store: make(map[string]string),
	}
}

// Get fetches the value of key from the store and returns (v, true) if
// it was found or ("", false) otherwise.
func (s *Store) Get(key string) (string, bool) {
	s.RLock()
	defer s.RUnlock()

	val, ok := s.store[key]

	return val, ok
}

//Replaces the previous value if any. Returns ("", false) on new addition;
// returns (oldValue, true) when a value existed previously
func (s *Store) Put(key, value string) (string, bool) {
	s.Lock()
	defer s.Unlock()

	val, ok := s.store[key]
	s.store[key] = value

	return val, ok
}

//Atomic update of value. Iff compare == oldvalue and the key exists the value is update, else non-op
func (s *Store) CAS(key, compare, value string) (string, bool) {
	s.Lock()
	defer s.Unlock()

	oldValue, ok := s.store[key]
	if ok && oldValue == compare {
		s.store[key] = value
	}

	return oldValue, ok
}

//Appends to value and returns returns (v, true) if
// it was found or ("", false) otherwise.
func (s *Store) Append(key, value string) (string, bool) {
	s.Lock()
	defer s.Unlock()

	v, ok := s.store[key]
	s.store[key] += value
	return v, ok
}
