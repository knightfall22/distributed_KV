package store

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/knightfall22/distributed_KV/api"
)

type Config struct {
	RaftDir   string
	Raddr     string
	NodeId    string
	Bootstrap bool
}

// Node represents a node in the cluster.
type Node struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// StoreStatus is the Status a Store returns.
type StoreStatus struct {
	Me        Node   `json:"me"`
	Leader    Node   `json:"leader"`
	Followers []Node `json:"followers"`
}

type Store struct {
	sync.RWMutex

	config Config

	store map[string]string

	raft *raft.Raft

	logger *log.Logger
}

// Simple Thread Safe Datasore
func NewStore(config Config) (*Store, error) {
	s := &Store{
		config: config,
		store:  make(map[string]string),
		logger: log.New(os.Stderr, "[store] ", log.LstdFlags),
	}
	if err := s.setupRaft(s.config.RaftDir); err != nil {
		return nil, err
	}

	return s, nil
}

// A Raft instance comprises:
//   - A finite-state machine that applies the commands you give Raft;
//   - A log store where Raft stores those commands;
//   - A stable store where Raft stores the cluster’s configuration—the servers
//     in the cluster, their addresses, and so on;
//   - A snapshot store where Raft stores compact snapshots of its data; and
//   - A transport that Raft uses to connect with the server’s peers.
func (s *Store) setupRaft(dir string) error {
	fsm := &fsm{
		s,
	}

	logDir := filepath.Join(dir, "raft")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	logStore, err := raftboltdb.NewBoltStore(
		filepath.Join(logDir, "log"),
	)

	// The stable store is a key-value store where Raft stores important metadata,
	// like the server’s current term or the candidate the server voted for
	stableStore, err := raftboltdb.NewBoltStore(
		filepath.Join(logDir, "stable"),
	)
	if err != nil {
		return err
	}

	retain := 1

	snapShotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dir, "raft"),
		retain,
		os.Stderr,
	)
	if err != nil {
		return err
	}

	//set up raft communication
	addr, err := net.ResolveTCPAddr("tcp", s.config.Raddr)
	if err != nil {
		return err
	}

	transport, err := raft.NewTCPTransport(
		s.config.Raddr,
		addr,
		3, 10*time.Second,
		os.Stderr,
	)
	if err != nil {
		log.Println("TCP address ", err)
		return err
	}

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.config.NodeId)

	s.raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapShotStore,
		transport,
	)
	if err != nil {
		return err
	}

	hasState, err := raft.HasExistingState(
		logStore,
		stableStore,
		snapShotStore,
	)
	if err != nil {
		return err
	}

	if !hasState && s.config.Bootstrap {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: raft.ServerAddress(s.config.Raddr),
			}},
		}
		err = s.raft.BootstrapCluster(config).Error()
	}
	return err
}

func (s *Store) Join(id, addr string) error {
	s.logger.Printf("received join request for remote node %s at %s", id, addr)
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Printf("failed to get raft configuration: %v", err)
		return err
	}

	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			//Already exist in the cluster
			if srv.ID == serverID || srv.Address == serverAddr {
				s.logger.Printf("node %s at %s already member of cluster, ignoring join request", id, addr)
				return nil
			}

			//Remove server from cluster
			removeFuture := s.raft.RemoveServer(serverID, 0, 0)
			if removeFuture.Error() != nil {
				return removeFuture.Error()
			}
		}
	}

	addVoter := s.raft.AddNonvoter(serverID, serverAddr, 0, 0)
	if err := addVoter.Error(); err != nil {
		return err
	}
	s.logger.Printf("node %s at %s joined successfully", id, addr)
	return nil
}

func (s *Store) Status() (*StoreStatus, error) {
	leaderServerAddr, leaderId := s.raft.LeaderWithID()
	leader := Node{
		ID:      string(leaderId),
		Address: string(leaderServerAddr),
	}

	status := StoreStatus{}
	status.Leader = leader

	future := s.raft.GetConfiguration()

	if err := future.Error(); err != nil {
		return nil, err
	}

	for _, srv := range future.Configuration().Servers {
		if srv.ID != leaderId {
			status.Followers = append(status.Followers, Node{
				ID:      string(srv.ID),
				Address: string(srv.Address),
			})
		}

		if srv.ID == raft.ServerID(s.config.NodeId) {
			status.Me = Node{
				ID:      string(srv.ID),
				Address: string(srv.Address),
			}
		}
	}

	return &status, nil
}

func (s *Store) Close() error {
	f := s.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}

	return nil
}

// func (s *Store) Handler(cmd *api.Command) (*api.Response, error) {
// 	switch cmd.Kind {
// 	case api.CommandGet:
// 		return s.get(cmd.Key), nil

// 	case api.CommandPut:
// 		return s.put(cmd.Key, cmd.Value), nil

// 	case api.CommandCAS:
// 		return s.cas(cmd.Key, cmd.Compare, cmd.Value), nil

// 	case api.CommandAppend:
// 		return s.append(cmd.Key, cmd.Value), nil
// 	default:
// 		return nil, fmt.Errorf("command not found")
// 	}
// }

// Get fetches the value of key from the store and returns (v, true) if
// it was found or ("", false) otherwise.
func (s *Store) Get(cmd api.Command) *api.Response {
	s.RLock()
	defer s.RUnlock()

	val, ok := s.store[cmd.Key]

	return &api.Response{
		Value: val,
		Found: ok,
	}
}

// Since we are replicating only write operation. Write operation is
// a universal function that represents all write operation(PUT, APPEND and CAS).
func (s *Store) Write(cmd api.Command) (*api.Response, error) {
	if s.raft.State() != raft.Leader {
		return nil, fmt.Errorf("not leader")
	}
	b, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	future := s.raft.Apply(b, 10*time.Second)

	if future.Error() != nil {
		return nil, future.Error()
	}

	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}

	return res.(*api.Response), nil
}

func (s *Store) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out")
		case <-ticker.C:
			if s := s.raft.Leader(); s != "" {
				return nil
			}
		}
	}
}

// An instance of a finite state machine that appies raft commands
var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	*Store
}

func (f *fsm) Apply(record *raft.Log) any {
	var cmd api.Command

	if err := json.Unmarshal(record.Data, &cmd); err != nil {
		return err
	}

	switch cmd.Kind {
	case api.CommandPut:
		return f.put(cmd)
	case api.CommandCAS:
		return f.cas(cmd)
	case api.CommandAppend:
		return f.append(cmd)
	default:
		return fmt.Errorf("error command not found")
	}
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.Lock()
	defer f.Unlock()

	clone := make(map[string]string)

	maps.Copy(clone, f.store)

	return &snapshot{store: clone}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	newM := make(map[string]string)

	if err := json.NewDecoder(rc).Decode(&newM); err != nil {
		return err
	}

	f.store = newM
	return nil
}

// Replaces the previous value if any. Returns ("", false) on new addition;
// returns (oldValue, true) when a value existed previously
func (f *fsm) put(cmd api.Command) *api.Response {
	f.Lock()
	defer f.Unlock()

	val, ok := f.store[cmd.Key]
	f.store[cmd.Key] = cmd.Value

	return &api.Response{
		PreviousValue: val,
		Found:         ok,
	}
}

// Atomic update of value. Iff compare == oldvalue and the key exists the value is update, else non-op
func (f *fsm) cas(cmd api.Command) *api.Response {
	f.Lock()
	defer f.Unlock()

	oldValue, ok := f.store[cmd.Key]
	if ok && oldValue == cmd.Compare {
		f.store[cmd.Key] = cmd.Value
	}

	return &api.Response{
		PreviousValue: oldValue,
		Found:         ok,
	}
}

// Appends to value and returns returns (v, true) if
// it was found or ("", false) otherwise.
func (f *fsm) append(cmd api.Command) *api.Response {
	f.Lock()
	defer f.Unlock()

	v, ok := f.store[cmd.Key]
	f.store[cmd.Key] += cmd.Value

	return &api.Response{
		PreviousValue: v,
		Found:         ok,
	}
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	store map[string]string
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		b, err := json.Marshal(s.store)
		if err != nil {
			return err
		}

		if _, err := sink.Write(b); err != nil {
			return err
		}

		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (s *snapshot) Release() {}
