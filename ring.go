package main

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// DefaultVirtualNodes is the number of ring positions used per physical node
// when the caller does not specify one.
const DefaultVirtualNodes = 64

// Node is a physical member of the hash ring.
type Node struct {
	Name        string `json:"name"`
	VirtualNodes int   `json:"virtualNodes"`
	JoinedAt    int64  `json:"joinedAt"` // monotonic join sequence (1-based)
}

func (n *Node) clone() *Node {
	if n == nil {
		return nil
	}
	c := *n
	return &c
}

// ---- structured errors carrying HTTP semantics ----

type statusErr struct {
	code int
	msg  string
}

func (e *statusErr) Error() string { return e.msg }

func badRequest(format string, a ...any) error {
	return &statusErr{code: http.StatusBadRequest, msg: fmt.Sprintf(format, a...)}
}
func conflict(format string, a ...any) error {
	return &statusErr{code: http.StatusConflict, msg: fmt.Sprintf(format, a...)}
}
func notFound(format string, a ...any) error {
	return &statusErr{code: http.StatusNotFound, msg: fmt.Sprintf(format, a...)}
}

// ---- hash function ----

// hashKey returns a 64-bit unsigned hash of the given string. SHA-1 is used so
// that hashes are well distributed across the 64-bit space, which keeps the
// ring balanced even for small virtual-node counts. SHA-1 is not used for any
// security purpose here.
func hashKey(s string) uint64 {
	sum := sha1.Sum([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}

// ---- ring entry ----

// ringEntry is one virtual position on the ring, owned by a physical node.
type ringEntry struct {
	hash   uint64
	node   string // physical node name
	vindex int    // virtual-node index within its owner (informational)
}

// ---- the in-memory service ----

// Service is an in-memory consistent-hash ring. It is safe for concurrent use.
type Service struct {
	mu     sync.Mutex
	nodes  map[string]*Node
	ring   []ringEntry // sorted ascending by hash
	seq    int64
}

// NewService returns an empty ring.
func NewService() *Service {
	return &Service{nodes: make(map[string]*Node)}
}

func trimName(name string) string { return strings.TrimSpace(name) }

// AddNode inserts a physical node (and its virtual positions) into the ring.
func (s *Service) AddNode(name string, virtualNodes int) (*Node, error) {
	name = trimName(name)
	if name == "" {
		return nil, badRequest("node name must not be empty")
	}
	if strings.Contains(name, "/") {
		return nil, badRequest("node name must not contain '/'")
	}
	if virtualNodes == 0 {
		virtualNodes = DefaultVirtualNodes
	}
	if virtualNodes < 1 {
		return nil, badRequest("virtualNodes must be a positive integer")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.nodes[name]; exists {
		return nil, conflict("node %q already exists", name)
	}

	s.seq++
	node := &Node{Name: name, VirtualNodes: virtualNodes, JoinedAt: s.seq}
	s.nodes[name] = node

	for i := 0; i < virtualNodes; i++ {
		s.ring = append(s.ring, ringEntry{
			hash:   hashKey(nodeKey(name, i)),
			node:   name,
			vindex: i,
		})
	}
	// Re-sort the whole ring by hash so lookup can binary-search. Appending and
	// re-sorting is simplest for an in-memory service; node counts stay small.
	sort.Slice(s.ring, func(i, j int) bool { return s.ring[i].hash < s.ring[j].hash })

	return node.clone(), nil
}

// RemoveNode takes a physical node out of the ring. Its virtual positions are
// removed, so subsequent key mappings skip it. Removing a node does not change
// the ownership of keys that were not owned by it (minimal migration).
func (s *Service) RemoveNode(name string) error {
	name = trimName(name)
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.nodes[name]; !ok {
		return notFound("node %q not found", name)
	}
	delete(s.nodes, name)

	// Filter the removed node's virtual positions out of the ring in place.
	// ringEntry holds a `node string` reference, so the trailing slots that fall
	// out of use after compaction must be zeroed: otherwise the underlying array
	// keeps the stale string alive and the GC cannot reclaim it. Under repeated
	// add/remove churn those retained references accumulate, leaking memory.
	n := 0
	for _, e := range s.ring {
		if e.node != name {
			s.ring[n] = e
			n++
		}
	}
	for i := n; i < len(s.ring); i++ {
		s.ring[i] = ringEntry{}
	}
	s.ring = s.ring[:n]
	return nil
}

func (s *Service) GetNode(name string) (*Node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[name]
	if !ok {
		return nil, notFound("node %q not found", name)
	}
	return n.clone(), nil
}

// ListNodes returns nodes in join order (stable).
func (s *Service) ListNodes() []*Node {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Node, 0, len(s.nodes))
	for _, n := range s.nodes {
		out = append(out, n.clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].JoinedAt < out[j].JoinedAt })
	return out
}

// Owner returns the name of the node that owns the given key: the first virtual
// node whose hash is >= hash(key), wrapping around clockwise. It returns an
// error when the ring has no online nodes (empty ring).
func (s *Service) Owner(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ownerLocked(key)
}

func (s *Service) ownerLocked(key string) (string, error) {
	if len(s.ring) == 0 {
		return "", badRequest("hash ring is empty: add a node before mapping keys")
	}
	h := hashKey(key)
	// First entry with hash >= h; if none, wrap to index 0.
	idx := sort.Search(len(s.ring), func(i int) bool { return s.ring[i].hash >= h })
	if idx == len(s.ring) {
		idx = 0
	}
	return s.ring[idx].node, nil
}

// Owners maps each key to its owner node. Keys whose owner cannot be resolved
// (empty ring) are reported in the returned error; keys resolved successfully
// before that point are still in the result map.
func (s *Service) Owners(keys []string) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		owner, err := s.ownerLocked(k)
		if err != nil {
			return out, err
		}
		out[k] = owner
	}
	return out, nil
}

// nodeKey is the string hashed to place a virtual node on the ring.
func nodeKey(name string, vindex int) string {
	return fmt.Sprintf("%s#%d", name, vindex)
}
