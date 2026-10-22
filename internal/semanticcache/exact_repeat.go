package semanticcache

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"
)

type repeatKey struct {
	Scope  Scope
	Digest [32]byte
}

// ExactRepeat is an exact normalized-float64-vector routing reference. It cannot
// establish semantic membership; even a positive decision still needs search.
// Observe rebuilds from the serialized snapshot, so removals/replacements cannot
// leave stale membership. It owns no cache handle and stores no answers.
type ExactRepeat struct {
	identity EmbeddingIdentity
	ready    bool
	keys     map[repeatKey]struct{}
}

func (*ExactRepeat) ID() string { return "exact-vector-repeat-v1" }
func (p *ExactRepeat) Reset(identity EmbeddingIdentity, snapshot CacheSnapshot) error {
	p.ready = false
	if err := identity.Validate(); err != nil {
		return err
	}
	p.identity = identity
	if err := p.Observe(snapshot); err != nil {
		return err
	}
	p.ready = true
	return nil
}
func (p *ExactRepeat) key(scope Scope, vector []float64) (repeatKey, error) {
	if err := validateScope(scope); err != nil {
		return repeatKey{}, err
	}
	v, err := normalizeDimension(vector, p.identity.Dimension)
	if err != nil {
		return repeatKey{}, err
	}
	b := make([]byte, 8*len(v))
	for i, x := range v {
		if x == 0 {
			x = 0
		} // canonicalize negative zero
		binary.LittleEndian.PutUint64(b[i*8:], math.Float64bits(x))
	}
	return repeatKey{scope, sha256.Sum256(b)}, nil
}
func (p *ExactRepeat) Observe(snapshot CacheSnapshot) (err error) {
	defer func() {
		if err != nil {
			p.ready = false
		}
	}()
	if err := p.identity.Validate(); err != nil {
		return err
	}
	keys := make(map[repeatKey]struct{}, len(snapshot.Entries))
	seen := make(map[string]bool, len(snapshot.Entries))
	for _, x := range snapshot.Entries {
		e := x.Entry
		if e.ID == "" || seen[e.ID] {
			return fmt.Errorf("invalid or duplicate summary entry")
		}
		seen[e.ID] = true
		if !e.Validity.Reusable || e.Validity.Revoked {
			continue
		}
		key, err := p.key(e.Scope, e.Embedding)
		if err != nil {
			return err
		}
		keys[key] = struct{}{}
	}
	p.keys = keys
	return nil
}
func (p *ExactRepeat) Predict(r Request, vector []float64) Decision {
	d := Decision{true, "repeat-unready-fallback", p.ID()}
	if !p.ready {
		return d
	}
	k, err := p.key(r.Scope(), vector)
	if err != nil {
		d.Reason = "repeat-hash-error-fallback"
		return d
	}
	_, d.Search = p.keys[k]
	d.Reason = "exact-vector-absent"
	if d.Search {
		d.Reason = "exact-vector-present"
	}
	return d
}
func (p *ExactRepeat) MemoryBytes() int64 {
	n := int64(unsafe.Sizeof(*p)) + 48 + int64(len(p.identity.Model)+len(p.identity.Revision)+len(p.identity.Preprocessing))
	n += int64(len(p.keys)) * (2*int64(unsafe.Sizeof(repeatKey{})) + 64)
	for k := range p.keys {
		n += scopeStringBytes(k.Scope)
	}
	return n
}
