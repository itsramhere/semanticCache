package semanticcache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
)

// LSHConfig fixes the embedding space and hyperplane generation algorithm.
type LSHConfig struct {
	Version  string            `json:"version"`
	Identity EmbeddingIdentity `json:"identity"`
	Seed     int64             `json:"seed"`
	Tables   int               `json:"tables"`
	Bits     int               `json:"bits"`
}

func (c LSHConfig) Validate() error {
	if err := c.Identity.Validate(); err != nil {
		return err
	}
	if c.Version != "cosine-hyperplanes-v1" || c.Tables < 1 || c.Tables > 256 || c.Bits < 1 || c.Bits > 64 || c.Identity.Dimension > 65536 || int64(c.Tables)*int64(c.Bits)*int64(c.Identity.Dimension) > 16*1024*1024 {
		return fmt.Errorf("invalid or excessive LSH configuration")
	}
	return nil
}

// Projections persists the actual coefficients; loading never regenerates them.
type Projections struct {
	Config LSHConfig     `json:"config"`
	Planes [][][]float64 `json:"planes"`
}

func NewProjections(c LSHConfig) (*Projections, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	rng := rand.New(rand.NewSource(c.Seed))
	p := &Projections{Config: c, Planes: make([][][]float64, c.Tables)}
	for t := range p.Planes {
		p.Planes[t] = make([][]float64, c.Bits)
		for b := range p.Planes[t] {
			v := make([]float64, c.Identity.Dimension)
			for d := range v {
				v[d] = rng.NormFloat64()
			}
			p.Planes[t][b], _ = Normalize(v)
		}
	}
	return p, nil
}

func (p *Projections) Validate() error {
	if p == nil {
		return fmt.Errorf("missing projections")
	}
	if err := p.Config.Validate(); err != nil {
		return err
	}
	if len(p.Planes) != p.Config.Tables {
		return fmt.Errorf("projection table count mismatch")
	}
	for _, table := range p.Planes {
		if len(table) != p.Config.Bits {
			return fmt.Errorf("projection bit count mismatch")
		}
		for _, v := range table {
			if _, err := normalizeDimension(v, p.Config.Identity.Dimension); err != nil {
				return err
			}
		}
	}
	return nil
}

func LoadProjections(r io.Reader, expected LSHConfig) (*Projections, error) {
	var p Projections
	dec := json.NewDecoder(io.LimitReader(r, 512*1024*1024))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return nil, err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("trailing projection JSON")
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	if p.Config != expected {
		return nil, fmt.Errorf("incompatible projection configuration")
	}
	return &p, nil
}

// ID commits to metadata AND coefficients, including manually supplied planes.
func (p *Projections) ID() string {
	b, _ := json.Marshal(p)
	sum := sha256.Sum256(b)
	return "lsh-" + hex.EncodeToString(sum[:])
}

type BucketKey struct {
	Scope        Scope  `json:"scope"`
	ProjectionID string `json:"projection_id"`
	Table        int    `json:"table"`
	Bucket       uint64 `json:"bucket"`
}

// Keys normalizes a private vector copy. Dot >= 0 sets bit b (including zero).
func (p *Projections) Keys(scope Scope, vector []float64) ([]BucketKey, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return p.keys(scope, vector, p.ID())
}

func (p *Projections) keys(scope Scope, vector []float64, id string) ([]BucketKey, error) {
	keys, _, err := p.hash(scope, vector, id)
	return keys, err
}

// hash also detects signs close enough to zero to be changed by float64
// roundoff, including repeated normalization at embedding/cache boundaries.
// Stored keys still follow dot >= 0; prediction searches conservatively when
// any sign is ambiguous instead of treating a rounding difference as absence.
func (p *Projections) hash(scope Scope, vector []float64, id string) ([]BucketKey, bool, error) {
	if err := validateScope(scope); err != nil {
		return nil, false, err
	}
	v, err := normalizeDimension(vector, p.Config.Identity.Dimension)
	if err != nil {
		return nil, false, err
	}
	ambiguous := false
	keys := make([]BucketKey, len(p.Planes))
	for t, table := range p.Planes {
		var bucket uint64
		for b, plane := range table {
			dot := 0.0
			absoluteProducts := 0.0
			for d, x := range v {
				product := x * plane[d]
				dot += product
				absoluteProducts += math.Abs(product)
			}
			// Eight epsilon per dimension allows for normalization, products
			// and accumulation. Scale by products so non-unit saved planes
			// use the same relative bound. This never changes a stored bit.
			if math.Abs(dot) <= 8*0x1p-52*float64(len(v))*absoluteProducts {
				ambiguous = true
			}
			if dot >= 0 {
				bucket |= uint64(1) << b
			}
		}
		keys[t] = BucketKey{scope, id, t, bucket}
	}
	return keys, ambiguous, nil
}
