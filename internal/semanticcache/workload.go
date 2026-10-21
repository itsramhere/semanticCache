package semanticcache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"strings"
	"time"
)

// FixtureIdentity is deliberately distinct from the real MiniLM adapter.
// Controlled geometry tests evaluation logic; it is not model-quality evidence.
func FixtureIdentity() EmbeddingIdentity {
	return EmbeddingIdentity{"simulated-faq-geometry", "v1", 64, PreprocessingV1}
}

type QAGroup struct {
	ID          string `json:"id"`
	Topic       string `json:"topic"`
	Split       string `json:"split"`
	Action      string `json:"action"`
	Instruction string `json:"instruction"`
}

type Catalog struct {
	ID     string    `json:"id"`
	Origin string    `json:"origin"`
	Groups []QAGroup `json:"groups"`
}

type LabeledRequest struct {
	Request  Request `json:"request"`
	GroupID  string  `json:"group_id"`
	Category string  `json:"category"`
	// AnswerClass is an author label independent of cosine matches.
	AnswerClass string    `json:"answer_class"`
	Reusable    bool      `json:"answer_reusable"`
	Vector      []float64 `json:"vector"`
}

type InitialEntry struct {
	Entry       Entry  `json:"entry"`
	GroupID     string `json:"group_id"`
	AnswerClass string `json:"answer_class"`
}

type Workload struct {
	ID           string            `json:"id"`
	Split        string            `json:"split"`
	Seed         int64             `json:"seed"`
	Origin       string            `json:"origin"`
	VectorSource string            `json:"vector_source"`
	Identity     EmbeddingIdentity `json:"identity"`
	Groups       []QAGroup         `json:"groups"`
	Warm         []InitialEntry    `json:"warm_initial_entries"`
	Cold         []InitialEntry    `json:"cold_initial_entries"`
	Requests     []LabeledRequest  `json:"requests"`
}

func decodeStrict(r io.Reader, dst any) error {
	d := json.NewDecoder(r)
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return fmt.Errorf("expected exactly one JSON value")
	}
	return nil
}

func LoadCatalog(r io.Reader) (Catalog, error) {
	var c Catalog
	if err := decodeStrict(r, &c); err != nil {
		return c, err
	}
	if strings.TrimSpace(c.ID) == "" || strings.TrimSpace(c.Origin) == "" || len(c.Groups) == 0 || len(c.Groups) > 60 {
		return c, fmt.Errorf("incomplete catalog")
	}
	ids, topics := map[string]bool{}, map[string]string{}
	for _, g := range c.Groups {
		if strings.TrimSpace(g.ID) == "" || ids[g.ID] || strings.TrimSpace(g.Topic) == "" || strings.TrimSpace(g.Action) == "" || strings.TrimSpace(g.Instruction) == "" || !validSplit(g.Split) {
			return c, fmt.Errorf("invalid catalog group %q", g.ID)
		}
		if s, ok := topics[g.Topic]; ok && s != g.Split {
			return c, fmt.Errorf("topic split leakage: %s", g.Topic)
		}
		ids[g.ID], topics[g.Topic] = true, g.Split
	}
	return c, nil
}

func validSplit(s string) bool { return s == "development" || s == "tuning" || s == "final" }

// BuildWorkloads records the entire order and both initial states before replay.
// The PRNG is used only for ordering, never for projection generation.
func BuildWorkloads(c Catalog, seed int64) ([]Workload, error) {
	// Reuse the external-input validation for programmatic callers as well.
	b, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	if _, err = LoadCatalog(strings.NewReader(string(b))); err != nil {
		return nil, err
	}
	var out []Workload
	base := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	for _, split := range []string{"development", "tuning", "final"} {
		w := Workload{ID: c.ID, Split: split, Seed: seed, Origin: c.Origin, VectorSource: "simulated-controlled-geometry-v1; not MiniLM inference", Identity: FixtureIdentity(), Cold: []InitialEntry{}}
		var indices []int
		for i, g := range c.Groups {
			if g.Split == split {
				w.Groups = append(w.Groups, g)
				indices = append(indices, i)
			}
		}
		if len(indices) == 0 {
			return nil, fmt.Errorf("empty split %s", split)
		}
		for _, i := range indices {
			g := c.Groups[i]
			v := make([]float64, w.Identity.Dimension)
			v[i] = 1
			r := fixtureRequest("warm-"+g.ID, "How do I "+g.Action+" in AcmeDesk?", base)
			w.Warm = append(w.Warm, InitialEntry{Entry: Entry{ID: r.ID, Embedding: v, Answer: "In AcmeDesk, " + g.Instruction, Scope: r.Scope(), InsertedAt: base, ExpiresAt: base.Add(time.Hour), Validity: ValidityMetadata{Reusable: true, PolicyVersion: PolicyV1ID}}, GroupID: g.ID, AnswerClass: g.ID})
		}
		n := 1000
		if split == "development" {
			n = 100
		}
		categories := []struct {
			name  string
			count int
		}{{"exact_repeat", n * 2 / 10}, {"paraphrase", n * 3 / 10}, {"unrelated", n * 2 / 10}, {"near_match", n / 10}, {"scope_conflict", n / 10}, {"noncacheable", n / 10}}
		for _, cat := range categories {
			for j := 0; j < cat.count; j++ {
				i := indices[j%len(indices)]
				g := c.Groups[i]
				v := make([]float64, w.Identity.Dimension)
				v[i] = 1
				r := fixtureRequest(fmt.Sprintf("%s-%d-%s-%04d", split, seed, cat.name, j), "How do I "+g.Action+" in AcmeDesk?", base)
				label := g.ID
				switch cat.name {
				case "paraphrase":
					r.Text = "Where can I find the steps to " + g.Action + " in AcmeDesk?"
					v[i] = 0.98
					v[63] = math.Sqrt(1 - 0.98*0.98)
				case "unrelated":
					r.Text = "Does AcmeDesk offer a training course about how to " + g.Action + "?"
					label += "/training"
					v[i] = -1
				case "near_match":
					r.Text = "Can an administrator " + g.Action + " for another user in AcmeDesk?"
					label += "/admin"
					v[i] = 0.96
					v[62] = 0.28
				case "scope_conflict":
					switch j % 6 {
					case 0:
						r.Context.TenantID = "tenant-b"
					case 1:
						r.Context.Locale = "fr"
					case 2:
						r.Context.ProductVersion = "v2"
					case 3:
						r.Context.KnowledgeVersion = "kb2"
					case 4:
						r.Namespace = "admin-faq"
					case 5:
						r.ModelVersion = "fake-v2"
					}
				case "noncacheable":
					r.Text = "What is my current personal status as I " + g.Action + " in AcmeDesk?"
					r.CacheEligible = false
					label += "/personal"
				}
				w.Requests = append(w.Requests, LabeledRequest{r, g.ID, cat.name, label, r.CacheEligible, v})
			}
		}
		rng := rand.New(rand.NewSource(seed))
		rng.Shuffle(len(w.Requests), func(i, j int) { w.Requests[i], w.Requests[j] = w.Requests[j], w.Requests[i] })
		for i := range w.Requests {
			w.Requests[i].Request.Timestamp = base.Add(time.Minute + time.Duration(i)*time.Second)
		}
		out = append(out, w)
	}
	return out, ValidateWorkloads(out)
}

func fixtureRequest(id, text string, t time.Time) Request {
	return Request{ID: id, Text: text, Namespace: "faq", Context: RequestContext{"tenant-a", "en", "v1", "kb1"}, ModelVersion: "fake-v1", Timestamp: t, CacheEligible: true}
}

func LoadWorkload(r io.Reader) (Workload, error) {
	var w Workload
	var raw json.RawMessage
	if err := decodeStrict(r, &raw); err != nil {
		return w, err
	}
	if err := decodeStrict(bytes.NewReader(raw), &w); err != nil {
		return w, err
	}
	// Bool/seed zero values are valid, so check their presence separately.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return w, err
	}
	if len(fields["seed"]) == 0 || string(fields["seed"]) == "null" {
		return w, fmt.Errorf("missing workload seed")
	}
	var requests []map[string]json.RawMessage
	if err := json.Unmarshal(fields["requests"], &requests); err != nil {
		return w, err
	}
	for _, r := range requests {
		if len(r["answer_reusable"]) == 0 || string(r["answer_reusable"]) == "null" {
			return w, fmt.Errorf("missing answer reusability label")
		}
		var request map[string]json.RawMessage
		if err := json.Unmarshal(r["request"], &request); err != nil {
			return w, err
		}
		if len(request["cache_eligible"]) == 0 || string(request["cache_eligible"]) == "null" {
			return w, fmt.Errorf("missing cache eligibility")
		}
	}
	return w, ValidateWorkloads([]Workload{w})
}

// ValidateWorkloads checks metadata only, without deriving vector hits or
// exposing held-out outcomes. Call with all split files to detect leakage.
func ValidateWorkloads(ws []Workload) error {
	if len(ws) == 0 {
		return fmt.Errorf("no workloads")
	}
	ids := map[string]bool{}
	groupSplits := map[string]string{}
	topicSplits := map[string]string{}
	textSplits := map[string]string{}
	for _, w := range ws {
		if strings.TrimSpace(w.ID) == "" || !validSplit(w.Split) || strings.TrimSpace(w.Origin) == "" || strings.TrimSpace(w.VectorSource) == "" || len(w.Requests) == 0 || len(w.Groups) == 0 || w.Cold == nil || len(w.Cold) != 0 {
			return fmt.Errorf("missing workload metadata or invalid cold state")
		}
		if err := w.Identity.Validate(); err != nil {
			return err
		}
		groups := map[string]bool{}
		for _, g := range w.Groups {
			if strings.TrimSpace(g.ID) == "" || groups[g.ID] || g.Split != w.Split || strings.TrimSpace(g.Topic) == "" || strings.TrimSpace(g.Action) == "" || strings.TrimSpace(g.Instruction) == "" {
				return fmt.Errorf("invalid group metadata")
			}
			if s, ok := groupSplits[g.ID]; ok && s != w.Split {
				return fmt.Errorf("group split leakage: %s", g.ID)
			}
			if s, ok := topicSplits[g.Topic]; ok && s != w.Split {
				return fmt.Errorf("topic split leakage: %s", g.Topic)
			}
			groups[g.ID] = true
			groupSplits[g.ID] = w.Split
			topicSplits[g.Topic] = w.Split
		}
		checkVector := func(v []float64) error {
			if _, err := normalizeDimension(v, w.Identity.Dimension); err != nil {
				return err
			}
			norm := 0.0
			for _, x := range v {
				norm += x * x
			}
			if math.Abs(norm-1) > 1e-9 {
				return fmt.Errorf("stored workload vectors must be normalized")
			}
			return nil
		}
		if len(w.Warm) != len(w.Groups) {
			return fmt.Errorf("warm state requires one canonical entry per group")
		}
		warmIDs := map[string]bool{}
		warmGroups := map[string]bool{}
		for _, e := range w.Warm {
			if strings.TrimSpace(e.Entry.ID) == "" || warmIDs[e.Entry.ID] || warmGroups[e.GroupID] || !groups[e.GroupID] || strings.TrimSpace(e.AnswerClass) == "" || e.Entry.InsertedAt.IsZero() || e.Entry.InsertedAt.Location() != time.UTC || e.Entry.ExpiresAt.Location() != time.UTC || !e.Entry.IsLive(w.Requests[0].Request.Timestamp) || !e.Entry.Validity.Reusable || e.Entry.Validity.Revoked || e.Entry.Validity.PolicyVersion != PolicyV1ID || strings.TrimSpace(e.Entry.Answer) == "" {
				return fmt.Errorf("invalid initial entry")
			}
			if err := validateScope(e.Entry.Scope); err != nil {
				return err
			}
			if err := checkVector(e.Entry.Embedding); err != nil {
				return err
			}
			warmIDs[e.Entry.ID] = true
			warmGroups[e.GroupID] = true
		}
		var last time.Time
		vectorsByText := map[string][]float64{}
		for _, r := range w.Requests {
			if err := ValidateRequest(r.Request); err != nil {
				return err
			}
			if ids[r.Request.ID] || warmIDs[r.Request.ID] {
				return fmt.Errorf("duplicate request ID: %s", r.Request.ID)
			}
			ids[r.Request.ID] = true
			if !groups[r.GroupID] || strings.TrimSpace(r.AnswerClass) == "" {
				return fmt.Errorf("missing request labels")
			}
			switch r.Category {
			case "exact_repeat", "paraphrase", "unrelated", "near_match", "scope_conflict", "noncacheable":
			default:
				return fmt.Errorf("invalid category")
			}
			if r.Request.Timestamp.Before(last) {
				return fmt.Errorf("request order moves backward")
			}
			last = r.Request.Timestamp
			if r.Category == "noncacheable" && (r.Request.CacheEligible || r.Reusable) {
				return fmt.Errorf("noncacheable request permits reuse")
			}
			if err := checkVector(r.Vector); err != nil {
				return err
			}
			text := strings.TrimSpace(r.Request.Text)
			if s, ok := textSplits[text]; ok && s != w.Split {
				return fmt.Errorf("request text split leakage")
			}
			textSplits[text] = w.Split
			if prev, ok := vectorsByText[text]; ok {
				for i, x := range prev {
					if x != r.Vector[i] {
						return fmt.Errorf("inconsistent vector for repeated text")
					}
				}
			}
			vectorsByText[text] = r.Vector
		}
	}
	return nil
}
