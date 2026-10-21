package semanticcache

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

//go:embed testdata/demo_embeddings.json
var demoEmbeddings []byte

func DemoRecordedEmbedder() (*RecordedEmbedder, error) {
	return LoadRecordedEmbedder(bytes.NewReader(demoEmbeddings), MiniLMIdentity())
}

// DemoRequests is a development-only smoke stream, not a tuning/final dataset.
func DemoRequests() []Request {
	scope := Scope{Namespace: "helpdesk", Context: RequestContext{TenantID: "demo-tenant", Locale: "en", ProductVersion: "v1", KnowledgeVersion: "kb1"}, ModelVersion: "fake-llm-v1"}
	texts := []string{"How do I reset my password?", "How do I reset my password?", "Where can I download my invoices?", "How do I change the application language?"}
	var requests []Request
	for i, text := range texts {
		requests = append(requests, Request{ID: fmt.Sprintf("demo-%03d", i+1), Text: text, Namespace: scope.Namespace, Context: scope.Context, ModelVersion: scope.ModelVersion, Timestamp: time.Date(2026, 10, 21, 0, 0, i, 0, time.UTC), CacheEligible: true})
	}
	return requests
}

func RunDemo(ctx context.Context, config Config, embedder Embedder) ([]Result, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	identity := MiniLMIdentity()
	if config.Embedding.Model != identity.Model || config.Embedding.Dimension != identity.Dimension || config.Embedding.Preprocessing != identity.Preprocessing {
		return nil, fmt.Errorf("demo requires the frozen MiniLM embedding configuration")
	}
	clock := &LogicalClock{Time: DemoRequests()[0].Timestamp}
	cache, err := NewReferenceCache(config.Policy, identity.Dimension, config.Policy.CapacityEntries[0], clock)
	if err != nil {
		return nil, err
	}
	baseline := Baseline{Embedder: embedder, Cache: cache, Generator: FakeLLM{}, Clock: clock, PolicyID: config.Policy.ID, Threshold: config.Policy.CosineThreshold, Identity: identity}
	var results []Result
	for _, r := range DemoRequests() {
		clock.Time = r.Timestamp
		results = append(results, baseline.Serve(ctx, r))
	}
	return results, nil
}

func hashBytes(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

// SaveDemo writes measured request logs, reusable real embeddings and a manifest.
// It does not apply the simulated latency/cost assumptions or claim evaluation.
func SaveDemo(dir, sourceRoot, mode, revision string, config Config, results []Result) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	var logs bytes.Buffer
	enc := json.NewEncoder(&logs)
	file := EmbeddingFile{Identity: MiniLMIdentity(), Origin: "demo capture: " + mode}
	seen := map[string]bool{}
	for _, result := range results {
		if err := enc.Encode(result); err != nil {
			return err
		}
		if record := result.Embedding; record != nil && !seen[record.Text] {
			file.Records = append(file.Records, *record)
			seen[record.Text] = true
		}
	}
	configBytes, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	vectors, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return err
	}
	requests, err := json.MarshalIndent(DemoRequests(), "", "  ")
	if err != nil {
		return err
	}
	artifacts := map[string][]byte{"requests.jsonl": logs.Bytes(), "embeddings.json": vectors, "config.json": configBytes, "workload.json": requests}
	hashes := map[string]string{}
	for name, data := range artifacts {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0644); err != nil {
			return err
		}
		hashes[name] = hashBytes(data)
	}
	// Record each source hash because the project currently has no Git commit.
	sources := map[string]string{}
	outputAbs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	err = filepath.WalkDir(sourceRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			absolute, err := filepath.Abs(path)
			if err != nil {
				return err
			}
			if absolute == outputAbs || d.Name() == ".git" || d.Name() == "results" || d.Name() == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		ext := filepath.Ext(path)
		if ext != ".go" && ext != ".md" && ext != ".json" && d.Name() != "go.mod" && d.Name() != "go.sum" {
			return nil
		}
		rel, err := filepath.Rel(sourceRoot, path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sources[filepath.ToSlash(rel)] = hashBytes(data)
		return nil
	})
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(sources))
	for key := range sources {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var sourceDigest strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&sourceDigest, "%s %s\n", key, sources[key])
	}
	if revision == "" {
		revision = "unknown; project untracked, source hashes recorded"
	}
	manifest := struct {
		RunID          string            `json:"run_id"`
		CreatedAt      time.Time         `json:"created_at"`
		GoVersion      string            `json:"go_version"`
		Platform       string            `json:"platform"`
		Revision       string            `json:"source_revision"`
		SourceSHA      string            `json:"source_sha256"`
		Sources        map[string]string `json:"source_files_sha256"`
		Mode           string            `json:"embedding_mode"`
		TimingBoundary string            `json:"timing_boundary"`
		Generation     string            `json:"generation"`
		Identity       EmbeddingIdentity `json:"embedding_identity"`
		Artifacts      map[string]string `json:"artifacts_sha256"`
		FixtureSHA     string            `json:"recorded_fixture_sha256"`
		Seeds          string            `json:"seeds"`
		Command        string            `json:"command"`
	}{"phase1-demo-v1", time.Now().UTC(), runtime.Version(), runtime.GOOS + "/" + runtime.GOARCH, revision, hashBytes([]byte(sourceDigest.String())), sources, mode, "measured wall time; recorded embeddings: routing-only including vector loading, excludes original model inference; live TEI: includes HTTP/inference and model verification; fake generation CPU only; no simulated delays/costs applied", "deterministic simulated fake LLM", MiniLMIdentity(), hashes, hashBytes(demoEmbeddings), "none used; fixed stream; configured experiment seeds saved but unused", strings.Join(os.Args, " ")}
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "manifest.json"), manifestBytes, 0644)
}
