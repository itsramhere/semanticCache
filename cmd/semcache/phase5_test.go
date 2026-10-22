package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"semanticcache/internal/semanticcache"
)

func TestPhase5ChurnCLI(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "churn")
	var out, errOut bytes.Buffer
	args := []string{"churn", "-config", "../../configs/churn.json", "-output", dir}
	if code := run(args, &out, &errOut); code != 0 {
		t.Fatalf("churn %d: %s", code, errOut.String())
	}
	b, err := os.ReadFile(filepath.Join(dir, "metrics.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m semanticcache.ChurnMetrics
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if !m.FinalAgreement || m.Requests != 60 || m.DroppedEvents == 0 || m.FallbackRequests == 0 {
		t.Fatalf("missing CLI behavior: %+v", m)
	}
	if code := run(args, &out, &errOut); code != 1 {
		t.Fatal("existing churn output overwritten")
	}
	for _, body := range []string{`{"unknown":1}`, `{} {}`, `{"id":"invalid"}`} {
		path := filepath.Join(t.TempDir(), "bad.json")
		if err := os.WriteFile(path, []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
		if code := run([]string{"churn", "-config", path, "-output", filepath.Join(t.TempDir(), "bad")}, &out, &errOut); code != 1 {
			t.Fatal("invalid config accepted", body)
		}
	}
	if code := run([]string{"churn", "extra"}, &out, &errOut); code != 2 {
		t.Fatal("positional argument accepted")
	}
}
