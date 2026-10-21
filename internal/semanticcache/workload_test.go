package semanticcache

import (
	"encoding/json"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
)

func testWorkloads(t *testing.T) []Workload {
	t.Helper()
	f, err := os.Open("../../data/catalog.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	c, err := LoadCatalog(f)
	if err != nil {
		t.Fatal(err)
	}
	ws, err := BuildWorkloads(c, 101)
	if err != nil {
		t.Fatal(err)
	}
	return ws
}

func TestWorkloadConstruction(t *testing.T) {
	ws := testWorkloads(t)
	if !reflect.DeepEqual(ws, testWorkloads(t)) {
		t.Fatal("fixed seed changes construction")
	}
	for _, w := range ws {
		want := 1000
		if w.Split == "development" {
			want = 100
		}
		if len(w.Requests) != want {
			t.Fatal("wrong request count")
		}
		counts := map[string]int{}
		for _, r := range w.Requests {
			counts[r.Category]++
		}
		for cat, pct := range map[string]int{"exact_repeat": 2, "paraphrase": 3, "unrelated": 2, "near_match": 1, "scope_conflict": 1, "noncacheable": 1} {
			if counts[cat] != want*pct/10 {
				t.Fatalf("%s count %d", cat, counts[cat])
			}
		}
		if len(w.Warm) != len(w.Groups) || len(w.Cold) != 0 {
			t.Fatal("initial states")
		}
	}
}

func TestWorkloadValidation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func([]Workload)
	}{
		{"duplicate ID", func(ws []Workload) { ws[0].Requests[1].Request.ID = ws[0].Requests[0].Request.ID }},
		{"topic leakage", func(ws []Workload) { ws[1].Groups[0].Topic = ws[0].Groups[0].Topic }},
		{"group leakage", func(ws []Workload) { ws[1].Groups[0].ID = ws[0].Groups[0].ID }},
		{"missing origin", func(ws []Workload) { ws[0].Origin = "" }},
		{"missing class", func(ws []Workload) { ws[0].Requests[0].AnswerClass = "" }},
		{"blank class", func(ws []Workload) { ws[0].Requests[0].AnswerClass = " \t " }},
		{"blank initial class", func(ws []Workload) { ws[0].Warm[0].AnswerClass = " \t " }},
		{"blank initial ID", func(ws []Workload) { ws[0].Warm[0].Entry.ID = " \t " }},
		{"blank topic", func(ws []Workload) { ws[0].Groups[0].Topic = " \t " }},
		{"blank action", func(ws []Workload) { ws[0].Groups[0].Action = " \t " }},
		{"blank instruction", func(ws []Workload) { ws[0].Groups[0].Instruction = " \t " }},
		{"missing scope", func(ws []Workload) { ws[0].Requests[0].Request.Context.TenantID = "" }},
		{"missing vector", func(ws []Workload) { ws[0].Requests[0].Vector = nil }},
		{"zero vector", func(ws []Workload) { ws[0].Requests[0].Vector = make([]float64, 64) }},
		{"NaN", func(ws []Workload) { ws[0].Requests[0].Vector[0] = math.NaN() }},
		{"unnormalized", func(ws []Workload) { ws[0].Requests[0].Vector[0] = 10 }},
		{"initial vector", func(ws []Workload) { ws[0].Warm[0].Entry.Embedding = nil }},
		{"backward order", func(ws []Workload) { ws[0].Requests[1].Request.Timestamp = ws[0].Requests[0].Request.Timestamp.Add(-1) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ws := testWorkloads(t)
			tc.mutate(ws)
			if ValidateWorkloads(ws) == nil {
				t.Fatal("accepted invalid workload")
			}
		})
	}
}

func TestWorkloadStrictJSON(t *testing.T) {
	w := testWorkloads(t)[0]
	b, _ := json.Marshal(w)
	for _, suffix := range []string{"{}", " trailing"} {
		if _, err := LoadWorkload(strings.NewReader(string(b) + suffix)); err == nil {
			t.Fatal("accepted trailing JSON")
		}
	}
	for _, field := range []string{"seed", "answer_reusable", "cache_eligible"} {
		var fields map[string]any
		if err := json.Unmarshal(b, &fields); err != nil {
			t.Fatal(err)
		}
		switch field {
		case "seed":
			delete(fields, field)
		case "answer_reusable":
			delete(fields["requests"].([]any)[0].(map[string]any), field)
		case "cache_eligible":
			delete(fields["requests"].([]any)[0].(map[string]any)["request"].(map[string]any), field)
		}
		missing, _ := json.Marshal(fields)
		if _, err := LoadWorkload(strings.NewReader(string(missing))); err == nil {
			t.Fatalf("accepted missing %s", field)
		}
	}
}
