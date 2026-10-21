package semanticcache

import "testing"

func TestPolicyExamplesMatchDocumentedOutcomes(t *testing.T) {
	t.Parallel()
	results, err := RunPolicyExamples()
	if err != nil {
		t.Fatalf("run policy examples: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("RunPolicyExamples returned no results")
	}
	for _, result := range results {
		if !result.OK {
			t.Errorf("example %q: winner %q, want %q", result.Name, result.Winner, result.ExpectedWinner)
		}
	}
}

func TestPolicyExampleNamesAreUnique(t *testing.T) {
	t.Parallel()
	seen := map[string]bool{}
	for _, example := range AllPolicyExamples() {
		if seen[example.Name] {
			t.Errorf("duplicate example name %q", example.Name)
		}
		seen[example.Name] = true
	}
}

func TestExactThresholdIsInclusive(t *testing.T) {
	e := PolicyExample{
		Name: "exact boundary", Request: exampleRequest(exampleScopeA),
		QueryEmbedding: []float64{1, 0}, EmbeddingModelID: exampleEmbeddingModel,
		Entries: []Entry{exampleEntry("boundary", []float64{0.9, 0.4358898943540673}, exampleScopeA)},
	}
	winner, err := EvaluatePolicyExample(e)
	if err != nil || winner != "boundary" {
		t.Fatalf("exact threshold: winner=%q err=%v", winner, err)
	}
}
