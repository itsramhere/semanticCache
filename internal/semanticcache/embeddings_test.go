package semanticcache

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTEIAdapter(t *testing.T) {
	identity := EmbeddingIdentity{"test", "pinned-sha", 2, PreprocessingV1}
	for _, tc := range []struct {
		name, body, sha string
		status          int
		wantErr         bool
	}{
		{"real protocol shape", "[[3,4]]", "pinned-sha", 200, false},
		{"wrong model", "[[3,4]]", "other", 200, true},
		{"bad dimension", "[[1,0,0]]", "pinned-sha", 200, true},
		{"zero vector", "[[0,0]]", "pinned-sha", 200, true},
		{"multiple", "[[1,0],[0,1]]", "pinned-sha", 200, true},
		{"malformed", "not json", "pinned-sha", 200, true},
		{"trailing", "[[1,0]] {}", "pinned-sha", 200, true},
		{"outage", "unavailable", "pinned-sha", 503, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.URL.Path == "/info" {
					if r.Method != "GET" {
						t.Error("info method")
					}
					json.NewEncoder(w).Encode(map[string]string{"model_id": "test", "model_sha": tc.sha})
					return
				}
				if r.URL.Path != "/embed" || r.Method != "POST" || r.Header.Get("Content-Type") != "application/json" {
					t.Error("embed protocol")
				}
				var input struct {
					Inputs   string `json:"inputs"`
					Truncate bool   `json:"truncate"`
				}
				if err := json.NewDecoder(r.Body).Decode(&input); err != nil || input.Inputs != "Keep  Case" || !input.Truncate {
					t.Errorf("preprocessing: %+v %v", input, err)
				}
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			}))
			defer server.Close()
			e, err := NewTEIEmbedder(server.URL, identity)
			if err != nil {
				t.Fatal(err)
			}
			v, err := e.Embed(context.Background(), " \tKeep  Case\u2003")
			if (err != nil) != tc.wantErr {
				t.Fatalf("vector %v error %v", v, err)
			}
			if !tc.wantErr && (v[0] != 0.6000000000000001 || v[1] != 0.8) {
				t.Fatalf("not normalized: %v", v)
			}
			if tc.sha != "pinned-sha" && calls != 1 {
				t.Fatal("embedded incompatible model")
			}
		})
	}
}

func TestTEIResponseSizeLimit(t *testing.T) {
	const limit = 4 << 20
	identity := EmbeddingIdentity{"test", "pinned-sha", 2, PreprocessingV1}
	info := `{"model_id":"test","model_sha":"pinned-sha"}`
	for _, path := range []string{"/info", "/embed"} {
		for _, tc := range []struct {
			name, suffix string
			padding      int
			wantErr      bool
		}{
			{name: "exact limit"},
			{name: "oversized whitespace", padding: 1, wantErr: true},
			{name: "hidden trailing JSON", suffix: "{}", wantErr: true},
		} {
			t.Run(path+"/"+tc.name, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body := "[[3,4]]"
					if r.URL.Path == "/info" {
						body = info
					}
					if r.URL.Path == path {
						body += strings.Repeat(" ", limit-len(body)+tc.padding) + tc.suffix
					}
					io.WriteString(w, body)
				}))
				defer server.Close()
				e, err := NewTEIEmbedder(server.URL, identity)
				if err != nil {
					t.Fatal(err)
				}
				v, err := e.Embed(context.Background(), "text")
				if (err != nil) != tc.wantErr {
					t.Fatalf("vector %v error %v; wantErr=%t", v, err, tc.wantErr)
				}
			})
		}
	}
}

func TestTEICancellationAndTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	e, err := NewTEIEmbedder(server.URL, MiniLMIdentity())
	if err != nil {
		t.Fatal(err)
	}
	e.client.Timeout = 20 * time.Millisecond
	if _, err := e.Embed(context.Background(), "text"); err == nil {
		t.Fatal("expected timeout")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.Embed(ctx, "text"); err == nil {
		t.Fatal("expected cancellation")
	}
	for _, endpoint := range []string{"file:///test", "http://", "http://host/?secret=test"} {
		if _, err := NewTEIEmbedder(endpoint, MiniLMIdentity()); err == nil {
			t.Fatal("accepted invalid URL")
		}
	}
}

func TestRecordedEmbeddings(t *testing.T) {
	e, err := DemoRecordedEmbedder()
	if err != nil {
		t.Fatal(err)
	}
	text := DemoRequests()[0].Text
	v, err := e.Embed(context.Background(), "\u2003"+text+" ")
	if err != nil || len(v) != 384 {
		t.Fatalf("recorded vector: %v", err)
	}
	v[0] = 99
	again, _ := e.Embed(context.Background(), text)
	if again[0] == 99 {
		t.Fatal("recorded vector alias")
	}
	if _, err := e.Embed(context.Background(), "unrecorded text"); err == nil {
		t.Fatal("invented embedding")
	}
	for _, data := range []string{string(demoEmbeddings) + " {}", strings.Replace(string(demoEmbeddings), MiniLMRevision, "bad", 1), strings.Replace(string(demoEmbeddings), `"origin":`, `"unknown":`, 1)} {
		if _, err := LoadRecordedEmbedder(strings.NewReader(data), MiniLMIdentity()); err == nil {
			t.Fatal("accepted corrupt metadata")
		}
	}
}
