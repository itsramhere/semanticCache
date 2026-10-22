package semanticcache

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRemoteRejectsUnconfirmedMutations(t *testing.T) {
	for _, operation := range []string{"upsert", "eviction"} {
		for _, result := range []string{`null`, `false`, `{}`, `{"status":"acknowledged"}`, `{"status":"unexpected"}`} {
			t.Run(operation+"/"+result, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					response := `true`
					if strings.HasSuffix(r.URL.Path, "/points") || strings.HasSuffix(r.URL.Path, "/points/delete") {
						response = `{"status":"completed","operation_id":0}`
						if operation == "upsert" || strings.HasSuffix(r.URL.Path, "/points/delete") {
							response = result
						}
					}
					fmt.Fprintf(w, `{"status":"ok","result":%s}`, response)
				}))
				defer server.Close()
				local, clock := testCache(t, 1)
				p := smallProjections(t)
				summary, _ := NewLSHSummary(p)
				cache, err := NewRemoteCache(context.Background(), server.URL, "protocol", time.Second, local.policy, p.Config.Identity, 1, clock, summary)
				if err != nil {
					t.Fatal(err)
				}
				if operation == "eviction" {
					if err := cache.Put(context.Background(), exampleEntry("old", []float64{1, 0}, exampleScopeA)); err != nil {
						t.Fatal(err)
					}
				}
				before := cache.local.Snapshot()
				if err := cache.Put(context.Background(), exampleEntry("new", []float64{0, 1}, exampleScopeA)); err == nil {
					t.Fatal("unconfirmed remote mutation committed")
				}
				if !reflect.DeepEqual(before, cache.local.Snapshot()) || summary.Ready() || cache.broken == nil {
					t.Fatal("uncertain mutation changed ledger or left skipping enabled")
				}
				if cache.Calls[len(cache.Calls)-1].Error == "" {
					t.Fatal("malformed acknowledgement missing from HTTP error log")
				}
			})
		}
	}
}

func TestRemoteRejectsMalformedQuerySuccess(t *testing.T) {
	for _, result := range []string{`null`, `{}`, `{"points":null}`} {
		t.Run(result, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/query") {
					fmt.Fprintf(w, `{"status":"ok","result":%s}`, result)
					return
				}
				fmt.Fprint(w, `{"status":"ok","result":true}`)
			}))
			defer server.Close()
			local, clock := testCache(t, 1)
			cache, err := NewRemoteCache(context.Background(), server.URL, "protocol", time.Second, local.policy, smallProjections(t).Config.Identity, 1, clock, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := cache.Search(context.Background(), exampleRequest(exampleScopeA), []float64{1, 0}); err == nil {
				t.Fatal("malformed response became ordinary cache miss")
			}
			if cache.Calls[len(cache.Calls)-1].Error == "" {
				t.Fatal("malformed query missing from HTTP error log")
			}
		})
	}
}

func TestRemoteRejectsUnconfirmedCollectionCreation(t *testing.T) {
	for _, result := range []string{`null`, `false`, `{}`} {
		t.Run(result, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, `{"status":"ok","result":%s}`, result)
			}))
			defer server.Close()
			local, clock := testCache(t, 1)
			if _, err := NewRemoteCache(context.Background(), server.URL, "protocol", time.Second, local.policy, smallProjections(t).Config.Identity, 1, clock, nil); err == nil {
				t.Fatal("accepted unconfirmed collection ownership")
			}
		})
	}
}
