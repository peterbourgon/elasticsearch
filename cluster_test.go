// +build cluster

// This file is only built and run if you specify
// -tags=cluster as part of the 'go test' invocation.
// http://golang.org/pkg/go/build/#Build_Constraints

package elasticsearch_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	es "github.com/peterbourgon/elasticsearch"
	"io/ioutil"
	"net/http"
	"testing"
	"time"
)

func init() {
	waitForCluster(15 * time.Second)
}

// Just tests es.Cluster internals; doesn't make any real connection.
func TestClusterShutdown(t *testing.T) {
	endpoints := []string{"http://host1:9200", "http://host2:9200"}
	pingInterval, pingTimeout := 30*time.Second, 3*time.Second
	c := es.NewCluster(endpoints, pingInterval, pingTimeout)

	e := make(chan error)
	go func() {
		c.Shutdown()
		e <- nil
	}()
	go func() {
		<-time.After(1 * time.Second)
		e <- fmt.Errorf("timeout")
	}()

	if err := <-e; err != nil {
		t.Fatalf("%s", err)
	}
}

func TestSimpleTermQuery(t *testing.T) {
	indices := []string{"twitter"}
	c := newCluster(t, indices, map[string]interface{}{
		"/twitter/tweet/1": map[string]string{
			"user":      "kimchy",
			"post_date": "2009-11-15T14:12:12",
			"message":   "trying out Elastic Search",
		},
	})
	defer c.Shutdown()
	defer deleteIndices(t, indices) // comment out to leave data after test

	q := es.QueryWrapper(es.TermQuery(es.TermQueryParams{
		Query: &es.Wrapper{
			Name:    "user",
			Wrapped: "kimchy",
		},
	}))

	request := es.SearchRequest{
		Indices: []string{"twitter"},
		Types:   []string{"tweet"},
		Query:   q,
	}

	response, err := c.Search(request)
	if err != nil {
		t.Error(err)
	}

	if response.Error != "" {
		t.Error(response.Error)
	}
	if expected, got := 1, response.HitsWrapper.Total; expected != got {
		t.Fatalf("expected %d, got %d", expected, got)
	}

	t.Logf("OK, %d hit(s), %dms", response.HitsWrapper.Total, response.Took)
}

func TestMultiSearch(t *testing.T) {
	indices := []string{"index1", "index2"}
	c := newCluster(t, indices, map[string]interface{}{
		"/index1/foo/1": map[string]string{
			"user":        "alice",
			"description": "index=index1 type=foo id=1 user=alice",
		},
		"/index2/bar/2": map[string]string{
			"user":        "bob",
			"description": "index=index2 type=bar id=2 user=bob",
		},
	})
	defer c.Shutdown()
	defer deleteIndices(t, indices) // comment out to leave data after test

	q1 := es.QueryWrapper(es.TermQuery(es.TermQueryParams{
		Query: &es.Wrapper{
			Name:    "user",
			Wrapped: "alice",
		},
	}))
	q2 := es.QueryWrapper(es.TermQuery(es.TermQueryParams{
		Query: &es.Wrapper{
			Name:    "user",
			Wrapped: "bob",
		},
	}))
	q3 := es.QueryWrapper(es.MatchAllQuery())

	requests := []es.SearchRequest{
		es.SearchRequest{
			Indices: []string{"index1"},
			Types:   []string{"foo"},
			Query:   q1,
		},
		es.SearchRequest{
			Indices: []string{"index2"},
			Types:   []string{"bar"},
			Query:   q2,
		},
		es.SearchRequest{
			Indices: []string{}, // "index1", "index2" is not supported (!)
			Types:   []string{}, // "type1", "type2" is not supported (!)
			Query:   q3,
		},
	}

	request := es.MultiSearchRequest(requests)
	response, err := c.MultiSearch(request)
	if err != nil {
		t.Fatal(err)
	}

	if expected, got := 3, len(response.Responses); expected != got {
		t.Fatalf("expected %d response(s), got %d", expected, got)
	}

	r1 := response.Responses[0]
	if r1.Error != "" {
		t.Fatalf("response 1: %s", r1.Error)
	}
	if expected, got := 1, r1.HitsWrapper.Total; expected != got {
		t.Fatalf("response 1: expected %d hit(s), got %d", expected, got)
	}
	buf, _ := json.Marshal(r1)
	t.Logf("response 1 OK: %s", buf)

	r2 := response.Responses[1]
	if r2.Error != "" {
		t.Fatalf("response 2: %s", r1.Error)
	}
	if expected, got := 1, r2.HitsWrapper.Total; expected != got {
		t.Fatalf("response 2: expected %d hit(s), got %d", expected, got)
	}
	buf, _ = json.Marshal(r2)
	t.Logf("response 2 OK: %s", buf)

	r3 := response.Responses[2]
	if r3.Error != "" {
		t.Fatalf("response 3: %s", r1.Error)
	}
	if expected, got := 2, r3.HitsWrapper.Total; expected != got {
		t.Fatalf("response 3: expected %d hit(s), got %d", expected, got)
	}
	buf, _ = json.Marshal(r3)
	t.Logf("response 3 OK: %s", buf)
}

//
//
//

func waitForCluster(timeout time.Duration) {
	giveUp := time.After(timeout)
	delay := 100 * time.Millisecond
	for {
		_, err := http.Get("http://127.0.0.1:9200")
		if err == nil {
			fmt.Printf("ElasticSearch now available\n")
			return // great
		}

		fmt.Printf("ElasticSearch not ready yet; waiting %s\n", delay)
		select {
		case <-time.After(delay):
			delay *= 2
		case <-giveUp:
			panic("ElasticSearch didn't come up in time")
		}
	}
}

func newCluster(t *testing.T, indices []string, m map[string]interface{}) *es.Cluster {
	deleteIndices(t, indices)
	loadData(t, m)

	endpoints := []string{"http://localhost:9200"}
	pingInterval, pingTimeout := 10*time.Second, 3*time.Second
	return es.NewCluster(endpoints, pingInterval, pingTimeout)
}

func deleteIndices(t *testing.T, indices []string) {
	for _, index := range indices {
		url := "http://127.0.0.1:9200/" + index + "?refresh=true"
		req, err := http.NewRequest("DELETE", url, nil)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}

		respBuf, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("DELETE %s: %s", index, respBuf)
	}
}

func loadData(t *testing.T, m map[string]interface{}) {
	for path, body := range m {
		reqBytes, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}

		// refresh=true to make document(s) immediately searchable
		url := "http://127.0.0.1:9200" + path + "?refresh=true"
		req, err := http.NewRequest("PUT", url, bytes.NewBuffer(reqBytes))
		if err != nil {
			t.Fatal(err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}

		respBuf, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("PUT %s: %s", path, respBuf)
	}
}
