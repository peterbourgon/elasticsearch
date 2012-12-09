// +xxxbuild cluster

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
	c := newCluster(t, []string{"twitter"}, map[string]interface{}{
		"/twitter/tweet/1": map[string]string{
			"user":      "kimchy",
			"post_date": "2009-11-15T14:12:12",
			"message":   "trying out Elastic Search",
		},
	})
	defer c.Shutdown()

	q := es.QueryWrapper(es.TermQuery(es.TermQueryParams{
		Query: &es.Wrapper{
			Name:    "user",
			Wrapped: "kimchy",
		},
	}))

	request := &es.SearchRequest{
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
		t.Errorf("expected %d, got %d", expected, got)
	}

	t.Logf("OK, %d hit(s), %dms", response.HitsWrapper.Total, response.Took)
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
