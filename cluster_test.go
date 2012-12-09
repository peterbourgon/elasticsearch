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
	"os"
	"os/exec"
	"testing"
	"time"
)

const (
	ES_VERSION = "0.19.12"
)

var (
	SCRIPT = fmt.Sprintf("elasticsearch-%s/bin/elasticsearch", ES_VERSION)
)

// Just tests es.Cluster internals: doesn't make any real connection.
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

func TestWithCluster(t *testing.T) {
	withCluster(
		t,
		testSimpleTermQuery,
	)
}

func testSimpleTermQuery(t *testing.T, c *es.Cluster) {
	q := es.QueryWrapper(es.TermQuery(es.TermQueryParams{
		Query: &es.Wrapper{
			Name:    "user",
			Wrapped: "kimchy",
		},
	}))

	request := es.NewSearchRequest("twitter", "tweet", q)
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

type testClusterFunc func(*testing.T, *es.Cluster)

func withCluster(t *testing.T, tests ...testClusterFunc) {
	eraseData(t)
	p := startNode(t)
	loadData(t)
	defer stopNode(t, p)

	endpoints := []string{"http://127.0.0.1:9200"}
	pingInterval, pingTimeout := 10*time.Second, 3*time.Second
	c := es.NewCluster(endpoints, pingInterval, pingTimeout)
	defer c.Shutdown()

	for _, f := range tests {
		f(t, c)
	}
}

func startNode(t *testing.T) *os.Process {
	ensureNode(t)

	// start the node in the foreground
	began := time.Now()
	cmd := exec.Command(SCRIPT, "-f")
	err := cmd.Start()
	if err != nil {
		t.Fatal(err)
	}

	// hit port 9200 until it's available
	backoffDelay := 50 * time.Millisecond
	timeout := time.After(15 * time.Second)
	func() {
		for {
			backoff := time.After(backoffDelay)
			select {
			case <-backoff:
				if _, err := http.Get("http://localhost:9200"); err == nil {
					t.Logf("ElasticSearch node available after %s", time.Since(began))
					return // success
				}
				backoffDelay *= 2
				t.Logf("ElasticSearch node unavailable, will retry in %s", backoffDelay)

			case <-timeout:
				cmd.Process.Kill() // too late; ignore errors for now
				t.Fatalf("ES node didn't start in time (PID %d)", cmd.Process.Pid)
			}
		}
	}()

	return cmd.Process
}

func stopNode(t *testing.T, p *os.Process) {
	if err := p.Kill(); err != nil {
		t.Fatal(err)
	}

	_, err := p.Wait()
	if err != nil {
		t.Fatal(err)
	}
}

func ensureNode(t *testing.T) {
	_, err := os.Stat(SCRIPT)
	if err == nil {
		return
	}

	os.Setenv("PATH", "$PATH:/bin:/usr/bin:/usr/local/bin:/sbin:/usr/sbin:/usr/local/sbin")
	wget, err := exec.LookPath("wget")
	if err != nil {
		t.Fatal(err)
	}

	tuple := fmt.Sprintf("elasticsearch-%s", ES_VERSION)
	tarGz := fmt.Sprintf("%s.tar.gz", tuple)
	if _, err := os.Stat(tarGz); err != nil {
		url := fmt.Sprintf("https://github.com/downloads/elasticsearch/elasticsearch/%s", tarGz)
		if err := exec.Command(wget, url).Run(); err != nil {
			t.Fatal(err)
		}
	}

	if err := exec.Command("tar", "zxvf", tarGz).Run(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(SCRIPT); err != nil {
		t.Fatal(err)
	}
}

func loadData(t *testing.T) {
	pathBody := map[string]interface{}{
		"/twitter/tweet/1": map[string]string{
			"user":      "kimchy",
			"post_date": "2009-11-15T14:12:12",
			"message":   "trying out Elastic Search",
		},
	}

	for path, body := range pathBody {
		reqBytes, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reqBuf := bytes.NewBuffer(reqBytes)

		// refresh=true to make document(s) immediately searchable
		url := "http://127.0.0.1:9200" + path + "?refresh=true"
		req, err := http.NewRequest("PUT", url, reqBuf)
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

func eraseData(t *testing.T) {
	tuple := fmt.Sprintf("elasticsearch-%s", ES_VERSION)
	dataDir := fmt.Sprintf("%s/data", tuple)
	if err := exec.Command("rm", "-rfv", dataDir).Run(); err != nil {
		t.Error(err)
	}
}
