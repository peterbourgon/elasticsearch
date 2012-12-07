package elasticsearch_test

import (
	"fmt"
	es "github.com/peterbourgon/elasticsearch"
	"testing"
	"time"
)

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
