package elasticsearch

import (
	"time"
)

// A Cluster is an actively-managed collection of Nodes. Cluster implements
// Searcher, so you can treat it as a single entity. Its Search method chooses
// the best Node to receive the Request.
type Cluster struct {
	nodes        Nodes
	pingInterval time.Duration
	shutdown     chan chan bool
}

// NewCluster returns a new, actively-managed Cluster, representing the
// passed endpoints as Nodes. Each endpoint should be of the form
// scheme://host:port, for example http://es001:9200.
//
// The Cluster will ping each Node on a schedule dictated by pingInterval.
// Each node has pingTimeout to respond before the ping is marked as failed.
//
// TODO node discovery from the list of seed-nodes.
func NewCluster(endpoints []string, pingInterval, pingTimeout time.Duration) *Cluster {
	nodes := Nodes{}
	for _, endpoint := range endpoints {
		nodes = append(nodes, NewNode(endpoint, pingTimeout))
	}

	c := &Cluster{
		nodes:        nodes,
		pingInterval: pingInterval,
		shutdown:     make(chan chan bool),
	}
	go c.loop()
	return c
}

// loop is the event dispatcher for a Cluster. It manages the regular pinging of
// Nodes, and serves incoming requests. Because every request against the
// cluster must pass through here, it cannot block.
func (c *Cluster) loop() {
	ticker := time.Tick(c.pingInterval)
	for {
		select {
		case <-ticker:
			go c.nodes.pingAll()

		case q := <-c.shutdown:
			q <- true
			return
		}
	}
}

// Search implements the Searcher interface for a Cluster. It executes the
// request against a suitable node.
func (c *Cluster) Search(r SearchRequest) (response SearchResponse, err error) {
	err = c.Execute(r, &response)
	return
}

// MultiSearch implements the MultiSearcher interface for a Cluster. It
// executes the search request against a suitable node.
func (c *Cluster) MultiSearch(r MultiSearchRequest) (response MultiSearchResponse, err error) {
	err = c.Execute(r, &response)
	return
}

func (c *Cluster) Index(r IndexRequest) (response IndexResponse, err error) {
	err = c.Execute(r, &response)
	return
}

func (c *Cluster) Create(r CreateRequest) (response IndexResponse, err error) {
	err = c.Execute(r, &response)
	return
}

func (c *Cluster) Update(r UpdateRequest) (response IndexResponse, err error) {
	err = c.Execute(r, &response)
	return
}

func (c *Cluster) Delete(r DeleteRequest) (response IndexResponse, err error) {
	err = c.Execute(r, &response)
	return
}

func (c *Cluster) Bulk(r BulkRequest) (response BulkResponse, err error) {
	err = c.Execute(r, &response)
	return
}

// Executes the request against a suitable node and decodes server's reply into
// response.
func (c *Cluster) Execute(f Fireable, response interface{}) error {
	node, err := c.nodes.getBest()
	if err != nil {
		return err
	}

	return node.Execute(f, response)
}

// Shutdown terminates the Cluster's event dispatcher.
func (c *Cluster) Shutdown() {
	q := make(chan bool)
	c.shutdown <- q
	<-q
}
