package elasticsearch

import (
	"time"
)

// A Cluster is an actively-managed collection of Nodes. Cluster implements
// Searcher, so you can treat it as a single entity. Its Search method chooses
// the best Node to receive the Request.
type Cluster struct {
	nodes              Nodes
	pingInterval       time.Duration
	searchBundles      chan searchBundle
	multiSearchBundles chan multiSearchBundle
	shutdown           chan chan bool
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
		nodes:              nodes,
		pingInterval:       pingInterval,
		searchBundles:      make(chan searchBundle),
		multiSearchBundles: make(chan multiSearchBundle),
		shutdown:           make(chan chan bool),
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

		case b := <-c.searchBundles:
			// getBest should be effectively nonblocking.
			// TODO refactor to generator architecture, with channels (?)
			node, err := c.nodes.getBest()
			if err != nil {
				b.err <- err
				continue
			}
			// Query will be blocking, so we fire it in a separate goroutine.
			go sendSearchBundle(node, b)

		case b := <-c.multiSearchBundles: // TODO combine with searchBundle?
			// getBest should be effectively nonblocking.
			// TODO refactor to generator architecture, with channels (?)
			node, err := c.nodes.getBest()
			if err != nil {
				b.err <- err
				continue
			}
			// Query will be blocking, so we fire it in a separate goroutine.
			go sendMultiSearchBundle(node, b)

		case q := <-c.shutdown:
			q <- true
			return
		}
	}
}

// Search implements the Searcher interface for a Cluster. It creates a
// searchBundle, forwards it to the Cluster's event dispatcher, and blocks
// for a response (or error).
func (c *Cluster) Search(r SearchRequest) (SearchResponse, error) {
	b := makeSearchBundle(r)
	c.searchBundles <- b
	select {
	case response := <-b.response:
		return response, nil
	case err := <-b.err:
		return SearchResponse{}, err
	}
	panic("unreachable")
}

// MultiSearch implements the MultiSearcher interface for a Cluster. It creates
// a multiSearchBundle, forwards it to the Cluster's event dispatcher, and
// blocks for a response (or error).
func (c *Cluster) MultiSearch(r MultiSearchRequest) (MultiSearchResponse, error) {
	b := makeMultiSearchBundle(r)
	c.multiSearchBundles <- b
	select {
	case response := <-b.response:
		return response, nil
	case err := <-b.err:
		return MultiSearchResponse{}, err
	}
	panic("unreachable")
}

// Shutdown terminates the Cluster's event dispatcher.
func (c *Cluster) Shutdown() {
	q := make(chan bool)
	c.shutdown <- q
	<-q
}

//
//
//

// searchBundle wraps a SearchRequest with response and error channels, so
// that it can be processed by the Cluster's event dispatcher.
type searchBundle struct {
	request  SearchRequest
	response chan SearchResponse
	err      chan error
}

// makeSearchBundle produces a searchBundle from a SearchRequest.
func makeSearchBundle(r SearchRequest) searchBundle {
	return searchBundle{
		request:  r,
		response: make(chan SearchResponse),
		err:      make(chan error),
	}
}

// sendSearchBundle sends the Request in the searchBundle to the given Searcher.
// It forwards the response, or the error, along the appropriate channel in the
// searchBundle. It should be called in a new goroutine from the Cluster's event
// dispatcher.
func sendSearchBundle(s Searcher, b searchBundle) {
	response, err := s.Search(b.request)
	if err != nil {
		b.err <- err
		return
	}
	b.response <- response
}

//
//
//

// multiSearchBundle wraps a MultiSearchRequest with response and error
// channels, so that it can be processed by the Cluster's event dispatcher.
type multiSearchBundle struct {
	request  MultiSearchRequest
	response chan MultiSearchResponse
	err      chan error
}

// makeMultiSearchBundle produces a multiSearchBundle from a MultiSearchRequest.
func makeMultiSearchBundle(r MultiSearchRequest) multiSearchBundle {
	return multiSearchBundle{
		request:  r,
		response: make(chan MultiSearchResponse),
		err:      make(chan error),
	}
}

// sendMultiSearchBundle sends the request in the multiSearchBundle to the given
// MultiSearcher. It forwards the response, or the error, along the appropriate
// channel in the multiSearchBundle. It should be called in a new goroutine from
// the Cluster's event dispatcher.
func sendMultiSearchBundle(s MultiSearcher, b multiSearchBundle) {
	response, err := s.MultiSearch(b.request)
	if err != nil {
		b.err <- err
		return
	}
	b.response <- response
}
