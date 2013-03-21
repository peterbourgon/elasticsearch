package elasticsearch

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// A Node is a structure which represents a single ElasticSearch host.
type Node struct {
	sync.RWMutex
	endpoint   string
	health     Health
	client     *http.Client // default http client
	pingClient *http.Client // used for Ping() only
}

// NewNode constructs a Node handle. The endpoint should be of the form
// "scheme://host:port", eg. "http://es001:9200".
//
// The ping interval is dictated at a higher level (the Cluster), but individual
// ping timeouts are stored with the Nodes themselves, in a custom HTTP client,
// with a timeout as part of the Transport dialer. This custom pingClient is
// used exclusively for Ping() calls.
//
// Regular queries are made with the default client http.Client, which has
// no explicit timeout set in the Transport dialer.
func NewNode(endpoint string, pingTimeout time.Duration) *Node {
	return &Node{
		endpoint: endpoint,
		health:   Yellow,
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 250,
			},
		},
		pingClient: &http.Client{
			Transport: &http.Transport{
				Dial: timeoutDialer(pingTimeout),
			},
		},
	}
}

// Ping attempts to HTTP GET a specific endpoint, parse some kind of
// status indicator, and returns true if everything was successful.
func (n *Node) Ping() bool {
	u, err := url.Parse(n.endpoint)
	if err != nil {
		log.Printf("ElasticSearch: ping: resolve: %s", err)
		return false
	}
	u.Path = "/_cluster/nodes/_local" // some arbitrary, reasonable endpoint

	resp, err := n.pingClient.Get(u.String())
	if err != nil {
		log.Printf("ElasticSearch: ping %s: GET: %s", u.Host, err)
		return false
	}
	defer resp.Body.Close()

	var status struct {
		OK bool `json:"ok"`
	}

	if err = json.NewDecoder(resp.Body).Decode(&status); err != nil {
		log.Printf("ElasticSearch: ping %s: %s", u.Host, err)
		return false
	}

	if !status.OK {
		log.Printf("ElasticSearch: ping %s: ok=false", u.Host)
		return false
	}

	return true
}

// PingAndSet performs a Ping, and updates the Node's health accordingly.
func (n *Node) pingAndSet() {
	success := n.Ping()
	func() {
		n.Lock()
		defer n.Unlock()
		if success {
			n.health = n.health.Improve()
		} else {
			n.health = n.health.Degrade()
		}
	}()
}

// GetHealth returns the health of the node, for use in the Cluster's GetBest.
func (n *Node) GetHealth() Health {
	n.RLock()
	defer n.RUnlock()
	return n.health
}

// Executes the Fireable f against the node and decodes the server's reply into
// response.
func (n *Node) Execute(f Fireable, response interface{}) error {
	uri, err := url.Parse(n.endpoint)
	if err != nil {
		return err
	}

	request, err := f.Request(uri)
	if err != nil {
		return err
	}

	r, err := n.client.Do(request)
	if err != nil {
		return err
	}

	defer r.Body.Close()

	return json.NewDecoder(r.Body).Decode(response)
}

//
//
//

type Nodes []*Node

// PingAll triggers simultaneous PingAndSets across all Nodes,
// and blocks until they've all completed.
func (n Nodes) pingAll() {
	c := make(chan bool, len(n))
	for _, node := range n {
		go func(tgt *Node) { tgt.pingAndSet(); c <- true }(node)
	}
	for i := 0; i < cap(c); i++ {
		<-c
	}
}

// GetBest returns the "best" Node, as decided by each Node's health.
// It's possible that no Node will be healthy enough to be returned.
// In that case, GetBest returns an error, and processing cannot continue.
func (n Nodes) getBest() (*Node, error) {
	green, yellow := []*Node{}, []*Node{}
	for _, node := range n {
		switch node.GetHealth() {
		case Green:
			green = append(green, node)
		case Yellow:
			yellow = append(yellow, node)
		}
	}

	if len(green) > 0 {
		return green[rand.Intn(len(green))], nil
	}

	if len(yellow) > 0 {
		return yellow[rand.Intn(len(yellow))], nil
	}

	return nil, fmt.Errorf("no healthy nodes available")
}

//
//
//

// Health is some encoding of the perceived state of a Node.
// A Cluster should favor sending queries against healthier nodes.
type Health int

const (
	Green Health = iota // resemblance to cluster health codes is coincidental
	Yellow
	Red
)

func (h Health) String() string {
	switch h {
	case Green:
		return "Green"
	case Yellow:
		return "Yellow"
	case Red:
		return "Red"
	}
	panic("unreachable")
}

func (h Health) Improve() Health {
	switch h {
	case Red:
		return Yellow
	default:
		return Green
	}
	panic("unreachable")
}

func (h Health) Degrade() Health {
	switch h {
	case Green:
		return Yellow
	default:
		return Red
	}
	panic("unreachable")
}

//
//
//

// timeoutDialer returns a function that can be put into an HTTP Client's
// Transport, which will cause all requests made on that client to abort
// if they're not handled within the passed duration.
func timeoutDialer(d time.Duration) func(net, addr string) (net.Conn, error) {
	return func(netw, addr string) (net.Conn, error) {
		c, err := net.Dial(netw, addr)
		if err != nil {
			return nil, err
		}
		c.SetDeadline(time.Now().Add(d))
		return c, nil
	}
}
