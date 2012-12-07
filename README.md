# elasticsearch

This is an opinionated library for ElasticSearch in Go. Its opinions are:

* Builders are bad: construct queries declaratively, using nested structures
* Don't be clever: when in doubt, be explicit and dumb

[![Build Status][1]][2]

[1]: https://secure.travis-ci.org/peterbourgon/elasticsearch.png
[2]: http://www.travis-ci.org/peterbourgon/elasticsearch


# Usage

First, it helps to import the package with a short name (package alias).

```go
import es "github.com/peterbourgon/elasticsearch"
```

Create a Cluster, which is an actively-managed handle to a set of nodes.

```go
endpoints := []string{"http://host1:9200", "http://host2:9200"}
pingInterval, pingTimeout := 30*time.Second, 3*time.Second
c := es.NewCluster(endpoints, pingInterval, pingTimeout)
```

Construct queries declaratively, and fire them against the cluster.

```go
q := es.TermQuery(es.TermQueryParams{
	Query: &es.Wrapper{
		Name:    "user",
		Wrapped: "kimchy",
	},
})

request := es.NewSearchRequest("twitter", "tweets", q)
response, err := c.Search(request)
if err != nil {
	// Fatal
}
fmt.Printf("got %d hit(s)", response.HitsWrapper.Total)
```

