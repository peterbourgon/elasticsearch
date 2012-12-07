package elasticsearch_test

import (
	"encoding/json"
	"fmt"
	es "github.com/peterbourgon/elasticsearch"
)

func marshalOrError(q es.SubQuery) string {
	buf, err := json.Marshal(q)
	if err != nil {
		return err.Error()
	}
	return string(buf)
}

// http://www.elasticsearch.org/guide/reference/query-dsl/term-query.html
func ExampleBasicTermQuery() {
	q := es.TermQuery(es.TermQueryParams{
		Query: &es.Wrapper{
			Name:    "user",
			Wrapped: "kimchy",
		},
	})

	fmt.Print(marshalOrError(q))
	// Output:
	// {"term":{"user":"kimchy"}}
}
