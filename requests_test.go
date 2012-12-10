package elasticsearch_test

import (
	es "github.com/peterbourgon/elasticsearch"
	"strings"
	"testing"
)

func TestSearchRequestPath(t *testing.T) {
	for _, tuple := range []struct {
		r        es.SearchRequest
		expected string
	}{
		{
			r: es.SearchRequest{
				Indices: []string{},
				Types:   []string{},
			},
			expected: "/_search",
		},
		{
			r: es.SearchRequest{
				Indices: []string{"i1"},
				Types:   []string{},
			},
			expected: "/i1/_search",
		},
		{
			r: es.SearchRequest{
				Indices: []string{},
				Types:   []string{"t1"},
			},
			expected: "/_all/t1/_search",
		},
		{
			r: es.SearchRequest{
				Indices: []string{"i1"},
				Types:   []string{"t1"},
			},
			expected: "/i1/t1/_search",
		},
		{
			r: es.SearchRequest{
				Indices: []string{"i1", "i2"},
				Types:   []string{},
			},
			expected: "/i1,i2/_search",
		},
		{
			r: es.SearchRequest{
				Indices: []string{},
				Types:   []string{"t1", "t2", "t3"},
			},
			expected: "/_all/t1,t2,t3/_search",
		},
		{
			r: es.SearchRequest{
				Indices: []string{"i1", "i2"},
				Types:   []string{"t1", "t2", "t3"},
			},
			expected: "/i1,i2/t1,t2,t3/_search",
		},
	} {
		if expected, got := tuple.expected, tuple.r.Path(); expected != got {
			t.Errorf("%v: expected '%s', got '%s'", tuple.r, expected, got)
		}
	}
}

func TestMultiSearchRequestBody(t *testing.T) {
	m := es.MultiSearchRequest([]es.SearchRequest{
		es.SearchRequest{
			Indices: []string{},
			Types:   []string{},
			Query:   map[string]interface{}{"query": "1"},
		},
		es.SearchRequest{
			Indices: []string{"i1"},
			Types:   []string{},
			Query:   map[string]interface{}{"query": "2"},
		},
		es.SearchRequest{
			Indices: []string{},
			Types:   []string{"t1"},
			Query:   map[string]interface{}{"query": "3"},
		},
		es.SearchRequest{
			Indices: []string{"i1"},
			Types:   []string{"t1"},
			Query:   map[string]interface{}{"query": "4"},
		},
		es.SearchRequest{
			Indices: []string{"i1", "i2"},
			Types:   []string{"t1", "t2", "t3"},
			Query:   map[string]interface{}{"query": "5"},
		},
	})

	if expected, got := "/_msearch", m.Path(); expected != got {
		t.Errorf("Path: expected '%s', got '%s'", expected, got)
	}

	expected := strings.Join(
		[]string{
			`{}`,
			`{"query":"1"}`,
			`{"index":["i1"]}`,
			`{"query":"2"}`,
			`{"type":["t1"]}`,
			`{"query":"3"}`,
			`{"index":["i1"],"type":["t1"]}`,
			`{"query":"4"}`,
			`{"index":["i1","i2"],"type":["t1","t2","t3"]}`,
			`{"query":"5"}`,
		},
		"\n",
	) + "\n"
	got, err := m.Body()
	if err != nil {
		t.Fatal(err)
	}
	if expected != string(got) {
		t.Errorf("Body: expected:\n---\n%s\n---\ngot:\n---\n%s\n---\n", expected, got)
	}
}
