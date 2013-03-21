package elasticsearch_test

import (
	es "github.com/peterbourgon/elasticsearch"
	"io/ioutil"
	"net/url"
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
				es.SearchParams{
					Indices: []string{},
					Types:   []string{},
				},
				nil,
			},
			expected: "/_search",
		},
		{
			r: es.SearchRequest{
				es.SearchParams{
					Indices: []string{"i1"},
					Types:   []string{},
				},
				nil,
			},
			expected: "/i1/_search",
		},
		{
			r: es.SearchRequest{
				es.SearchParams{
					Indices: []string{},
					Types:   []string{"t1"},
				},
				nil,
			},
			expected: "/_all/t1/_search",
		},
		{
			r: es.SearchRequest{
				es.SearchParams{
					Indices: []string{"i1"},
					Types:   []string{"t1"},
				},
				nil,
			},
			expected: "/i1/t1/_search",
		},
		{
			r: es.SearchRequest{
				es.SearchParams{
					Indices: []string{"i1", "i2"},
					Types:   []string{},
				},
				nil,
			},
			expected: "/i1,i2/_search",
		},
		{
			r: es.SearchRequest{
				es.SearchParams{
					Indices: []string{},
					Types:   []string{"t1", "t2", "t3"},
				},
				nil,
			},
			expected: "/_all/t1,t2,t3/_search",
		},
		{
			r: es.SearchRequest{
				es.SearchParams{
					Indices: []string{"i1", "i2"},
					Types:   []string{"t1", "t2", "t3"},
				},
				nil,
			},
			expected: "/i1,i2/t1,t2,t3/_search",
		},
	} {
		if expected, got := tuple.expected, tuple.r.Path(); expected != got {
			t.Errorf("%v: expected '%s', got '%s'", tuple.r, expected, got)
		}
	}
}

func TestSearchRequestValues(t *testing.T) {
	for _, tuple := range []struct {
		r        es.SearchRequest
		expected string
	}{
		{
			r: es.SearchRequest{
				Params: es.SearchParams{
					Preference: "foo",
				},
			},
			expected: "preference=foo",
		},
	} {
		if expected, got := tuple.expected, tuple.r.Params.Values().Encode(); expected != got {
			t.Errorf("%v: expected '%s', got '%s'", tuple.r, expected, got)
		}
	}
}

func TestMultiSearchRequestBody(t *testing.T) {
	m := es.MultiSearchRequest{
		es.MultiSearchParams{},
		[]es.SearchRequest{
			es.SearchRequest{
				es.SearchParams{
					Indices: []string{},
					Types:   []string{},
				},
				map[string]interface{}{"query": "1"},
			},
			es.SearchRequest{
				es.SearchParams{
					Indices: []string{"i1"},
					Types:   []string{},
				},
				map[string]interface{}{"query": "2"},
			},
			es.SearchRequest{
				es.SearchParams{
					Indices: []string{},
					Types:   []string{"t1"},
				},
				map[string]interface{}{"query": "3"},
			},
			es.SearchRequest{
				es.SearchParams{
					Indices: []string{"i1"},
					Types:   []string{"t1"},
				},
				map[string]interface{}{"query": "4"},
			},
			es.SearchRequest{
				es.SearchParams{
					Indices: []string{"i1", "i2"},
					Types:   []string{"t1", "t2", "t3"},
				},
				map[string]interface{}{"query": "5"},
			},
		},
	}

	req, err := m.Request(&url.URL{})

	if expected, got := "/_msearch", req.URL.Path; expected != got {
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
	got, err := ioutil.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	}
	if expected != string(got) {
		t.Errorf("Body: expected:\n---\n%s\n---\ngot:\n---\n%s\n---\n", expected, got)
	}
}
