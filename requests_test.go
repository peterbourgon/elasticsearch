package elasticsearch_test

import (
	es "github.com/peterbourgon/elasticsearch"
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

func TestSearchRequestValues(t *testing.T) {
	for _, tuple := range []struct {
		r        es.SearchRequest
		expected string
	}{
		{
			r: es.SearchRequest{
				Params: url.Values{"preference": []string{"foo"}},
			},
			expected: "preference=foo",
		},
	} {
		if expected, got := tuple.expected, tuple.r.Values().Encode(); expected != got {
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

func TestSearchRequestUnspecifiedValues(t *testing.T) {
	request := es.SearchRequest{}
	v := request.Values()
	a := v.Get("foo") // shouldn't crash

	if expected, got := 0, len(a); expected != got {
		t.Errorf("len(foo): expected %d, got %d", expected, got)
	}
}

func TestMultiSearchRequestMergesValues(t *testing.T) {
	requests := []es.SearchRequest{
		es.SearchRequest{
			Params: url.Values{"foo": []string{"a", "b"}},
		},
		es.SearchRequest{
			Params: url.Values{"foo": []string{"b", "c"}},
		},
	}
	request := es.MultiSearchRequest(requests)
	v := request.Values()
	a := v["foo"]
	t.Logf("foo: %v", a)

	if expected, got := 4, len(a); expected != got {
		t.Errorf("len(foo): expected %d, got %d", expected, got)
	}
}
