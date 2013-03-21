package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// Helper function which turns a map of strings into url.Values, omitting empty
// values.
func values(v map[string]string) url.Values {
	values := url.Values{}

	for key, value := range v {
		if value != "" {
			values.Set(key, value)
		}
	}

	return values
}

// Fireable defines anything which can be fired against the search cluster.
type Fireable interface {
	Request(uri *url.URL) (*http.Request, error)
}

//
//
//

type SearchParams struct {
	Indices []string `json:"index,omitempty"`
	Types   []string `json:"type,omitempty"`

	Routing    string `json:"routing,omitempty"`
	Preference string `json:"preference,omitempty"`
	SearchType string `json:"search_type,omitempty"`
}

func (p SearchParams) Values() url.Values {
	return values(map[string]string{
		"routing":     p.Routing,
		"preference":  p.Preference,
		"search_type": p.SearchType,
	})
}

type SearchRequest struct {
	Params SearchParams
	Query  SubQuery
}

func (r SearchRequest) EncodeMultiHeader(enc *json.Encoder) error {
	return enc.Encode(r.Params)
}

func (r SearchRequest) EncodeQuery(enc *json.Encoder) error {
	return enc.Encode(r.Query)
}

func (r SearchRequest) Request(uri *url.URL) (*http.Request, error) {
	uri.Path = r.Path()
	uri.RawQuery = r.Params.Values().Encode()

	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)

	if err := r.EncodeQuery(enc); err != nil {
		return nil, err
	}

	return http.NewRequest("GET", uri.String(), buf)
}

func (r SearchRequest) Path() string {
	switch true {
	case len(r.Params.Indices) == 0 && len(r.Params.Types) == 0:
		return fmt.Sprintf(
			"/_search", // all indices, all types
		)

	case len(r.Params.Indices) > 0 && len(r.Params.Types) == 0:
		return fmt.Sprintf(
			"/%s/_search",
			strings.Join(r.Params.Indices, ","),
		)

	case len(r.Params.Indices) == 0 && len(r.Params.Types) > 0:
		return fmt.Sprintf(
			"/_all/%s/_search",
			strings.Join(r.Params.Types, ","),
		)

	case len(r.Params.Indices) > 0 && len(r.Params.Types) > 0:
		return fmt.Sprintf(
			"/%s/%s/_search",
			strings.Join(r.Params.Indices, ","),
			strings.Join(r.Params.Types, ","),
		)
	}
	panic("unreachable")
}

//
//
//

type MultiSearchParams struct {
	Indices []string
	Types   []string

	SearchType string
}

func (p MultiSearchParams) Values() url.Values {
	return values(map[string]string{
		"search_type": p.SearchType,
	})
}

type MultiSearchRequest struct {
	Params   MultiSearchParams
	Requests []SearchRequest
}

func (r MultiSearchRequest) Request(uri *url.URL) (*http.Request, error) {
	uri.Path = "/_msearch"
	uri.RawQuery = r.Params.Values().Encode()

	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)

	for _, req := range r.Requests {
		if err := req.EncodeMultiHeader(enc); err != nil {
			return nil, err
		}
		if err := req.EncodeQuery(enc); err != nil {
			return nil, err
		}
	}

	return http.NewRequest("GET", uri.String(), buf)
}
