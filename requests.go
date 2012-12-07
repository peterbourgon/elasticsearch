package elasticsearch

import (
	"fmt"
	"strings"
)

type Request interface {
	Indices() []string
	Types() []string
	Query() []byte
}

func Path(r Request) string {
	switch indices, types := r.Indices(), r.Types(); true {
	case len(indices) == 0 && len(types) == 0:
		return "/_search" // all indices, all types
	case len(indices) > 0 && len(types) == 0:
		return fmt.Sprintf("/%s/_search", strings.Join(indices, ","))
	case len(indices) == 0 && len(types) > 0:
		return fmt.Sprintf("/_all/%s/_search", strings.Join(types, ","))
	case len(indices) > 0 && len(types) > 0:
		return fmt.Sprintf(
			"/%s/%s/_search",
			strings.Join(indices, ","),
			strings.Join(types, ","),
		)
	}
	panic("unreachable")
}

//
//
//

type SearchRequest struct {
	indices []string
	types   []string
	query   []byte // JSON-encoded search query
}

func (r *SearchRequest) Indices() []string { return r.indices }
func (r *SearchRequest) Types() []string   { return r.types }
func (r *SearchRequest) Query() []byte     { return r.query }

func NewSearchRequest(index, typ string, query []byte) *SearchRequest {
	return &SearchRequest{
		indices: []string{index},
		types:   []string{typ},
		query:   query,
	}
}
