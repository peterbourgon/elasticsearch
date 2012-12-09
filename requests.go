package elasticsearch

import (
	"fmt"
	"strings"
)

type SearchRequest struct {
	Indices []string
	Types   []string
	Query   SubQuery
}

func (r *SearchRequest) Path() string {
	switch true {
	case len(r.Indices) == 0 && len(r.Types) == 0:
		return "/_search" // all indices, all types

	case len(r.Indices) > 0 && len(r.Types) == 0:
		return fmt.Sprintf("/%s/_search", strings.Join(r.Indices, ","))

	case len(r.Indices) == 0 && len(r.Types) > 0:
		return fmt.Sprintf("/_all/%s/_search", strings.Join(r.Types, ","))

	case len(r.Indices) > 0 && len(r.Types) > 0:
		return fmt.Sprintf(
			"/%s/%s/_search",
			strings.Join(r.Indices, ","),
			strings.Join(r.Types, ","),
		)
	}
	panic("unreachable")
}
