package elasticsearch

import (
	"encoding/json"
)

// This file contains structures that represent all of the various JSON-
// marshalable queries and sub-queries that are part of the ElasticSearch
// grammar. These structures are one-way: they're only meant to be Marshaled.

type SubQuery interface{}

var nilSubQuery SubQuery

//
//
//

// Wrapper gives a dynamic name to a SubQuery. For example, Name="foo"
// Wrapped=`{"bar": 123}` marshals to `{"foo": {"bar": 123}}`.
//
// You *can* use this directly in your business-logic code, but if you do, it's
// probably a sign you should file an issue (or make a pull request) to give
// your specific use-case proper, first class support, via a FooQuery[Params]
// type-set.
type Wrapper struct {
	Name    string
	Wrapped SubQuery
}

func (w *Wrapper) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]SubQuery{
		w.Name: w.Wrapped,
	})
}

//
//
//

func QueryWrapper(q SubQuery) SubQuery {
	return &Wrapper{
		Name:    "query",
		Wrapped: q,
	}
}

//
//
//

// GenericQueryParams marshal to a valid query object for a large number of
// query types. You generally use them applied to a particular field, ie. scope;
// see FieldedGenericQuery.
type GenericQueryParams struct {
	Query              string  `json:"query,omitempty"`
	Analyzer           string  `json:"analyzer,omitempty"`
	Type               string  `json:"type,omitempty"`
	MaxExpansions      string  `json:"max_expansions,omitempty"`
	Boost              float32 `json:"boost,omitempty"`
	Operator           string  `json:"operator,omitempty"`
	MinimumShouldMatch string  `json:"minimum_should_match,omitempty"`
	CutoffFrequency    float32 `json:"cutoff_frequency,omitempty"`
}

// FieldedGenericQuery returns a SubQuery representing the passed QueryParams
// applied to the given scope, ie. field. The returned SubQuery can be used as
// the Query in a MatchQuery, for example.
func FieldedGenericQuery(field string, p GenericQueryParams) SubQuery {
	return &Wrapper{
		Name:    field,
		Wrapped: p,
	}
}

//
//
//

// http://www.elasticsearch.org/guide/reference/query-dsl/match-query.html
type MatchQueryParams struct {
	Query SubQuery `json:"match"`
}

func MatchQuery(p MatchQueryParams) SubQuery {
	return p
}

//
//
//

// http://www.elasticsearch.org/guide/reference/query-dsl/term-query.html
// Typically `Query` would be &Wrapper{Name: "fieldname", Wrapped: "value"}.
type TermQueryParams struct {
	Query SubQuery `json:"term"`
}

func TermQuery(p TermQueryParams) SubQuery {
	return p
}

//
//
//

// http://www.elasticsearch.org/guide/reference/query-dsl/terms-query.html
// "a simpler syntax query for using a `bool` query with several `term` queries
// in the `should` clauses."
type TermsQueryParams struct {
	Query SubQuery `json:"terms"` // often `{ "field": ["value1", "value2"] }`
}

func TermsQuery(p TermsQueryParams) SubQuery {
	return p
}

//
//
//

// http://www.elasticsearch.org/guide/reference/query-dsl/dis-max-query.html
type DisMaxQueryParams struct {
	Queries    []SubQuery `json:"queries"`
	Boost      float32    `json:"boost,omitempty"`
	TieBreaker float32    `json:"tie_breaker,omitempty"`
}

func DisMaxQuery(p DisMaxQueryParams) SubQuery {
	return &Wrapper{
		Name:    "dis_max",
		Wrapped: p,
	}
}

//
//
//

// http://www.elasticsearch.org/guide/reference/query-dsl/bool-query.html
type BoolQueryParams struct {
	Must                     SubQuery `json:"must,omitempty"` // can be slice!
	Should                   SubQuery `json:"should,omitempty"`
	MustNot                  SubQuery `json:"must_not,omitempty"`
	MinimumNumberShouldMatch int      `json:"minimum_number_should_match,omitempty"`
	Boost                    float32  `json:"boost,omitempty"`
}

func BoolQuery(p BoolQueryParams) SubQuery {
	return &Wrapper{
		Name:    "bool",
		Wrapped: p,
	}
}

//
//
//

// http://www.elasticsearch.org/guide/reference/query-dsl/custom-score-query.html
type CustomScoreQueryParams struct {
	Script string                 `json:"script"`
	Lang   string                 `json:"lang"`
	Params map[string]interface{} `json:"params"`
	Query  SubQuery               `json:"query"`
}

func CustomScoreQuery(p CustomScoreQueryParams) SubQuery {
	return &Wrapper{
		Name:    "custom_score",
		Wrapped: p,
	}
}

//
//
//

type ConstantScoreQueryParams struct {
	Query  SubQuery       `json:"query,omitempty"`
	Filter FilterSubQuery `json:"filter,omitempty"`
	Boost  float32        `json:"boost,omitempty"`
}

func ConstantScoreQuery(p ConstantScoreQueryParams) SubQuery {
	return &Wrapper{
		Name:    "constant_score",
		Wrapped: p,
	}
}

//
//
//

func MatchAllQuery() SubQuery {
	return &Wrapper{
		Name:    "match_all",
		Wrapped: map[string]interface{}{}, // render to '{}'
	}
}

//
//
//

// Haven't quite figured out how to best represent this.
// TODO break these up into embeddable query-parts?
type OffsetLimitFacetsFilterQueryParams struct {
	Offset int            `json:"from"`
	Limit  int            `json:"size"`
	Facets FacetSubQuery  `json:"facets,omitempty"`
	Filter FilterSubQuery `json:"filter,omitempty"`
	Query  SubQuery       `json:"query"`
}

//
//
//
// =============================================================================
// HERE BE FILTERS
// =============================================================================
//
//
//

type FilterSubQuery SubQuery

func MakeFilter(filter SubQuery) FilterSubQuery {
	return FilterSubQuery(filter)
}

func MakeFilters(filters []SubQuery) []FilterSubQuery {
	a := []FilterSubQuery{}
	for _, filter := range filters {
		a = append(a, MakeFilter(filter))
	}
	return a
}

type BooleanFiltersParams struct {
	AndFilters []FilterSubQuery
	OrFilters  []FilterSubQuery
}

func BooleanFilters(p BooleanFiltersParams) SubQuery {
	switch nAnd, nOr := len(p.AndFilters), len(p.OrFilters); true {
	case nAnd <= 0 && nOr <= 0:
		return map[string]interface{}{} // render to '{}'

	case nAnd > 0 && nOr <= 0:
		return &Wrapper{
			Name:    "and",
			Wrapped: p.AndFilters,
		}

	case nAnd <= 0 && nOr > 0:
		return &Wrapper{
			Name:    "or",
			Wrapped: p.OrFilters,
		}

	case nAnd > 0 && nOr > 0:
		combinedFilters := append(p.AndFilters, &Wrapper{
			Name:    "or",
			Wrapped: p.OrFilters,
		})
		return &Wrapper{
			Name:    "and",
			Wrapped: combinedFilters,
		}
	}
	panic("unreachable")
}

//
//
//

type QueryFilterParams struct {
	Query SubQuery `json:"query"`
}

func QueryFilter(p QueryFilterParams) FilterSubQuery {
	return p // no need for another layer of indirection; just like a typecast
}

//
//
//

type TermFilterParams struct {
	Field string
	Value string // for multiple values, use TermsFilter
}

func TermFilter(p TermFilterParams) FilterSubQuery {
	return &Wrapper{
		Name: "term",
		Wrapped: &Wrapper{
			Name:    p.Field,
			Wrapped: p.Value,
		},
	}
}

type TermsFilterParams struct {
	Field  string
	Values []string
}

func TermsFilter(p TermsFilterParams) FilterSubQuery {
	return &Wrapper{
		Name: "terms",
		Wrapped: &Wrapper{
			Name:    p.Field,
			Wrapped: p.Values,
		},
	}
}

//
//
//

// http://www.elasticsearch.org/guide/reference/query-dsl/type-filter.html
type FieldedFilterParams struct {
	Value string `json:"value"`
}

// TODO I guess remove all Fielded* functions except FieldedGenericQuery?
// TODO and rename FieldedGenericQuery to like FieldedSubObject, maybe?
func FieldedFilter(fieldName string, p FieldedFilterParams) FilterSubQuery {
	return &Wrapper{
		Name:    fieldName,
		Wrapped: p,
	}
}

//
//
//

type RangeSubQuery SubQuery

// http://www.elasticsearch.org/guide/reference/query-dsl/range-filter.html
type RangeFilterParams struct {
	From         string `json:"from,omitempty"`
	To           string `json:"to,omitempty"`
	IncludeLower bool   `json:"include_lower"`
	IncludeUpper bool   `json:"include_upper"`
}

func FieldedRangeSubQuery(field string, p RangeFilterParams) RangeSubQuery {
	return &Wrapper{
		Name:    field,
		Wrapped: p,
	}
}

func RangeFilter(q RangeSubQuery) FilterSubQuery {
	return &Wrapper{
		Name:    "range",
		Wrapped: q,
	}
}

//
//
//
// =============================================================================
// HERE BE FACETS
// =============================================================================
//
//
//

type FacetSubQuery SubQuery

// http://www.elasticsearch.org/guide/reference/api/search/facets/terms-facet.html
type TermsFacetParams struct {
	Field string `json:"field"`
	Size  int    `json:"size"`
}

func TermsFacet(p TermsFacetParams) FacetSubQuery {
	return &Wrapper{
		Name:    "terms",
		Wrapped: p,
	}
}

// TODO other types of facets

// NamedFacet wraps any FooFacet SubQuery so that it can be used
// wherever a facet is called for.
func NamedFacet(name string, q FacetSubQuery) FacetSubQuery {
	return &Wrapper{
		Name:    name,
		Wrapped: q,
	}
}
