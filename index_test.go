package elasticsearch_test

import (
	"encoding/json"
	es "github.com/peterbourgon/elasticsearch"
	"net/url"
	"testing"
)

func TestIndexRequest(t *testing.T) {
	doc := map[string]string{
		"user":      "kimchy",
		"post_date": "2009-11-15T14:12:12",
		"message":   "trying out Elastic Search",
	}

	request, err := es.IndexRequest{
		es.IndexParams{
			Index:     "twitter",
			Type:      "tweet",
			Id:        "1",
			Percolate: "*",
			Version:   "4",
		},
		doc,
	}.Request(&url.URL{})

	if err != nil {
		t.Fatal(err)
	}

	if expected, got := "PUT", request.Method; expected != got {
		t.Errorf("expected method = %q; got %q", expected, got)
	}

	if expected, got := "twitter/tweet/1", request.URL.Path; expected != got {
		t.Errorf("expected path = %q; got %q", expected, got)
	}

	q := request.URL.Query()

	if expected, got := "*", q.Get("percolate"); expected != got {
		t.Errorf("expected percolate = %q; got %q", expected, got)
	}

	if expected, got := "4", q.Get("version"); expected != got {
		t.Errorf("expected version = %q; got %q", expected, got)
	}

	var body struct {
		User     string `json:"user"`
		PostDate string `json:"post_date"`
		Message  string `json:"message"`
	}

	if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}

	if expected, got := doc["user"], body.User; expected != got {
		t.Errorf("expected user = %q; got %q", expected, got)
	}

	if expected, got := doc["post_date"], body.PostDate; expected != got {
		t.Errorf("expected post_date = %q; got %q", expected, got)
	}

	if expected, got := doc["message"], body.Message; expected != got {
		t.Errorf("expected message = %q; got %q", expected, got)
	}
}

func TestCreateRequest(t *testing.T) {
	doc := map[string]string{
		"user":      "kimchy",
		"post_date": "2009-11-15T14:12:12",
		"message":   "trying out Elastic Search",
	}

	request, err := es.CreateRequest{
		es.IndexParams{
			Index:     "twitter",
			Type:      "tweet",
			Id:        "1",
			Percolate: "*",
			Version:   "4",
		},
		doc,
	}.Request(&url.URL{})

	if err != nil {
		t.Fatal(err)
	}

	if expected, got := "PUT", request.Method; expected != got {
		t.Errorf("expected method = %q; got %q", expected, got)
	}

	if expected, got := "twitter/tweet/1/_create", request.URL.Path; expected != got {
		t.Errorf("expected path = %q; got %q", expected, got)
	}

	q := request.URL.Query()

	if expected, got := "*", q.Get("percolate"); expected != got {
		t.Errorf("expected percolate = %q; got %q", expected, got)
	}

	if expected, got := "4", q.Get("version"); expected != got {
		t.Errorf("expected version = %q; got %q", expected, got)
	}

	var body struct {
		User     string `json:"user"`
		PostDate string `json:"post_date"`
		Message  string `json:"message"`
	}

	if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}

	if expected, got := doc["user"], body.User; expected != got {
		t.Errorf("expected user = %q; got %q", expected, got)
	}

	if expected, got := doc["post_date"], body.PostDate; expected != got {
		t.Errorf("expected post_date = %q; got %q", expected, got)
	}

	if expected, got := doc["message"], body.Message; expected != got {
		t.Errorf("expected message = %q; got %q", expected, got)
	}
}

func TestUpdateRequest(t *testing.T) {
	doc := map[string]string{
		"script": `ctx._source.text = "some text"`,
	}

	request, err := es.UpdateRequest{
		es.IndexParams{
			Index:     "twitter",
			Type:      "tweet",
			Id:        "1",
			Percolate: "*",
			Version:   "4",
		},
		doc,
	}.Request(&url.URL{})

	if err != nil {
		t.Fatal(err)
	}

	if expected, got := "POST", request.Method; expected != got {
		t.Errorf("expected method = %q; got %q", expected, got)
	}

	if expected, got := "twitter/tweet/1/_update", request.URL.Path; expected != got {
		t.Errorf("expected path = %q; got %q", expected, got)
	}

	q := request.URL.Query()

	if expected, got := "*", q.Get("percolate"); expected != got {
		t.Errorf("expected percolate = %q; got %q", expected, got)
	}

	if expected, got := "4", q.Get("version"); expected != got {
		t.Errorf("expected version = %q; got %q", expected, got)
	}

	var body struct {
		Script string `json:"script"`
	}

	if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}

	if expected, got := doc["script"], body.Script; expected != got {
		t.Errorf("expected user = %q; got %q", expected, got)
	}
}

func TestDeleteRequest(t *testing.T) {
	request, err := es.DeleteRequest{
		es.IndexParams{
			Index:     "twitter",
			Type:      "tweet",
			Id:        "1",
			Percolate: "*",
			Version:   "4",
		},
	}.Request(&url.URL{})

	if err != nil {
		t.Fatal(err)
	}

	if expected, got := "DELETE", request.Method; expected != got {
		t.Errorf("expected method = %q; got %q", expected, got)
	}

	if expected, got := "twitter/tweet/1", request.URL.Path; expected != got {
		t.Errorf("expected path = %q; got %q", expected, got)
	}

	q := request.URL.Query()

	if expected, got := "*", q.Get("percolate"); expected != got {
		t.Errorf("expected percolate = %q; got %q", expected, got)
	}

	if expected, got := "4", q.Get("version"); expected != got {
		t.Errorf("expected version = %q; got %q", expected, got)
	}

	if request.Body != nil {
		t.Errorf("expected request to have an empty body")
	}
}

func TestBulkRequest(t *testing.T) {
	request, err := es.BulkRequest{
		es.BulkParams{
			Consistency: "quorum",
		},
		[]es.BulkIndexable{
			es.IndexRequest{
				es.IndexParams{
					Index:   "twitter",
					Type:    "tweet",
					Id:      "1",
					Routing: "foo",
				},
				map[string]string{"user": "kimchy"},
			},
			es.CreateRequest{
				es.IndexParams{
					Index:   "twitter",
					Type:    "tweet",
					Id:      "2",
					Version: "2",
				},
				map[string]string{"user": "kimchy2"},
			},
			es.DeleteRequest{
				es.IndexParams{
					Index: "twitter",
					Type:  "tweet",
					Id:    "1",
				},
			},
		},
	}.Request(&url.URL{})

	if err != nil {
		t.Fatal(err)
	}

	if expected, got := "PUT", request.Method; expected != got {
		t.Errorf("expected method = %q; got %q", expected, got)
	}

	if expected, got := "_bulk", request.URL.Path; expected != got {
		t.Errorf("expected path = %q; got %q", expected, got)
	}

	q := request.URL.Query()

	if expected, got := "quorum", q.Get("consistency"); expected != got {
		t.Errorf("expected percolate = %q; got %q", expected, got)
	}

	type actionMetadata struct {
		Index  map[string]string `json:"index"`
		Create map[string]string `json:"create"`
		Delete map[string]string `json:"delete"`
	}

	header := actionMetadata{}
	body := map[string]string{}
	decoder := json.NewDecoder(request.Body)

	if err := decoder.Decode(&header); err != nil {
		t.Fatal(err)
	}

	if header.Index == nil {
		t.Fatal("index metadata was not encoded")
	}

	if expected, got := "twitter", header.Index["_index"]; expected != got {
		t.Errorf("expected _index = %q; got %q", expected, got)
	}

	if expected, got := "tweet", header.Index["_type"]; expected != got {
		t.Errorf("expected _type = %q; got %q", expected, got)
	}

	if expected, got := "1", header.Index["_id"]; expected != got {
		t.Errorf("expected _id = %q; got %q", expected, got)
	}

	if expected, got := "foo", header.Index["_routing"]; expected != got {
		t.Errorf("expected _id = %q; got %q", expected, got)
	}

	if err := decoder.Decode(&body); err != nil {
		t.Fatal(err)
	}

	if expected, got := "kimchy", body["user"]; expected != got {
		t.Errorf("expected user = %q; got %q", expected, got)
	}

	if err := decoder.Decode(&header); err != nil {
		t.Fatal(err)
	}

	if header.Create == nil {
		t.Fatal("create metadata was not encoded")
	}

	if expected, got := "twitter", header.Create["_index"]; expected != got {
		t.Errorf("expected _index = %q; got %q", expected, got)
	}

	if expected, got := "tweet", header.Create["_type"]; expected != got {
		t.Errorf("expected _type = %q; got %q", expected, got)
	}

	if expected, got := "2", header.Create["_id"]; expected != got {
		t.Errorf("expected _id = %q; got %q", expected, got)
	}

	if expected, got := "2", header.Create["_version"]; expected != got {
		t.Errorf("expected _id = %q; got %q", expected, got)
	}

	if err := decoder.Decode(&body); err != nil {
		t.Fatal(err)
	}

	if expected, got := "kimchy2", body["user"]; expected != got {
		t.Errorf("expected user = %q; got %q", expected, got)
	}

	if err := decoder.Decode(&header); err != nil {
		t.Fatal(err)
	}

	if header.Delete == nil {
		t.Fatal("delete metadata was not encoded")
	}

	if expected, got := "twitter", header.Delete["_index"]; expected != got {
		t.Errorf("expected _index = %q; got %q", expected, got)
	}

	if expected, got := "tweet", header.Delete["_type"]; expected != got {
		t.Errorf("expected _type = %q; got %q", expected, got)
	}

	if expected, got := "1", header.Delete["_id"]; expected != got {
		t.Errorf("expected _id = %q; got %q", expected, got)
	}
}
