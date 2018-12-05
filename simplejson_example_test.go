package simplejson_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/grafana-simple-json-go"
)

// GSJExample demonstrates how to create a new Grafana Simple JSON compatible
// HTTP server.
type GSJExample struct{}

// GrafanaQuery handles timeserie type queries.
func (GSJExample) GrafanaQuery(ctx context.Context, target string, args simplejson.QueryArguments) ([]simplejson.DataPoint, error) {
	return []simplejson.DataPoint{
		{Time: args.To.Add(-5 * time.Second), Value: 1234.0},
		{Time: args.To, Value: 1500.0},
	}, nil
}

func (GSJExample) GrafanaQueryTable(ctx context.Context, target string, args simplejson.TableQueryArguments) ([]simplejson.TableColumn, error) {
	return []simplejson.TableColumn{
		{Text: "Time", Data: simplejson.TableTimeColumn{args.To}},
		{Text: "SomeText", Data: simplejson.TableStringColumn{"blah"}},
		{Text: "Value", Data: simplejson.TableNumberColumn{1.0}},
	}, nil
}

func (GSJExample) GrafanaAnnotations(ctx context.Context, query string, args simplejson.AnnotationsArguments) ([]simplejson.Annotation, error) {
	return []simplejson.Annotation{
		// A single point in time annotation
		{
			Time:  time.Unix(1234, 0),
			Title: "First Title",
			Text:  "First annotation",
		},
		// An annotation over a time range
		{
			Time:    time.Unix(1235, 0),
			TimeEnd: time.Unix(1237, 0),
			Title:   "Second Title",
			Text:    "Second annotation with range",
			Tags:    []string{"outage"},
		},
	}, nil
}

func (GSJExample) GrafanaSearch(ctx context.Context, target string) ([]string, error) {
	return []string{"example1", "example2", "example3"}, nil
}

func (GSJExample) GrafanaAdhocFilterTags(ctx context.Context) ([]simplejson.TagInfoer, error) {
	return []simplejson.TagInfoer{
		simplejson.TagStringKey("mykey"),
	}, nil
}

func (GSJExample) GrafanaAdhocFilterTagValues(ctx context.Context, key string) ([]simplejson.TagValuer, error) {
	return []simplejson.TagValuer{
		simplejson.TagStringValue("value1"),
		simplejson.TagStringValue("value2"),
	}, nil
}

func Example() {
	gsj := simplejson.New(
		simplejson.WithQuerier(GSJExample{}),
		simplejson.WithTableQuerier(GSJExample{}),
		simplejson.WithSearcher(GSJExample{}),
		simplejson.WithAnnotator(GSJExample{}),
	)

	// This is the format of the inbound request from Grafana
	reqBuf := bytes.NewBufferString(`{"range": { "from": "2016-04-15T13:44:39.070Z", "to": "2016-04-15T14:44:39.070Z" }, "rangeRaw": { "from": "now-1h", "to": "now" },"annotation": {"name":"query","datasource":"yoursjsource","query":"some query","enable":true,"iconColor":"#1234"}}`)
	req := httptest.NewRequest(http.MethodGet, "/annotations", reqBuf)
	w := httptest.NewRecorder()

	gsj.ServeHTTP(w, req)
	res := w.Result()

	buf := &bytes.Buffer{}
	io.Copy(buf, res.Body)
	fmt.Println(buf.String())

	// Output:
	// [{"annotation":{"name":"query","datasource":"yoursjsource","query":"some query","enable":true,"iconColor":"#1234"},"time":1234000,"title":"First Title","text":"First annotation","tags":null},{"annotation":{"name":"query","datasource":"yoursjsource","query":"some query","enable":true,"iconColor":"#1234"},"time":1235000,"regionId":1,"title":"Second Title","text":"Second annotation with range","tags":["outage"]},{"annotation":{"name":"query","datasource":"yoursjsource","query":"some query","enable":true,"iconColor":"#1234"},"time":1237000,"regionId":1,"title":"Second Title","text":"Second annotation with range","tags":["outage"]}]
}
