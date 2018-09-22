package grafanasj_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/tcolgate/grafanasj"
)

// GSJExample demonstrates how to create a new Grafana Simple JSON compatible
// HTTP server.
type GSJExample struct{}

// GrafanaQuery handles timeseri type queries.
func (GSJExample) GrafanaQuery(ctx context.Context, from, to time.Time, interval time.Duration, maxDPs int, target string) ([]grafanasj.Data, error) {
	return []grafanasj.Data{
		{Time: time.Now().Add(-5 * time.Second), Value: 1234.0},
		{Time: time.Now(), Value: 1500.0},
	}, nil
}

func (GSJExample) GrafanaQueryTable(ctx context.Context, from, to time.Time, target string) ([]grafanasj.TableColumn, error) {
	return []grafanasj.TableColumn{
		{Text: "Time", Data: grafanasj.TimeColumn{time.Now()}},
		{Text: "SomeText", Data: grafanasj.StringColumn{"blah"}},
		{Text: "Value", Data: grafanasj.NumberColumn{1.0}},
	}, nil
}

func (GSJExample) GrafanaAnnotations(ctx context.Context, from, to time.Time, query string) ([]grafanasj.Annotation, error) {
	return []grafanasj.Annotation{
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

func Example() {
	gsj := grafanasj.New(GSJExample{})

	mux := http.NewServeMux()
	mux.HandleFunc("/query", gsj.HandleQuery)
	mux.HandleFunc("/annotations", gsj.HandleAnnotations)
	mux.HandleFunc("/search", gsj.HandleSearch)
	// This is just a convenience if your / doesn't return 200
	mux.HandleFunc("/", gsj.HandleRoot)

	// This is the format of the inbound request from grafana
	reqBuf := bytes.NewBufferString(`{"range": { "from": "2016-04-15T13:44:39.070Z", "to": "2016-04-15T14:44:39.070Z" }, "rangeRaw": { "from": "now-1h", "to": "now" },"annotation": {"name":"query","datasource":"yoursjsource","query":"some query","enable":true,"iconColor":"#1234"}}`)
	req := httptest.NewRequest(http.MethodGet, "/annotations", reqBuf)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)
	res := w.Result()

	buf := &bytes.Buffer{}
	io.Copy(buf, res.Body)
	fmt.Println(buf.String())

	// Output:
	// [{"annotation":{"name":"query","datasource":"yoursjsource","query":"some query","enable":true,"iconColor":"#1234"},"time":1234000,"title":"First Title","text":"First annotation","tags":null},{"annotation":{"name":"query","datasource":"yoursjsource","query":"some query","enable":true,"iconColor":"#1234"},"time":1235000,"regionId":1,"title":"Second Title","text":"Second annotation with range","tags":["outage"]},{"annotation":{"name":"query","datasource":"yoursjsource","query":"some query","enable":true,"iconColor":"#1234"},"time":1237000,"regionId":1,"title":"Second Title","text":"Second annotation with range","tags":["outage"]}]

}
