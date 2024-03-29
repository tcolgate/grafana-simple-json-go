package simplejson_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	simplejson "github.com/tcolgate/grafana-simple-json-go"
)

func TestWithQuerier(t *testing.T) {
	gsj := simplejson.New(
		simplejson.WithQuerier(GSJExample{}),
	)

	q := `{
				"panelId": 1,
				"range": {
					"from": "2016-10-31T06:33:44.866Z",
					"to": "2016-10-31T12:33:44.866Z",
					"raw": { "from": "now-6h", "to": "now"}
				},
				"rangeRaw": { "from": "now-6h", "to": "now" },
				"interval": "30s",
				"intervalMs": 30000,
				"targets": [
					{ "target": "upper_50", "refId": "A" , "hide": false, "type": "timeserie"},
					{ "target": "upper_75", "refId": "B" }
				],
				"format": "json",
				"maxDataPoints": 550
			}`
	// This is the format of the inbound request from Grafana
	reqBuf := bytes.NewBufferString(q)
	req := httptest.NewRequest(http.MethodGet, "/query", reqBuf)
	w := httptest.NewRecorder()

	gsj.ServeHTTP(w, req)
	res := w.Result()

	buf := &bytes.Buffer{}
	io.Copy(buf, res.Body)
	expect := `[{"target":"upper_50","datapoints":[[1234,1477917219866],[1500,1477917224866]]},{"target":"upper_75","datapoints":[[1234,1477917219866],[1500,1477917224866]]}]`

	if buf.String() != expect {
		t.Fatalf("\nexpected: %q\ngot:%s", expect, buf.String())
	}
}

func TestWithTableQuerier(t *testing.T) {
	gsj := simplejson.New(
		simplejson.WithTableQuerier(GSJExample{}),
	)

	// This is the format of the inbound request from Grafana
	q := `{
				"panelId": 1,
				"range": {
					"from": "2016-10-31T06:33:44.866Z",
					"to": "2016-10-31T12:33:44.866Z",
					"raw": { "from": "now-6h", "to": "now"}
				},
				"rangeRaw": { "from": "now-6h", "to": "now" },
				"interval": "30s",
				"intervalMs": 30000,
				"targets": [
					{ "target": "upper_50", "refId": "A" , "hide": false, "type": "table"}
				],
				"format": "json",
				"maxDataPoints": 550
			}`
	reqBuf := bytes.NewBufferString(q)
	req := httptest.NewRequest(http.MethodGet, "/query", reqBuf)
	w := httptest.NewRecorder()

	gsj.ServeHTTP(w, req)
	res := w.Result()

	buf := &bytes.Buffer{}
	io.Copy(buf, res.Body)
	expect := `[{"type":"table","columns":[{"text":"Time","type":"time"},{"text":"SomeText","type":"string"},{"text":"Value","type":"number"}],"rows":[["2016-10-31T12:33:44.866Z","blah",1]]}]`

	if buf.String() != expect {
		t.Fatalf("\nexpected: %q\ngot:%s", expect, buf.String())
	}
}

func TestWithAnnotator(t *testing.T) {
	gsj := simplejson.New(
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
	expect := `[{"annotation":{"name":"query","query":"some query","enable":true,"iconColor":"#1234"},"time":1234000,"title":"First Title","text":"First annotation","tags":null},{"annotation":{"name":"query","query":"some query","enable":true,"iconColor":"#1234"},"time":1235000,"regionId":1,"title":"Second Title","text":"Second annotation with range","tags":["outage"]},{"annotation":{"name":"query","query":"some query","enable":true,"iconColor":"#1234"},"time":1237000,"regionId":1,"title":"Second Title","text":"Second annotation with range","tags":["outage"]}]`
	if buf.String() != expect {
		t.Fatalf("\nexpected: %q\ngot:%s", expect, buf.String())
	}
}

func TestWithSearcher(t *testing.T) {
	gsj := simplejson.New(
		simplejson.WithSearcher(GSJExample{}),
	)

	// This is the format of the inbound request from Grafana
	reqBuf := bytes.NewBufferString(`{"target": "upper_50"}`)

	req := httptest.NewRequest(http.MethodGet, "/search", reqBuf)
	w := httptest.NewRecorder()

	gsj.ServeHTTP(w, req)
	res := w.Result()

	buf := &bytes.Buffer{}
	io.Copy(buf, res.Body)
	expect := `["example1","example2","example3"]`

	if buf.String() != expect {
		t.Fatalf("\nexpected: %q\ngot:%s", expect, buf.String())
	}
}

func TestWithTagSearcher_Keys(t *testing.T) {
	gsj := simplejson.New(
		simplejson.WithTagSearcher(GSJExample{}),
	)

	// This is the format of the inbound request from Grafana
	reqBuf := bytes.NewBufferString(`{}`)
	req := httptest.NewRequest(http.MethodGet, "/tag-keys", reqBuf)
	w := httptest.NewRecorder()

	gsj.ServeHTTP(w, req)
	res := w.Result()

	buf := &bytes.Buffer{}
	io.Copy(buf, res.Body)
	expect := `[{"type":"string","text":"mykey"}]`
	if buf.String() != expect {
		t.Fatalf("\nexpected: %q\ngot:%s", expect, buf.String())
	}
}

func TestWithTagSearcher_Values(t *testing.T) {
	gsj := simplejson.New(
		simplejson.WithTagSearcher(GSJExample{}),
	)

	// This is the format of the inbound request from Grafana
	reqBuf := bytes.NewBufferString(`{"key": "mykey"}`)
	req := httptest.NewRequest(http.MethodGet, "/tag-values", reqBuf)
	w := httptest.NewRecorder()

	gsj.ServeHTTP(w, req)
	res := w.Result()

	buf := &bytes.Buffer{}
	io.Copy(buf, res.Body)
	expect := `[{"text":"value1"},{"text":"value2"}]`
	if buf.String() != expect {
		t.Fatalf("\nexpected: %q\ngot:%s", expect, buf.String())
	}
}
