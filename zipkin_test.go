package zipkin

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Excalibur-1/gutil"
	"github.com/Excalibur-1/trace"
)

func TestZipkin(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected 'POST' request, got '%s'", r.Method)
		}

		aSpanPayload, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Errorf("unexpected error: %s", err.Error())
		}

		t.Logf("%s\n", aSpanPayload)
	}))
	defer ts.Close()

	c := &Config{
		Endpoint:  ts.URL,
		Timeout:   gutil.Duration(time.Second * 5),
		BatchSize: 100,
	}
	// c.Endpoint = "http://127.0.0.1:9411/api/v2/spans"
	report := newReport(c)
	t1 := trace.NewTracer("service1", []trace.Tag{}, report, true)
	t2 := trace.NewTracer("service2", []trace.Tag{}, report, true)
	sp1 := t1.New("option_1")
	sp2 := sp1.Fork("service3", "opt_client")
	sp2.SetLog(trace.Log("log_k", "log_v"))
	// inject
	header := make(http.Header)
	_ = t1.Inject(sp2, trace.HTTPFormat, header)
	t.Log(header)
	sp3, err := t2.Extract(trace.HTTPFormat, header)
	if err != nil {
		t.Fatal(err)
	}
	sp3.Finish(nil)
	sp2.Finish(nil)
	sp1.Finish(nil)
	_ = report.Close()
}
