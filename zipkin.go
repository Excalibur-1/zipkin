package zipkin

import (
	"fmt"
	"time"

	"github.com/Excalibur-1/configuration"
	"github.com/Excalibur-1/gutil"
	"github.com/Excalibur-1/trace"
	"github.com/Excalibur-1/trace/proto"
	"github.com/openzipkin/zipkin-go/model"
	"github.com/openzipkin/zipkin-go/reporter"
	"github.com/openzipkin/zipkin-go/reporter/http"
)

func Init(systemId string, conf configuration.Configuration, tag []trace.Tag) {
	fmt.Println("Loading FoChange Zipkin Engine ver:0.9.9")
	var c Config
	if err := conf.Clazz("myconf", "base", "", "", "zipkin", &c); err == nil {
		if c.BatchSize == 0 {
			c.BatchSize = 100
		}
		if c.Timeout == 0 {
			c.Timeout = gutil.Duration(200 * time.Millisecond)
		}
		trace.SetGlobalTracer(trace.NewTracer(systemId, tag, newReport(&c), c.DisableSample))
	} else {
		panic("加载Zipkin参数出错!")
	}
}

// Config 配置。
// url应该是将跨度发送到的端点,例如
type Config struct {
	Endpoint      string         `json:"endpoint"`
	BatchSize     int            `json:"batchSize"`
	Timeout       gutil.Duration `json:"timeout"`
	DisableSample bool           `json:"disableSample"`
}

type report struct {
	rpt reporter.Reporter
}

func newReport(c *Config) *report {
	return &report{
		rpt: http.NewReporter(c.Endpoint,
			http.Timeout(time.Duration(c.Timeout)),
			http.BatchSize(c.BatchSize),
		),
	}
}

// WriteSpan write a trace span to queue.
func (r *report) WriteSpan(raw *trace.Span) (err error) {
	ctx := raw.Context()
	traceID := model.TraceID{Low: ctx.TraceId}
	spanID := model.ID(ctx.SpanId)
	parentID := model.ID(ctx.ParentId)
	tags := raw.Tags()
	span := model.SpanModel{
		SpanContext: model.SpanContext{
			TraceID:  traceID,
			ID:       spanID,
			ParentID: &parentID,
		},
		Name:      raw.OperationName(),
		Timestamp: raw.StartTime(),
		Duration:  raw.Duration(),
		Tags:      make(map[string]string, len(tags)),
	}
	span.LocalEndpoint = &model.Endpoint{ServiceName: raw.ServiceName()}
	for _, tag := range tags {
		switch tag.Key {
		case trace.TagSpanKind:
			switch tag.Value.(string) {
			case "client":
				span.Kind = model.Client
			case "server":
				span.Kind = model.Server
			case "producer":
				span.Kind = model.Producer
			case "consumer":
				span.Kind = model.Consumer
			}
		default:
			v, ok := tag.Value.(string)
			if ok {
				span.Tags[tag.Key] = v
			} else {
				span.Tags[tag.Key] = fmt.Sprint(v)
			}
		}
	}
	// log save to zipkin annotation
	span.Annotations = r.convertLogsToAnnotations(raw.Logs())
	r.rpt.Send(span)
	return
}

// Close close the report.
func (r *report) Close() error {
	return r.rpt.Close()
}

func (r *report) convertLogsToAnnotations(logs []*proto.Log) (annotations []model.Annotation) {
	annotations = make([]model.Annotation, 0, len(annotations))
	for _, lg := range logs {
		annotations = append(annotations, r.convertLogToAnnotation(lg)...)
	}
	return annotations
}

func (r *report) convertLogToAnnotation(log *proto.Log) (annotations []model.Annotation) {
	annotations = make([]model.Annotation, 0, len(log.Fields))
	for _, field := range log.Fields {
		val := string(field.Value)
		annotation := model.Annotation{
			Timestamp: time.Unix(0, log.Timestamp),
			Value:     field.Key + " : " + val,
		}
		annotations = append(annotations, annotation)
	}
	return annotations
}
