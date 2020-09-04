package sinks

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/eapache/channels"
	"github.com/elastic/go-elasticsearch/v6"
	"github.com/go-co-op/gocron"
	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
)

const indexPrefix = "eventrouter-" // Could have this configurable

// ElasticSink sends events directly to ElasticSearch
// Useful when logging K8s events to an external ELK
type ElasticSink struct {
	client  *elasticsearch.Client
	tag     string
	index   string
	eventCh channels.Channel
}

// NewElasticSink will create a new ElasticSink with default options, returned as
// an EventSinkInterface
func NewElasticSink(url string, user string, pass string, tag string, bufSize int, discard bool) *ElasticSink {
	var (
		r       map[string]interface{}
		eventCh channels.Channel
	)
	cfg := elasticsearch.Config{
		Addresses: []string{
			url,
		},
		Username: user,
		Password: pass,
	}
	glog.Infof("Connecting to %s using tag %s", url, tag)
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		glog.Fatalf("Error creating the ES client: %v", err)
	}

	res, err := es.Info()
	if err != nil {
		glog.Fatalf("Error getting response: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		glog.Fatalf("Error: %s", res.String())
	}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		glog.Fatalf("Error parsing the response body: %v", err)
	}

	glog.Infof("ElasticSearch client: v%s", elasticsearch.Version)
	glog.Infof("ElasticSearch server: v%s", r["version"].(map[string]interface{})["number"])

	if discard {
		eventCh = channels.NewOverflowingChannel(channels.BufferCap(bufSize))
	} else {
		eventCh = channels.NewNativeChannel(channels.BufferCap(bufSize))
	}

	elasticSink := &ElasticSink{
		client:  es,
		eventCh: eventCh,
		index:   todayIndex(),
		tag:     tag,
	}

	s := gocron.NewScheduler(time.UTC)
	s.Every(1).Day().At("00:00:00").Do(func() { elasticSink.index = todayIndex() })
	s.StartAsync()

	return elasticSink
}

func todayIndex() string {
	return indexPrefix + time.Now().Format("2006.01.02")
}

// UpdateEvents implements the EventSinkInterface
func (s *ElasticSink) UpdateEvents(eNew *v1.Event, eOld *v1.Event) {
	s.eventCh.In() <- NewTaggedEventData(eNew, eOld, s.tag)
}

func (s *ElasticSink) send(evt EventData) {
	var (
		jsonBytes []byte
		err       error
	)
	if jsonBytes, err = json.Marshal(evt); err != nil {
		glog.Errorf("Failed to json serialize event: %v", err)
		return
	}

	res, err := s.client.Index(
		s.index,                            // Index name
		bytes.NewReader(jsonBytes),         // Document body
		s.client.Index.WithRefresh("true"), // Refresh
	)
	if err != nil {
		glog.Errorf("Failed sending to ElasticSearch: %v", err)
	}
	defer res.Body.Close()
}

// Run sits in a loop, waiting for data to come in through s.eventCh,
// and forwarding them to the sink.
func (s *ElasticSink) Run(stopCh <-chan bool) {

loop:
	for {
		select {
		case e := <-s.eventCh.Out():

			var evt EventData
			var ok bool
			if evt, ok = e.(EventData); !ok {
				glog.Warningf("Invalid type sent through event channel: %T", e)
				continue loop
			}

			// Start with just this event...
			s.send(evt)
			// Send all buffered events in case more have been written
			numEvents := s.eventCh.Len()
			for i := 0; i < numEvents; i++ {
				e := <-s.eventCh.Out()
				if evt, ok = e.(EventData); ok {
					s.send(evt)
				} else {
					glog.Warningf("Invalid type sent through event channel: %T", e)
				}
			}
		case <-stopCh:
			break loop
		}
	}
}
