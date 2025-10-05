package lokiclient

import (
	"encoding/json"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var gClient Client

type PushTsReformat func(s string, m map[string]string) (string, error)

type ClientConfig struct {
	// E.g. http://localhost:3100/api/prom/push
	PushURL string
	// E.g. "{\"job\"=\"somejob\"}"
	Labels               map[string]string
	BatchWait            time.Duration
	BatchEntriesNumber   int
	pushOriginTs         bool
	pushOriginTsReformat PushTsReformat
}

type jsonLogEntry []interface{} // #0:unix_nano, #1:logLine, #2:metadata map[string]string

type promtailStream struct {
	Labels  map[string]string `json:"stream"`
	Entries []*jsonLogEntry   `json:"values"`
}

type promtailMsg struct {
	Streams []promtailStream `json:"streams"`
}

type clientJson struct {
	config    *ClientConfig
	quit      chan struct{}
	entries   chan *jsonLogEntry
	waitGroup sync.WaitGroup
	client    httpClient
}

func (c *clientJson) Logf(logLine string, meta map[string]string) {
	var ts string
	var err error

	if c.config.pushOriginTs {
		ts, err = c.config.pushOriginTsReformat(logLine, meta)
		if err != nil {
			log.Error(err)
			return
		}
	} else {
		ts = strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	if meta != nil {
		c.entries <- &jsonLogEntry{
			ts,
			logLine,
			meta,
		}
	} else {
		c.entries <- &jsonLogEntry{
			ts,
			logLine,
		}
	}
}

func (c *clientJson) Shutdown() {
	close(c.quit)
	c.waitGroup.Wait()
}

func (c *clientJson) run() {
	var batch []*jsonLogEntry
	batchSize := 0
	maxWait := time.NewTimer(c.config.BatchWait)

	defer func() {
		if batchSize > 0 {
			c.send(batch)
		}
		c.waitGroup.Done()
	}()

	for {
		select {
		case <-c.quit:
			return
		case entry := <-c.entries:
			batch = append(batch, entry)
			batchSize++
			if batchSize >= c.config.BatchEntriesNumber {
				c.send(batch)
				batch = []*jsonLogEntry{}
				batchSize = 0
				maxWait.Reset(c.config.BatchWait)
			}
		case <-maxWait.C:
			if batchSize > 0 {
				c.send(batch)
				batch = []*jsonLogEntry{}
				batchSize = 0
			}
			maxWait.Reset(c.config.BatchWait)
		}
	}
}

func (c *clientJson) send(entries []*jsonLogEntry) {
	var streams []promtailStream
	streams = append(streams, promtailStream{
		Labels:  c.config.Labels,
		Entries: entries,
	})

	msg := promtailMsg{Streams: streams}
	jsonMsg, err := json.Marshal(msg)

	log.Debug("posting HTTP: ", jsonMsg)
	if err != nil {
		log.Error("unable to marshal a JSON document, reason: ", err)
		return
	}

	resp, body, err := c.client.sendJsonReq("POST", c.config.PushURL, "application/json", jsonMsg)
	if err != nil {
		log.Error("unable to send a HTTP request, reason: ", err)
		return
	}

	if resp.StatusCode != 204 {
		log.Error("got unexpected HTTP status code: ", resp.StatusCode, ", message: ", string(body))
		return
	}
}

func NewClientJson(conf ClientConfig) (Client, error) {
	client := clientJson{
		config:  &conf,
		quit:    make(chan struct{}),
		entries: make(chan *jsonLogEntry, LogEntriesChanSize),
		client:  httpClient{},
	}

	client.waitGroup.Add(1)
	go client.run()

	return &client, nil
}

func BootStrap(fmt PushTsReformat) error {
	conf := viper.GetStringMap("loki")
	if conf == nil || len(conf) == 0 {
		panic("unconfigured loki")
	}
	var pushUrl_, batchWait_ string
	var batchWait time.Duration
	var batchSize int
	if pushUrl_ = viper.GetString("loki.push_url"); pushUrl_ == "" {
		panic("unconfigured loki.push_url")
	}
	labels_ := viper.GetStringMapString("loki.labels")
	if batchWait_ = viper.GetString("loki.batch_wait"); batchWait_ == "" {
		panic("unconfigured loki.batch_wait")
	}
	batchWait, err := time.ParseDuration(batchWait_)
	if err != nil {
		panic(err)
	}
	if batchSize = viper.GetInt("loki.batch_size"); batchSize == 0 {
		panic("unconfigured loki.batch_size")
	}
	pushTs := viper.GetBool("loki.push_origin_ts")
	if pushTs && fmt == nil {
		panic("need bind PushTsReformat when push_origin_ts is true")
	}

	gClient, err = NewClientJson(ClientConfig{
		PushURL:              pushUrl_,
		Labels:               labels_,
		BatchWait:            batchWait,
		BatchEntriesNumber:   batchSize,
		pushOriginTs:         pushTs,
		pushOriginTsReformat: fmt,
	})

	if err != nil {
		return err
	}

	return nil
}

func GetClient() Client {
	return gClient
}
