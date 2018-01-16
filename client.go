package http

import (
	"errors"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/outil"
	"github.com/elastic/beats/libbeat/outputs/transport"
	"github.com/elastic/beats/libbeat/publisher"
	"github.com/elastic/beats/libbeat/testing"
)

// Client is an elasticsearch client.
type Client struct {
	Connection
	tlsConfig *transport.TLSConfig

	index    outil.Selector
	pipeline *outil.Selector
	params   map[string]string
	timeout  time.Duration

	// buffered bulk requests
	bulkRequ *bulkRequest

	// buffered json response reader
	json jsonReader

	// additional configs
	compressionLevel int
	proxyURL         *url.URL

	observer outputs.Observer
}

// ClientSettings contains the settings for a client.
type ClientSettings struct {
	URL                string
	Proxy              *url.URL
	TLS                *transport.TLSConfig
	Username, Password string
	Parameters         map[string]string
	Headers            map[string]string
	Index              outil.Selector
	Pipeline           *outil.Selector
	Timeout            time.Duration
	CompressionLevel   int
	Observer           outputs.Observer
}

type connectCallback func(client *Client) error

// Connection manages the connection for a given client.
type Connection struct {
	URL      string
	Username string
	Password string
	Headers  map[string]string

	http              *http.Client
	onConnectCallback func() error

	encoder   bodyEncoder
	version   string
	connected bool
}

var (
	ackedEvents            = expvar.NewInt("libbeatHttpPublishedAndAckedEvents")
	eventsNotAcked         = expvar.NewInt("libbeatHttpPublishedButNotAckedEvents")
	publishEventsCallCount = expvar.NewInt("libbeatHttpPublishEventsCallCount")
)

// NewClient instantiates a new client.
func NewClient(s ClientSettings, onConnect *callbacksRegistry) (*Client, error) {
	proxy := http.ProxyFromEnvironment
	if s.Proxy != nil {
		proxy = http.ProxyURL(s.Proxy)
	}

	pipeline := s.Pipeline
	if pipeline != nil && pipeline.IsEmpty() {
		pipeline = nil
	}

	u, err := url.Parse(s.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse elasticsearch URL: %v", err)
	}
	if u.User != nil {
		s.Username = u.User.Username()
		s.Password, _ = u.User.Password()
		u.User = nil

		// Re-write URL without credentials.
		s.URL = u.String()
	}

	logp.Info("Elasticsearch url: %s", s.URL)

	// TODO: add socks5 proxy support
	var dialer, tlsDialer transport.Dialer

	dialer = transport.NetDialer(s.Timeout)
	tlsDialer, err = transport.TLSDialer(dialer, s.TLS, s.Timeout)
	if err != nil {
		return nil, err
	}

	if st := s.Observer; st != nil {
		dialer = transport.StatsDialer(dialer, st)
		tlsDialer = transport.StatsDialer(tlsDialer, st)
	}

	params := s.Parameters
	bulkRequ, err := newBulkRequest(s.URL, "", "", params, nil)
	if err != nil {
		return nil, err
	}

	var encoder bodyEncoder
	compression := s.CompressionLevel
	if compression == 0 {
		encoder = newJSONEncoder(nil)
	} else {
		encoder, err = newGzipEncoder(compression, nil)
		if err != nil {
			return nil, err
		}
	}

	client := &Client{
		Connection: Connection{
			URL:      s.URL,
			Username: s.Username,
			Password: s.Password,
			Headers:  s.Headers,
			http: &http.Client{
				Transport: &http.Transport{
					Dial:    dialer.Dial,
					DialTLS: tlsDialer.Dial,
					Proxy:   proxy,
				},
				Timeout: s.Timeout,
			},
			encoder: encoder,
		},
		tlsConfig: s.TLS,
		index:     s.Index,
		pipeline:  pipeline,
		params:    params,
		timeout:   s.Timeout,

		bulkRequ: bulkRequ,

		compressionLevel: compression,
		proxyURL:         s.Proxy,
	}

	client.Connection.onConnectCallback = func() error {
		if onConnect != nil {
			onConnect.mutex.Lock()
			defer onConnect.mutex.Unlock()

			for _, callback := range onConnect.callbacks {
				err := callback(client)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}

	return client, nil
}

// Clone clones a client.
func (client *Client) Clone() *Client {
	// when cloning the connection callback and params are not copied. A
	// client's close is for example generated for topology-map support. With params
	// most likely containing the ingest node pipeline and default callback trying to
	// create install a template, we don't want these to be included in the clone.

	c, _ := NewClient(
		ClientSettings{
			URL:              client.URL,
			Index:            client.index,
			Pipeline:         client.pipeline,
			Proxy:            client.proxyURL,
			TLS:              client.tlsConfig,
			Username:         client.Username,
			Password:         client.Password,
			Parameters:       nil, // XXX: do not pass params?
			Headers:          client.Headers,
			Timeout:          client.http.Timeout,
			CompressionLevel: client.compressionLevel,
		},
		nil, // XXX: do not pass connection callback?
	)
	return c
}

func (client *Client) Publish(batch publisher.Batch) error {
	client.publishEvents(batch.Events())
	//rest, err := client.publishEvents(events)
	//if len(rest) == 0 {
	//	batch.ACK()
	//} else {
	//	batch.RetryEvents(rest)
	//}
	//return err
	return nil
}

// PublishEvents posts all events to the http endpoint. On error a slice with all
// events not published will be returned.
func (client *Client) publishEvents(
	data []publisher.Event,
) ([]publisher.Event, error) {
	begin := time.Now()
	publishEventsCallCount.Add(1)

	if len(data) == 0 {
		return nil, nil
	}

	if !client.connected {
		return data, ErrNotConnected
	}

	var failedEvents []publisher.Event

	sendErr := error(nil)
	for _, event := range data {
		sendErr = client.PublishEvent(event)
		// TODO more gracefully handle failures return the failed events
		// below instead of bailing out directly here:
		if sendErr != nil {
			return nil, sendErr
		}
	}

	debugf("PublishEvents: %d metrics have been published over HTTP in %v.",
		len(data),
		time.Now().Sub(begin))

	ackedEvents.Add(int64(len(data) - len(failedEvents)))
	eventsNotAcked.Add(int64(len(failedEvents)))
	if len(failedEvents) > 0 {
		return failedEvents, sendErr
	}

	return nil, nil
}

func (client *Client) PublishEvent(data publisher.Event) error {
	if !client.connected {
		return ErrNotConnected
	}

	event := data

	debugf("Publish event: %s", event)

	status, _, err := client.request("POST", "", client.params, event)
	if err != nil {
		logp.Warn("Fail to insert a single event: %s", err)
		if err == ErrJSONEncodeFailed {
			// don't retry unencodable values
			return nil
		}
	}
	switch {
	case status == 0: // event was not send yet
		return nil
	case status >= 500 || status == 429: // server error, retry
		return err
	case status >= 300 && status < 500:
		// other error => don't retry
		return nil
	}

	return nil
}

func getPipeline(event *beat.Event, pipelineSel *outil.Selector) (string, error) {
	if event.Meta != nil {
		if pipeline, exists := event.Meta["pipeline"]; exists {
			if p, ok := pipeline.(string); ok {
				return p, nil
			}
			return "", errors.New("pipeline metadata is no string")
		}
	}

	if pipelineSel != nil {
		return pipelineSel.Select(event)
	}
	return "", nil
}

// getIndex returns the full index name
// Index is either defined in the config as part of the output
// or can be overload by the event through setting index
func getIndex(event *beat.Event, index outil.Selector) (string, error) {
	if event.Meta != nil {
		if str, exists := event.Meta["index"]; exists {
			idx, ok := str.(string)
			if ok {
				ts := event.Timestamp.UTC()
				return fmt.Sprintf("%s-%d.%02d.%02d",
					idx, ts.Year(), ts.Month(), ts.Day()), nil
			}
		}
	}

	return index.Select(event)
}

func (client *Client) Test(d testing.Driver) {
	d.Run("elasticsearch: "+client.URL, func(d testing.Driver) {
		u, err := url.Parse(client.URL)
		d.Fatal("parse url", err)

		address := u.Hostname()
		if u.Port() != "" {
			address += ":" + u.Port()
		}
		d.Run("connection", func(d testing.Driver) {
			netDialer := transport.TestNetDialer(d, client.timeout)
			_, err = netDialer.Dial("tcp", address)
			d.Fatal("dial up", err)
		})

		if u.Scheme != "https" {
			d.Warn("TLS", "secure connection disabled")
		} else {
			d.Run("TLS", func(d testing.Driver) {
				netDialer := transport.NetDialer(client.timeout)
				tlsDialer, err := transport.TestTLSDialer(d, netDialer, client.tlsConfig, client.timeout)
				_, err = tlsDialer.Dial("tcp", address)
				d.Fatal("dial up", err)
			})
		}

		err = client.Connect()
		d.Fatal("talk to server", err)
		d.Info("version", client.version)
	})
}

// Connect connects the client.
func (conn *Connection) Connect() error {
	var err error
	conn.connected, err = conn.Ping()
	if err != nil {
		return err
	}

	err = conn.onConnectCallback()
	if err != nil {
		return fmt.Errorf("Connection marked as failed because the onConnect callback failed: %v", err)
	}
	return nil
}

// Ping sends a GET request to the endpoint.
func (conn *Connection) Ping() (bool, error) {
	debugf("HTTP Ping(url=%v)", conn.URL)

	status, _, err := conn.execRequest("GET", conn.URL, nil)
	if err != nil {
		debugf("Ping request failed with: %v", err)
		return false, err
	}

	if status >= 300 {
		return false, fmt.Errorf("Non 2xx response code: %d", status)
	}

	if status != 200 {
		return false, fmt.Errorf("Could not connect to %s, got response code: %d", conn.URL, status)
	}

	return true, nil
}

// Close closes a connection.
func (conn *Connection) Close() error {
	return nil
}

// RequestURL sends a request with the connection object to an alternative url
func (conn *Connection) RequestURL(
	method, url string,
	body interface{},
) (int, []byte, error) {

	if body == nil {
		return conn.execRequest(method, url, nil)
	}

	if err := conn.encoder.Marshal(body); err != nil {
		logp.Warn("Failed to json encode body (%v): %#v", err, body)
		return 0, nil, ErrJSONEncodeFailed
	}
	return conn.execRequest(method, url, conn.encoder.Reader())
}

func (conn *Connection) request(
	method, path string,
	params map[string]string,
	body interface{},
) (int, []byte, error) {
	url := makeURL(conn.URL, path, "", params)
	debugf("%s %s %v", method, url, body)

	if body == nil {
		return conn.execRequest(method, url, nil)
	}

	if err := conn.encoder.Marshal(body); err != nil {
		logp.Warn("Failed to json encode body (%v): %#v", err, body)
		return 0, nil, ErrJSONEncodeFailed
	}
	return conn.execRequest(method, url, conn.encoder.Reader())
}

// Request sends a request via the connection.
func (conn *Connection) Request(
	method, path string,
	pipeline string,
	params map[string]string,
	body interface{},
) (int, []byte, error) {

	url := addToURL(conn.URL, path, pipeline, params)
	debugf("%s %s %s %v", method, url, pipeline, body)

	return conn.RequestURL(method, url, body)
}

func (conn *Connection) execRequest(
	method, url string,
	body io.Reader,
) (int, []byte, error) {
	req, err := http.NewRequest(method, url, body)
	req.Close = true
	if err != nil {
		logp.Warn("Failed to create request", err)
		return 0, nil, err
	}
	if body != nil {
		conn.encoder.AddHeader(&req.Header)
	}
	return conn.execHTTPRequest(req)
}

func (conn *Connection) execHTTPRequest(req *http.Request) (int, []byte, error) {
	req.Header.Add("Accept", "application/json")
	if conn.Username != "" || conn.Password != "" {
		req.SetBasicAuth(conn.Username, conn.Password)
	}

	for name, value := range conn.Headers {
		req.Header.Add(name, value)
	}

	// The stlib will override the value in the header based on the configured `Host`
	// on the request which default to the current machine.
	//
	// We use the normalized key header to retrieve the user configured value and assign it to the host.
	if host := req.Header.Get("Host"); host != "" {
		req.Host = host
	}

	resp, err := conn.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer closing(resp.Body)

	status := resp.StatusCode
	obj, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return status, nil, err
	}

	if status >= 300 {
		// add the response body with the error returned by Elasticsearch
		err = fmt.Errorf("%v: %s", resp.Status, obj)
	}

	return status, obj, err
}

func closing(c io.Closer) {
	err := c.Close()
	if err != nil {
		logp.Warn("Close failed with: %v", err)
	}
}
