package httppool

import (
	"io"
	"io/ioutil"
	"net/http"
	"sync/atomic"
	"time"
)

type Client struct {
	client    *http.Client
	usedAt    int64 // atomic
	createdAt time.Time
	pooled    bool
}

func NewClient(client *http.Client) *Client {
	c := &Client{
		client:    client,
		createdAt: time.Now(),
	}

	c.SetUsedAt(time.Now())
	return c
}

func (c *Client) Close() error {
	c.client.CloseIdleConnections()
	return nil
}

func (c *Client) UsedAt() time.Time {
	unix := atomic.LoadInt64(&c.usedAt)
	return time.Unix(unix, 0)
}

func (c *Client) SetUsedAt(tm time.Time) {
	atomic.StoreInt64(&c.usedAt, tm.Unix())
}

func (c *Client) SetCreatedAt(tm time.Time) {
	c.createdAt = tm
}

func (c *Client) Request(url, method string, header http.Header, body io.Reader) ([]byte, error) {
	request, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	if header == nil {
		header = http.Header{}
		header.Set("Content-Type", "application/json")
	}
	request.Header = header

	resp, err := c.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	return data, err
}
