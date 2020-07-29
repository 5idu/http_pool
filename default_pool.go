package httppool

import (
	"context"
	"runtime"
	"time"
)

var defaultPool = NewClientPool(&Options{
	PoolSize:           5 * runtime.NumCPU(),
	PoolTimeout:        5 * time.Second,
	IdleTimeout:        5 * time.Minute,
	IdleCheckFrequency: time.Minute,
})

func CloseClient(c *Client) error {
	return defaultPool.CloseClient(c)
}

func Get(ctx context.Context) (*Client, error) {
	return defaultPool.Get(ctx)
}

func Put(c *Client) {
	defaultPool.Put(c)
}

func Remove(c *Client, err error) {
	defaultPool.Remove(c, err)
}

func Len() int {
	return 	defaultPool.Len()
}

func IdleLen() int {
	return defaultPool.IdleLen()
}

func GetStats() *Stats  {
	return defaultPool.GetStats()
}

func Close() error {
	return defaultPool.Close()
}