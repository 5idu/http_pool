package httppool

import (
	"context"
	"errors"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Pooler interface {
	NewClient(context.Context) (*Client, error)
	CloseClient(*Client) error

	Get(context.Context) (*Client, error)
	Put(*Client)
	Remove(*Client, error)

	Len() int
	IdleLen() int
	Stats() *Stats

	Close() error
}

var ErrClosed = errors.New("redis: client is closed")
var ErrPoolTimeout = errors.New("redis: connection pool timeout")

var timers = sync.Pool{
	New: func() interface{} {
		t := time.NewTimer(time.Hour)
		t.Stop()
		return t
	},
}

// Stats contains pool state information and accumulated stats.
type Stats struct {
	Hits     uint32 // number of times free client was found in the pool
	Misses   uint32 // number of times free client was NOT found in the pool
	Timeouts uint32 // number of times a wait timeout occurred

	TotalClients uint32 // number of total client in the pool
	IdleClients  uint32 // number of idle client in the pool
	StaleClients uint32 // number of stale client removed from the pool
}

type Options struct {
	Dialer  func(context.Context) (*http.Client, error)
	OnClose func(*Client) error

	PoolSize           int
	MinIdleClients     int
	MaxClientAge       time.Duration
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration
}

func (opt *Options) init() {
	if opt.Dialer == nil {
		opt.Dialer = func(ctx context.Context) (*http.Client, error) {
			return http.DefaultClient, nil
		}
	}
}

type ClientPool struct {
	opt *Options

	dialErrorsNum uint32 // atomic

	lastDialErrorMu sync.RWMutex
	lastDialError   error

	queue chan struct{}

	clientsMu      sync.Mutex
	clients        []*Client
	idleClients    []*Client
	poolSize       int
	idleClientsLen int

	stats Stats

	_closed  uint32 // atomic
	closedCh chan struct{}
}

var _ Pooler = (*ClientPool)(nil)

var DefaultPool Pooler

func NewClientPool(opt *Options) *ClientPool {
	opt.init()

	p := &ClientPool{
		opt: opt,

		queue:       make(chan struct{}, opt.PoolSize),
		clients:     make([]*Client, 0, opt.PoolSize),
		idleClients: make([]*Client, 0, opt.PoolSize),
		closedCh:    make(chan struct{}),
	}

	p.clientsMu.Lock()
	p.checkMinIdleClients()
	p.clientsMu.Unlock()

	if opt.IdleTimeout > 0 && opt.IdleCheckFrequency > 0 {
		go p.reaper(opt.IdleCheckFrequency)
	}

	return p
}

func (p *ClientPool) reaper(frequency time.Duration) {
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// It is possible that ticker and closedCh arrive together,
			// and select pseudo-randomly pick ticker case, we double
			// check here to prevent being executed after closed.
			if p.closed() {
				return
			}
			_, err := p.ReapStaleClients()
			if err != nil {
				log.Printf("ReapStaleClients failed: %s\n", err)
				continue
			}
		case <-p.closedCh:
			return
		}
	}
}

func (p *ClientPool) ReapStaleClients() (int, error) {
	var n int
	for {
		p.getTurn()

		p.clientsMu.Lock()
		cn := p.reapStaleClient()
		p.clientsMu.Unlock()
		p.freeTurn()

		if cn != nil {
			_ = p.closeClient(cn)
			n++
		} else {
			break
		}
	}
	atomic.AddUint32(&p.stats.StaleClients, uint32(n))
	return n, nil
}

func (p *ClientPool) reapStaleClient() *Client {
	if len(p.idleClients) == 0 {
		return nil
	}

	cn := p.idleClients[0]
	if !p.isStaleClient(cn) {
		return nil
	}

	p.idleClients = append(p.idleClients[:0], p.idleClients[1:]...)
	p.idleClientsLen--
	p.removeClient(cn)

	return cn
}

func (p *ClientPool) NewClient(ctx context.Context) (*Client, error) {
	return p.newClient(ctx, false)
}

func (p *ClientPool) newClient(ctx context.Context, pooled bool) (*Client, error) {
	cn, err := p.dialClient(ctx, pooled)
	if err != nil {
		return nil, err
	}

	p.clientsMu.Lock()
	p.clients = append(p.clients, cn)
	if pooled {
		// If pool is full remove the cn on next Put.
		if p.poolSize >= p.opt.PoolSize {
			cn.pooled = false
		} else {
			p.poolSize++
		}
	}
	p.clientsMu.Unlock()
	return cn, nil
}

func (p *ClientPool) dialClient(ctx context.Context, pooled bool) (*Client, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	if atomic.LoadUint32(&p.dialErrorsNum) >= uint32(p.opt.PoolSize) {
		return nil, p.getLastDialError()
	}

	c, err := p.opt.Dialer(ctx)
	if err != nil {
		p.setLastDialError(err)
		if atomic.AddUint32(&p.dialErrorsNum, 1) == uint32(p.opt.PoolSize) {
			go p.tryDial()
		}
		return nil, err
	}

	cn := NewClient(c)
	cn.pooled = pooled
	return cn, nil
}

func (p *ClientPool) closed() bool {
	return atomic.LoadUint32(&p._closed) == 1
}

func (p *ClientPool) setLastDialError(err error) {
	p.lastDialErrorMu.Lock()
	p.lastDialError = err
	p.lastDialErrorMu.Unlock()
}

func (p *ClientPool) getLastDialError() error {
	p.lastDialErrorMu.RLock()
	err := p.lastDialError
	p.lastDialErrorMu.RUnlock()
	return err
}

func (p *ClientPool) tryDial() {
	for {
		if p.closed() {
			return
		}

		c, err := p.opt.Dialer(context.Background())
		if err != nil {
			p.setLastDialError(err)
			time.Sleep(time.Second)
			continue
		}

		atomic.StoreUint32(&p.dialErrorsNum, 0)
		c.CloseIdleConnections()
		return
	}
}

func (p *ClientPool) CloseClient(client *Client) error {
	p.removeClientWithLock(client)
	return p.closeClient(client)
}

func (p *ClientPool) removeClientWithLock(c *Client) {
	p.clientsMu.Lock()
	p.removeClient(c)
	p.clientsMu.Unlock()
}

func (p *ClientPool) removeClient(cn *Client) {
	for i, c := range p.clients {
		if c == cn {
			p.clients = append(p.clients[:i], p.clients[i+1:]...)
			if cn.pooled {
				p.poolSize--
				p.checkMinIdleClients()
			}
			return
		}
	}
}

func (p *ClientPool) closeClient(c *Client) error {
	if p.opt.OnClose != nil {
		_ = p.opt.OnClose(c)
	}
	return c.Close()
}

func (p *ClientPool) Get(ctx context.Context) (*Client, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	err := p.waitTurn(ctx)
	if err != nil {
		return nil, err
	}

	for {
		p.clientsMu.Lock()
		cn := p.popIdle()
		p.clientsMu.Unlock()

		if cn == nil {
			break
		}

		if p.isStaleClient(cn) {
			_ = p.CloseClient(cn)
			continue
		}

		atomic.AddUint32(&p.stats.Hits, 1)
		return cn, nil
	}

	atomic.AddUint32(&p.stats.Misses, 1)

	newc, err := p.newClient(ctx, true)
	if err != nil {
		p.freeTurn()
		return nil, err
	}

	return newc, nil
}

func (p *ClientPool) getTurn() {
	p.queue <- struct{}{}
}

func (p *ClientPool) waitTurn(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	select {
	case p.queue <- struct{}{}:
		return nil
	default:
	}

	timer := timers.Get().(*time.Timer)
	timer.Reset(p.opt.PoolTimeout)

	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		timers.Put(timer)
		return ctx.Err()
	case p.queue <- struct{}{}:
		if !timer.Stop() {
			<-timer.C
		}
		timers.Put(timer)
		return nil
	case <-timer.C:
		timers.Put(timer)
		atomic.AddUint32(&p.stats.Timeouts, 1)
		return ErrPoolTimeout
	}
}

func (p *ClientPool) freeTurn() {
	<-p.queue
}

func (p *ClientPool) popIdle() *Client {
	if len(p.idleClients) == 0 {
		return nil
	}

	idx := len(p.idleClients) - 1
	cn := p.idleClients[idx]
	p.idleClients = p.idleClients[:idx]
	p.idleClientsLen--
	p.checkMinIdleClients()
	return cn
}

func (p *ClientPool) checkMinIdleClients() {
	if p.opt.MinIdleClients == 0 {
		return
	}
	for p.poolSize < p.opt.PoolSize && p.idleClientsLen < p.opt.MinIdleClients {
		p.poolSize++
		p.idleClientsLen++
		go func() {
			err := p.addIdleClient()
			if err != nil {
				p.clientsMu.Lock()
				p.poolSize--
				p.idleClientsLen--
				p.clientsMu.Unlock()
			}
		}()
	}
}

func (p *ClientPool) addIdleClient() error {
	cn, err := p.dialClient(context.TODO(), true)
	if err != nil {
		return err
	}

	p.clientsMu.Lock()
	p.clients = append(p.clients, cn)
	p.idleClients = append(p.idleClients, cn)
	p.clientsMu.Unlock()
	return nil
}

func (p *ClientPool) isStaleClient(cn *Client) bool {
	if p.opt.IdleTimeout == 0 && p.opt.MaxClientAge == 0 {
		return false
	}

	now := time.Now()
	if p.opt.IdleTimeout > 0 && now.Sub(cn.UsedAt()) >= p.opt.IdleTimeout {
		return true
	}
	if p.opt.MaxClientAge > 0 && now.Sub(cn.createdAt) >= p.opt.MaxClientAge {
		return true
	}

	return false
}

func (p *ClientPool) Put(client *Client) {
	if !client.pooled {
		p.Remove(client, nil)
		return
	}

	p.clientsMu.Lock()
	p.idleClients = append(p.idleClients, client)
	p.idleClientsLen++
	p.clientsMu.Unlock()
	p.freeTurn()
}

func (p *ClientPool) Remove(client *Client, reason error) {
	p.removeClientWithLock(client)
	p.freeTurn()
	_ = p.closeClient(client)
}

func (p *ClientPool) Len() int {
	p.clientsMu.Lock()
	n := len(p.clients)
	p.clientsMu.Unlock()
	return n
}

func (p *ClientPool) IdleLen() int {
	p.clientsMu.Lock()
	n := p.idleClientsLen
	p.clientsMu.Unlock()
	return n
}

func (p *ClientPool) Stats() *Stats {
	idleLen := p.IdleLen()
	return &Stats{
		Hits:     atomic.LoadUint32(&p.stats.Hits),
		Misses:   atomic.LoadUint32(&p.stats.Misses),
		Timeouts: atomic.LoadUint32(&p.stats.Timeouts),

		TotalClients: uint32(p.Len()),
		IdleClients:  uint32(idleLen),
		StaleClients: atomic.LoadUint32(&p.stats.StaleClients),
	}
}

func (p *ClientPool) Close() error {
	if !atomic.CompareAndSwapUint32(&p._closed, 0, 1) {
		return ErrClosed
	}
	close(p.closedCh)

	var firstErr error
	p.clientsMu.Lock()
	for _, cn := range p.clients {
		if err := p.closeClient(cn); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.clients = nil
	p.poolSize = 0
	p.idleClients = nil
	p.idleClientsLen = 0
	p.clientsMu.Unlock()

	return firstErr
}
