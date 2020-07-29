package httppool_test

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	pool "github.com/5idu/httppool"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestGinkgoSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pool")
}

func perform(n int, cbs ...func(int)) {
	var wg sync.WaitGroup
	for _, cb := range cbs {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(cb func(int), i int) {
				defer GinkgoRecover()
				defer wg.Done()

				cb(i)
			}(cb, i)
		}
	}
	wg.Wait()
}

func dummyDialer(context.Context) (*http.Client, error) {
	return http.DefaultClient, nil
}

var _ = Describe("ConnPool", func() {
	c := context.Background()
	var connPool *pool.ClientPool

	BeforeEach(func() {
		connPool = pool.NewClientPool(&pool.Options{
			Dialer:             dummyDialer,
			PoolSize:           10,
			PoolTimeout:        time.Hour,
			IdleTimeout:        time.Millisecond,
			IdleCheckFrequency: time.Millisecond,
		})
	})

	AfterEach(func() {
		connPool.Close()
	})

	It("should unblock client when conn is removed", func() {
		// Reserve one connection.
		cn, err := connPool.Get(c)
		Expect(err).NotTo(HaveOccurred())

		// Reserve all other connections.
		var cns []*pool.Client
		for i := 0; i < 9; i++ {
			cn, err := connPool.Get(c)
			Expect(err).NotTo(HaveOccurred())
			cns = append(cns, cn)
		}

		started := make(chan bool, 1)
		done := make(chan bool, 1)
		go func() {
			defer GinkgoRecover()

			started <- true
			_, err := connPool.Get(c)
			Expect(err).NotTo(HaveOccurred())
			done <- true

			connPool.Put(cn)
		}()
		<-started

		// Check that Get is blocked.
		select {
		case <-done:
			Fail("Get is not blocked")
		case <-time.After(time.Millisecond):
			// ok
		}

		connPool.Remove(cn, nil)

		// Check that Get is unblocked.
		select {
		case <-done:
			// ok
		case <-time.After(time.Second):
			Fail("Get is not unblocked")
		}

		for _, cn := range cns {
			connPool.Put(cn)
		}
	})
})

var _ = Describe("MinIdleConns", func() {
	c := context.Background()
	const poolSize = 100
	var minIdleConns int
	var connPool *pool.ClientPool

	newConnPool := func() *pool.ClientPool {
		connPool := pool.NewClientPool(&pool.Options{
			Dialer:             dummyDialer,
			PoolSize:           poolSize,
			MinIdleClients:     minIdleConns,
			PoolTimeout:        100 * time.Millisecond,
			IdleTimeout:        -1,
			IdleCheckFrequency: -1,
		})
		Eventually(func() int {
			return connPool.Len()
		}).Should(Equal(minIdleConns))
		return connPool
	}

	assert := func() {
		It("has idle connections when created", func() {
			Expect(connPool.Len()).To(Equal(minIdleConns))
			Expect(connPool.IdleLen()).To(Equal(minIdleConns))
		})

		Context("after Get", func() {
			var cn *pool.Client

			BeforeEach(func() {
				var err error
				cn, err = connPool.Get(c)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return connPool.Len()
				}).Should(Equal(minIdleConns + 1))
			})

			It("has idle connections", func() {
				Expect(connPool.Len()).To(Equal(minIdleConns + 1))
				Expect(connPool.IdleLen()).To(Equal(minIdleConns))
			})

			Context("after Remove", func() {
				BeforeEach(func() {
					connPool.Remove(cn, nil)
				})

				It("has idle connections", func() {
					Expect(connPool.Len()).To(Equal(minIdleConns))
					Expect(connPool.IdleLen()).To(Equal(minIdleConns))
				})
			})
		})

		Describe("Get does not exceed pool size", func() {
			var mu sync.RWMutex
			var cns []*pool.Client

			BeforeEach(func() {
				cns = make([]*pool.Client, 0)

				perform(poolSize, func(_ int) {
					defer GinkgoRecover()

					cn, err := connPool.Get(c)
					Expect(err).NotTo(HaveOccurred())
					mu.Lock()
					cns = append(cns, cn)
					mu.Unlock()
				})

				Eventually(func() int {
					return connPool.Len()
				}).Should(BeNumerically(">=", poolSize))
			})

			It("Get is blocked", func() {
				done := make(chan struct{})
				go func() {
					connPool.Get(c)
					close(done)
				}()

				select {
				case <-done:
					Fail("Get is not blocked")
				case <-time.After(time.Millisecond):
					// ok
				}

				select {
				case <-done:
					// ok
				case <-time.After(time.Second):
					Fail("Get is not unblocked")
				}
			})

			Context("after Put", func() {
				BeforeEach(func() {
					perform(len(cns), func(i int) {
						mu.RLock()
						connPool.Put(cns[i])
						mu.RUnlock()
					})

					Eventually(func() int {
						return connPool.Len()
					}).Should(Equal(poolSize))
				})

				It("pool.Len is back to normal", func() {
					Expect(connPool.Len()).To(Equal(poolSize))
					Expect(connPool.IdleLen()).To(Equal(poolSize))
				})
			})

			Context("after Remove", func() {
				BeforeEach(func() {
					perform(len(cns), func(i int) {
						mu.RLock()
						connPool.Remove(cns[i], nil)
						mu.RUnlock()
					})

					Eventually(func() int {
						return connPool.Len()
					}).Should(Equal(minIdleConns))
				})

				It("has idle connections", func() {
					Expect(connPool.Len()).To(Equal(minIdleConns))
					Expect(connPool.IdleLen()).To(Equal(minIdleConns))
				})
			})
		})
	}

	Context("minIdleConns = 1", func() {
		BeforeEach(func() {
			minIdleConns = 1
			connPool = newConnPool()
		})

		AfterEach(func() {
			connPool.Close()
		})

		assert()
	})

	Context("minIdleConns = 32", func() {
		BeforeEach(func() {
			minIdleConns = 32
			connPool = newConnPool()
		})

		AfterEach(func() {
			connPool.Close()
		})

		assert()
	})
})

var _ = Describe("conns reaper", func() {
	c := context.Background()

	const idleTimeout = time.Minute
	const maxAge = time.Hour

	var connPool *pool.ClientPool
	var conns, staleConns, closedConns []*pool.Client

	assert := func(typ string) {
		BeforeEach(func() {
			closedConns = nil
			connPool = pool.NewClientPool(&pool.Options{
				Dialer:             dummyDialer,
				PoolSize:           10,
				IdleTimeout:        idleTimeout,
				MaxClientAge:       maxAge,
				PoolTimeout:        time.Second,
				IdleCheckFrequency: time.Hour,
				OnClose: func(cn *pool.Client) error {
					closedConns = append(closedConns, cn)
					return nil
				},
			})

			conns = nil

			// add stale connections
			staleConns = nil
			for i := 0; i < 3; i++ {
				cn, err := connPool.Get(c)
				Expect(err).NotTo(HaveOccurred())
				switch typ {
				case "idle":
					cn.SetUsedAt(time.Now().Add(-2 * idleTimeout))
				case "aged":
					cn.SetCreatedAt(time.Now().Add(-2 * maxAge))
				}
				conns = append(conns, cn)
				staleConns = append(staleConns, cn)
			}

			// add fresh connections
			for i := 0; i < 3; i++ {
				cn, err := connPool.Get(c)
				Expect(err).NotTo(HaveOccurred())
				conns = append(conns, cn)
			}

			for _, cn := range conns {
				connPool.Put(cn)
			}

			Expect(connPool.Len()).To(Equal(6))
			Expect(connPool.IdleLen()).To(Equal(6))

			n, err := connPool.ReapStaleClients()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(3))
		})

		AfterEach(func() {
			_ = connPool.Close()
			Expect(connPool.Len()).To(Equal(0))
			Expect(connPool.IdleLen()).To(Equal(0))
			Expect(len(closedConns)).To(Equal(len(conns)))
			Expect(closedConns).To(ConsistOf(conns))
		})

		It("reaps stale connections", func() {
			Expect(connPool.Len()).To(Equal(3))
			Expect(connPool.IdleLen()).To(Equal(3))
		})

		It("does not reap fresh connections", func() {
			n, err := connPool.ReapStaleClients()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(0))
		})

		It("stale connections are closed", func() {
			Expect(len(closedConns)).To(Equal(len(staleConns)))
			Expect(closedConns).To(ConsistOf(staleConns))
		})

		It("pool is functional", func() {
			for j := 0; j < 3; j++ {
				var freeCns []*pool.Client
				for i := 0; i < 3; i++ {
					cn, err := connPool.Get(c)
					Expect(err).NotTo(HaveOccurred())
					Expect(cn).NotTo(BeNil())
					freeCns = append(freeCns, cn)
				}

				Expect(connPool.Len()).To(Equal(3))
				Expect(connPool.IdleLen()).To(Equal(0))

				cn, err := connPool.Get(c)
				Expect(err).NotTo(HaveOccurred())
				Expect(cn).NotTo(BeNil())
				conns = append(conns, cn)

				Expect(connPool.Len()).To(Equal(4))
				Expect(connPool.IdleLen()).To(Equal(0))

				connPool.Remove(cn, nil)

				Expect(connPool.Len()).To(Equal(3))
				Expect(connPool.IdleLen()).To(Equal(0))

				for _, cn := range freeCns {
					connPool.Put(cn)
				}

				Expect(connPool.Len()).To(Equal(3))
				Expect(connPool.IdleLen()).To(Equal(3))
			}
		})
	}

	assert("idle")
	assert("aged")
})

var _ = Describe("race", func() {
	c := context.Background()
	var connPool *pool.ClientPool
	var C, N int

	BeforeEach(func() {
		C, N = 10, 1000
		if testing.Short() {
			C = 4
			N = 100
		}
	})

	AfterEach(func() {
		connPool.Close()
	})

	It("does not happen on Get, Put, and Remove", func() {
		connPool = pool.NewClientPool(&pool.Options{
			Dialer:             dummyDialer,
			PoolSize:           10,
			PoolTimeout:        time.Minute,
			IdleTimeout:        time.Millisecond,
			IdleCheckFrequency: time.Millisecond,
		})

		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				cn, err := connPool.Get(c)
				Expect(err).NotTo(HaveOccurred())
				if err == nil {
					connPool.Put(cn)
				}
			}
		}, func(id int) {
			for i := 0; i < N; i++ {
				cn, err := connPool.Get(c)
				Expect(err).NotTo(HaveOccurred())
				if err == nil {
					connPool.Remove(cn, nil)
				}
			}
		})
	})
})
