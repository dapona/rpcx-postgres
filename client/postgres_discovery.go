package client

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dapona/rpcx-postgres/serverplugin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/smallnest/rpcx/client"
	"github.com/smallnest/rpcx/log"
)

type ServiceChange struct {
	Operation      string `json:"operation"`
	ServicePath    string `json:"service_path"`
	ServiceAddress string `json:"service_address"`
	Meta           string `json:"meta"`
}

// PostgresDiscovery is a PostgreSQL-based service discovery.
// It accepts an external connection pool and watches for service updates.
type PostgresDiscovery struct {
	servicePath    string
	serviceAddress string
	table          string
	pool           *pgxpool.Pool
	pairsMu        sync.RWMutex
	pairs          []*client.KVPair
	chans          []chan []*client.KVPair
	mu             sync.Mutex

	// -1 means it always retry to watch until postgres is ok, 0 means no retry.
	RetriesAfterWatchFailed int

	filter client.ServiceDiscoveryFilter

	stopCh chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
}

// PostgresDiscoveryOption represents options for PostgresDiscovery
type PostgresDiscoveryOption struct {
	// RetryCount for watch failures. -1 means infinite retries
	RetryCount int
	// Filter for filtering services
	Filter client.ServiceDiscoveryFilter
	// Table for storing services
	Table string
}

// NewPostgresDiscoveryWithPool returns a new PostgresDiscovery using an existing pool.
func NewPostgresDiscoveryWithPool(ctx context.Context, serviceAddress, servicePath string, pool *pgxpool.Pool, opt *PostgresDiscoveryOption) (*PostgresDiscovery, error) {
	if pool == nil {
		return nil, fmt.Errorf("pgxpool cannot be nil")
	}

	ctx, cancel := context.WithCancel(ctx)
	d := &PostgresDiscovery{
		serviceAddress: serviceAddress,
		servicePath:    servicePath,
		pool:           pool,
		ctx:            ctx,
		cancel:         cancel,
		stopCh:         make(chan struct{}),
	}

	// Apply options
	if opt != nil {
		d.RetriesAfterWatchFailed = opt.RetryCount
		d.filter = opt.Filter
		if len(opt.Table) == 0 {
			opt.Table = serverplugin.DefaultServiceTable
		}
		d.table = opt.Table
	} else {
		d.RetriesAfterWatchFailed = -1
		d.table = serverplugin.DefaultServiceTable
	}

	// Initial load of services
	err := d.loadServices()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to load initial services: %w", err)
	}

	go d.watch()
	return d, nil
}

// loadServices loads all services from the database
func (d *PostgresDiscovery) loadServices() error {
	rows, err := d.pool.Query(d.ctx, fmt.Sprintf("SELECT address, meta FROM %s WHERE path = $1", d.table), d.servicePath)
	if err != nil {
		return fmt.Errorf("unable to query services: %w", err)
	}
	defer rows.Close()

	var pairs []*client.KVPair
	for rows.Next() {
		var address, meta string
		err := rows.Scan(&address, &meta)
		if err != nil {
			return fmt.Errorf("error scanning row: %w", err)
		}

		pair := &client.KVPair{Key: address, Value: meta}
		if d.filter != nil && !d.filter(pair) {
			continue
		}
		pairs = append(pairs, pair)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	d.pairsMu.Lock()
	d.pairs = pairs
	d.pairsMu.Unlock()

	return nil
}

func (d *PostgresDiscovery) watch() {
	for {
		select {
		case <-d.stopCh:
			log.Info("discovery has been closed")
			return
		default:
			var tempDelay time.Duration
			retry := d.RetriesAfterWatchFailed

			for d.RetriesAfterWatchFailed < 0 || retry >= 0 {
				watchCtx, cancel := context.WithCancel(d.ctx)
				err := d.watchChanges(watchCtx)
				cancel()

				if err != nil {
					if d.RetriesAfterWatchFailed > 0 {
						retry--
					}
					if tempDelay == 0 {
						tempDelay = time.Second
					} else {
						tempDelay *= 2
					}
					if max := 30 * time.Second; tempDelay > max {
						tempDelay = max
					}
					log.Warnf("watch error (with retry %d, sleep %v): %v", retry, tempDelay, err)
					time.Sleep(tempDelay)
					continue
				}
				break
			}
		}
	}
}

func (d *PostgresDiscovery) watchChanges(ctx context.Context) error {
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", serverplugin.ServiceChangeChannel))
	if err != nil {
		return fmt.Errorf("failed to start listening: %w", err)
	}

	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("error waiting for notification: %w", err)
		}

		var change ServiceChange
		err = json.Unmarshal([]byte(notification.Payload), &change)
		if err != nil {
			log.Errorf("failed to unmarshal notification: %v", err)
			continue
		}

		// Skip self-instance notifications
		if change.ServiceAddress == d.serviceAddress {
			continue
		}

		// Reload services and notify watchers
		err = d.loadServices()
		if err != nil {
			log.Errorf("failed to reload services after change: %v", err)
			continue
		}

		d.pairsMu.RLock()
		pairs := d.pairs
		d.pairsMu.RUnlock()

		d.mu.Lock()
		for _, ch := range d.chans {
			ch := ch
			go func() {
				defer func() {
					recover()
				}()

				select {
				case ch <- pairs:
				case <-time.After(time.Minute):
					log.Warn("chan is full and new change has been dropped")
				}
			}()
		}
		d.mu.Unlock()
	}
}

// GetServices returns the servers
func (d *PostgresDiscovery) GetServices() []*client.KVPair {
	d.pairsMu.RLock()
	defer d.pairsMu.RUnlock()
	return d.pairs
}

// WatchService returns a channel to watch for changes
func (d *PostgresDiscovery) WatchService() chan []*client.KVPair {
	d.mu.Lock()
	defer d.mu.Unlock()

	ch := make(chan []*client.KVPair, 10)
	d.chans = append(d.chans, ch)
	return ch
}

func (d *PostgresDiscovery) RemoveWatcher(ch chan []*client.KVPair) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var chans []chan []*client.KVPair
	for _, c := range d.chans {
		if c == ch {
			continue
		}
		chans = append(chans, c)
	}
	d.chans = chans
}

// Clone clones this ServiceDiscovery with new servicePath
func (d *PostgresDiscovery) Clone(servicePath string) (client.ServiceDiscovery, error) {
	return NewPostgresDiscoveryWithPool(context.Background(), d.serviceAddress, servicePath, d.pool, &PostgresDiscoveryOption{
		RetryCount: d.RetriesAfterWatchFailed,
		Filter:     d.filter,
		Table:      d.table,
	})
}

// SetFilter sets the filter
func (d *PostgresDiscovery) SetFilter(filter client.ServiceDiscoveryFilter) {
	d.filter = filter
}

// Close closes the discovery but not the underlying pool
func (d *PostgresDiscovery) Close() {
	close(d.stopCh)
	d.cancel()
}
