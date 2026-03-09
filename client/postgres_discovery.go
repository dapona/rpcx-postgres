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
	rpcxlog "github.com/smallnest/rpcx/log"
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

	filter client.ServiceDiscoveryFilter

	stopCh    chan struct{}
	watchDone chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
}

// PostgresDiscoveryOption represents options for PostgresDiscovery
type PostgresDiscoveryOption struct {
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
		watchDone:      make(chan struct{}),
	}

	// Apply options
	if opt != nil {
		d.filter = opt.Filter
		if len(opt.Table) == 0 {
			opt.Table = serverplugin.DefaultServiceTable
		}
		d.table = opt.Table
	} else {
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

	d.pairsMu.RLock()
	filter := d.filter
	d.pairsMu.RUnlock()

	var pairs []*client.KVPair
	for rows.Next() {
		var address, meta string
		err := rows.Scan(&address, &meta)
		if err != nil {
			return fmt.Errorf("error scanning row: %w", err)
		}

		pair := &client.KVPair{Key: address, Value: meta}
		if filter != nil && !filter(pair) {
			continue
		}
		pairs = append(pairs, pair)
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	d.pairsMu.Lock()
	d.pairs = pairs
	snapshot := make([]*client.KVPair, len(d.pairs))
	copy(snapshot, d.pairs)
	d.pairsMu.Unlock()

	d.mu.Lock()
	for _, ch := range d.chans {
		go func() {
			defer func() {
				recover()
			}()

			select {
			case ch <- snapshot:
			case <-time.After(time.Minute):
				rpcxlog.Warn("chan is full and new change has been dropped")
			}
		}()
	}
	d.mu.Unlock()

	return nil
}

// watch retries indefinitely to maintain a LISTEN connection for service changes.
func (d *PostgresDiscovery) watch() {
	defer close(d.watchDone)

	var tempDelay time.Duration
	for {
		select {
		case <-d.stopCh:
			return
		default:
		}

		start := time.Now()
		watchCtx, cancel := context.WithCancel(d.ctx)
		err := d.watchChanges(watchCtx)
		cancel()

		if err == nil {
			continue
		}

		// Reset backoff if the connection was healthy for a while before failing.
		if time.Since(start) > 30*time.Second {
			tempDelay = 0
		}

		if tempDelay == 0 {
			tempDelay = time.Second
		} else {
			tempDelay *= 2
		}
		if max := 30 * time.Second; tempDelay > max {
			tempDelay = max
		}
		rpcxlog.Warnf("watch error (sleep %v): %v", tempDelay, err)
		select {
		case <-time.After(tempDelay):
		case <-d.stopCh:
			return
		}

		// Reload services after reconnecting — we may have missed
		// NOTIFY events during the outage (e.g. DB failover).
		if loadErr := d.loadServices(); loadErr != nil {
			rpcxlog.Warnf("failed to reload services during watch recovery: %v", loadErr)
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
		select {
		case <-d.stopCh:
			return nil
		case <-ctx.Done():
			return nil
		default:
			notification, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("error waiting for notification: %w", err)
			}

			var change ServiceChange
			err = json.Unmarshal([]byte(notification.Payload), &change)
			if err != nil {
				return fmt.Errorf("failed to unmarshal notification: %v", err)
			}

			// Skip if the change is not for this service
			if change.ServicePath != d.servicePath {
				continue
			}

			if change.Operation == "UPDATE" || change.Operation == "INSERT" {
				// Create new KVPair from the change
				pair := &client.KVPair{
					Key:   change.ServiceAddress,
					Value: change.Meta,
				}

				d.pairsMu.Lock()
				// Find and update existing pair or append new one
				updated := false
				for i, p := range d.pairs {
					if p.Key == pair.Key {
						d.pairs[i] = pair
						updated = true
						break
					}
				}
				if !updated {
					d.pairs = append(d.pairs, pair)
				}
				snapshot := make([]*client.KVPair, len(d.pairs))
				copy(snapshot, d.pairs)
				d.pairsMu.Unlock()

				// Notify watchers of the change
				d.mu.Lock()
				for _, ch := range d.chans {
					go func() {
						defer func() {
							recover()
						}()

						select {
						case ch <- snapshot:
						case <-time.After(time.Minute):
							rpcxlog.Warn("chan is full and new change has been dropped")
						}
					}()
				}
				d.mu.Unlock()
			} else if change.Operation == "DELETE" {
				d.pairsMu.Lock()
				// Remove the pair with matching address
				filtered := d.pairs[:0]
				for _, p := range d.pairs {
					if p.Key != change.ServiceAddress {
						filtered = append(filtered, p)
					}
				}
				d.pairs = filtered
				snapshot := make([]*client.KVPair, len(d.pairs))
				copy(snapshot, d.pairs)
				d.pairsMu.Unlock()

				// Notify watchers of the change
				d.mu.Lock()
				for _, ch := range d.chans {
					go func() {
						defer func() {
							recover()
						}()

						select {
						case ch <- snapshot:
						case <-time.After(time.Minute):
							rpcxlog.Warn("chan is full and new change has been dropped")
						}
					}()
				}
				d.mu.Unlock()
			}
		}
	}
}

// GetServices returns the servers
func (d *PostgresDiscovery) GetServices() []*client.KVPair {
	d.pairsMu.RLock()
	defer d.pairsMu.RUnlock()
	result := make([]*client.KVPair, len(d.pairs))
	copy(result, d.pairs)
	return result
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
	d.pairsMu.RLock()
	filter := d.filter
	d.pairsMu.RUnlock()

	return NewPostgresDiscoveryWithPool(context.Background(), d.serviceAddress, servicePath, d.pool, &PostgresDiscoveryOption{
		Filter: filter,
		Table:  d.table,
	})
}

// SetFilter sets the filter
func (d *PostgresDiscovery) SetFilter(filter client.ServiceDiscoveryFilter) {
	d.pairsMu.Lock()
	d.filter = filter
	d.pairsMu.Unlock()
}

// Close closes the discovery but not the underlying pool
func (d *PostgresDiscovery) Close() {
	// Signal to stop watching
	close(d.stopCh)
	if d.cancel != nil {
		d.cancel()
	}

	// Wait for watch goroutine to finish and release its connection
	<-d.watchDone

	// Close all watcher channels
	d.mu.Lock()
	for _, ch := range d.chans {
		close(ch)
	}
	d.chans = nil
	d.mu.Unlock()
}

// RefreshCache manually reloads the services from the database.
// This is useful when the NOTIFY mechanism fails to update the cache properly.
func (d *PostgresDiscovery) RefreshCache() error {
	rpcxlog.Infof("manually refreshing service cache for path: %s", d.servicePath)

	// Use loadServices to update the cache
	err := d.loadServices()
	if err != nil {
		return fmt.Errorf("failed to refresh service cache: %w", err)
	}

	rpcxlog.Infof("service cache refreshed successfully for path: %s", d.servicePath)
	return nil
}
