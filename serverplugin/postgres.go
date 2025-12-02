package serverplugin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/smallnest/rpcx/log"
)

const (
	ServiceChangeChannel = "rpcx_service_changes"
	DefaultServiceTable  = "rpcx_services"
)

// PostgresRegisterPlugin implements postgres registry.
type PostgresRegisterPlugin struct {
	// service address, for example, tcp@127.0.0.1:8972, quic@127.0.0.1:1234
	ServiceAddress string
	// service path for rpcx server, for example com/example/rpcx/myservice
	ServicePath string
	// PostgreSQL table for service registry
	table string
	// Metrics for monitoring
	Metrics metrics.Registry
	// Registered services
	Services []string
	// metadata
	metasLock sync.RWMutex
	metas     map[string]string
	// Update interval
	UpdateInterval time.Duration

	// PostgreSQL pool
	pool *pgxpool.Pool
	// Context for managing lifecycles
	ctx    context.Context
	cancel context.CancelFunc

	dying chan struct{}
	done  chan struct{}
}

// NewPostgresRegisterPlugin creates a new PostgresRegisterPlugin
func NewPostgresRegisterPlugin(ctx context.Context, pool *pgxpool.Pool, serviceAddress, servicePath, table string, updateInterval time.Duration) (*PostgresRegisterPlugin, error) {
	if pool == nil {
		return nil, fmt.Errorf("pgxpool cannot be nil")
	}
	if len(table) == 0 {
		table = DefaultServiceTable
	}

	ctx, cancel := context.WithCancel(ctx)
	return &PostgresRegisterPlugin{
		ServiceAddress: serviceAddress,
		ServicePath:    servicePath,
		table:          table,
		UpdateInterval: updateInterval,
		pool:           pool,
		ctx:            ctx,
		cancel:         cancel,
		metas:          make(map[string]string),
		dying:          make(chan struct{}),
		done:           make(chan struct{}),
	}, nil
}

// Start starts to connect postgres cluster
func (p *PostgresRegisterPlugin) Start() error {
	// Create required tables if they don't exist
	_, err := p.pool.Exec(p.ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			path TEXT NOT NULL,
    		address TEXT NOT NULL,
    		meta TEXT NOT NULL,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY(path, address)
		);

		CREATE OR REPLACE FUNCTION notify_service_change()
		RETURNS TRIGGER AS $$
		BEGIN
			PERFORM pg_notify('%s', 
				json_build_object(
           			'operation', TG_OP,
            		'service_path', CASE WHEN TG_OP = 'DELETE' THEN OLD.path ELSE NEW.path END,
            		'service_address', CASE WHEN TG_OP = 'DELETE' THEN OLD.address ELSE NEW.address END,
            		'meta', CASE WHEN TG_OP = 'DELETE' THEN OLD.meta ELSE NEW.meta END
        		)::text
			);
			RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
		END;
		$$ LANGUAGE plpgsql;

		CREATE TRIGGER IF NOT EXISTS service_changes_trigger
			AFTER INSERT OR UPDATE OR DELETE ON %s
			FOR EACH ROW EXECUTE FUNCTION notify_service_change();
	`, p.table, ServiceChangeChannel, p.table))

	if err != nil {
		log.Errorf("cannot create postgres tables: %v", err)
		return fmt.Errorf("failed to create tables: %w", err)
	}

	if p.UpdateInterval > 0 {
		go p.updateMetrics()
	}

	return nil
}

// updateMetrics updates service metrics
func (p *PostgresRegisterPlugin) updateMetrics() {
	ticker := time.NewTicker(p.UpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.dying:
			close(p.done)
			return
		case <-ticker.C:
			extra := make(map[string]string)
			if p.Metrics != nil {
				extra["calls"] = fmt.Sprintf("%.2f", metrics.GetOrRegisterMeter("calls", p.Metrics).RateMean())
				extra["connections"] = fmt.Sprintf("%.2f", metrics.GetOrRegisterMeter("connections", p.Metrics).RateMean())
			}

			conn, err := p.pool.Acquire(p.ctx)
			if err != nil {
				log.Errorf("failed to acquire connection: %v", err)
				continue
			}

			tx, err := conn.Begin(p.ctx)
			if err != nil {
				log.Errorf("failed to start transaction: %v", err)
				conn.Release()
				continue
			}

			for _, name := range p.Services {
				p.metasLock.RLock()
				meta := p.metas[name]
				p.metasLock.RUnlock()

				v, _ := url.ParseQuery(meta)
				for key, value := range extra {
					v.Set(key, value)
				}
				newMeta := v.Encode()

				_, err := tx.Exec(p.ctx,
					fmt.Sprintf(`UPDATE %s 
					SET meta = $1, updated_at = CURRENT_TIMESTAMP
					WHERE path = $2 AND address = $3`, p.table),
					newMeta, p.ServicePath, p.ServiceAddress)

				if err != nil {
					log.Errorf("failed to update service metrics: %v", err)
					tx.Rollback(p.ctx)
					break
				}
			}

			err = tx.Commit(p.ctx)
			if err != nil {
				log.Errorf("failed to commit transaction: %v", err)
			}
			conn.Release()

			p.cleanupStaleServices()
		}
	}
}

// Stop unregisters all services.
func (p *PostgresRegisterPlugin) Stop() error {
	close(p.dying)
	<-p.done

	// Unregister all services
	for _, name := range p.Services {
		if err := p.Unregister(name); err != nil {
			log.Errorf("failed to unregister service %s: %v", name, err)
		}
	}

	p.cancel()
	return nil
}

// HandleConnAccept handles connections from clients
func (p *PostgresRegisterPlugin) HandleConnAccept(conn net.Conn) (net.Conn, bool) {
	if p.Metrics != nil {
		metrics.GetOrRegisterMeter("connections", p.Metrics).Mark(1)
	}
	return conn, true
}

// PreCall handles rpc call from clients
func (p *PostgresRegisterPlugin) PreCall(_ context.Context, _, _ string, args interface{}) (interface{}, error) {
	if p.Metrics != nil {
		metrics.GetOrRegisterMeter("calls", p.Metrics).Mark(1)
	}
	return args, nil
}

// Register handles registering event.
func (p *PostgresRegisterPlugin) Register(name string, rcvr interface{}, metadata string) (err error) {
	if strings.TrimSpace(name) == "" {
		return errors.New("register service `name` can't be empty")
	}

	_, err = p.pool.Exec(p.ctx,
		fmt.Sprintf(`INSERT INTO %s (path, address, meta)
		VALUES ($1, $2, $3)
		ON CONFLICT (path, address) 
		DO UPDATE SET meta = $3, updated_at = CURRENT_TIMESTAMP`, p.table),
		p.ServicePath, p.ServiceAddress, metadata)

	if err != nil {
		log.Errorf("failed to register service: %v", err)
		return fmt.Errorf("failed to register service: %w", err)
	}

	p.Services = append(p.Services, name)

	p.metasLock.Lock()
	p.metas[name] = metadata
	p.metasLock.Unlock()

	return nil
}

func (p *PostgresRegisterPlugin) RegisterFunction(serviceName, fname string, fn interface{}, metadata string) error {
	return p.Register(serviceName, fn, metadata)
}

// Unregister removes service from registry.
func (p *PostgresRegisterPlugin) Unregister(name string) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("register service `name` can't be empty")
	}

	_, err := p.pool.Exec(p.ctx,
		fmt.Sprintf("DELETE FROM %s WHERE path = $1 AND address = $2", p.table),
		p.ServicePath, p.ServiceAddress)

	if err != nil {
		log.Errorf("failed to unregister service: %v", err)
		return fmt.Errorf("failed to unregister service: %w", err)
	}

	// Remove from local services list
	var services = make([]string, 0, len(p.Services)-1)
	for _, s := range p.Services {
		if s != name {
			services = append(services, s)
		}
	}
	p.Services = services

	p.metasLock.Lock()
	delete(p.metas, name)
	p.metasLock.Unlock()

	return nil
}

// cleanupStaleServices deletes services that haven't been updated for more than 2 update intervals
func (p *PostgresRegisterPlugin) cleanupStaleServices() {
	_, err := p.pool.Exec(p.ctx,
		fmt.Sprintf(`DELETE FROM %s 
		WHERE path = $1 AND updated_at < $2`, p.table),
		p.ServicePath,
		time.Now().Add(-p.UpdateInterval*2))

	if err != nil {
		log.Errorf("failed to cleanup stale services: %v", err)
	}
}
