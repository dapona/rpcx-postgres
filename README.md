## Features
- PostgreSQL-based service discovery for [rpcx](https://github.com/smallnest/rpcx/) by [@smallnest](https://github.com/smallnest/)
- Real-time service updates using PostgreSQL NOTIFY/LISTEN
- Automatic cleanup of stale services

## Requirements
- PostgreSQL 13 or higher

## Configuration
The plugin can be configured with the following options:

### Server Plugin
```go
type PostgresRegisterPlugin struct {
    Table          string        // Table name for service registration (Default: "rpcx_services")
    ServicePath    string        // Service registration path
    ServiceAddress string        // Service address (e.g., "tcp@localhost:8972")
    UpdateInterval time.Duration // Interval for updating service TTL
}
```

### Client Discovery
```go
type PostgresDiscoveryOption struct {
    Table      string                         // Table name for service discovery (Default: "rpcx_services")
    RetryCount int                            // Retry count for watch operations (-1 for infinite)
    Filter     client.ServiceDiscoveryFilter  // Optional filter for services
}
```

## Example
Here's a complete example showing both server and client:

```go
package main

import (
    "context"
    "flag"
    "log"
    "time"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/smallnest/rpcx/server"
    "github.com/smallnest/rpcx/client"
    pgregistry "github.com/dapona/rpcx-postgres"
)

type Arith struct{}

func (t *Arith) Mul(ctx context.Context, args *Args, reply *Reply) error {
    reply.C = args.A * args.B
    return nil
}

type Args struct {
    A int `msg:"a"`
    B int `msg:"b"`
}

type Reply struct {
    C int `msg:"c"`
}

// Server example
func runServer(pool *pgxpool.Pool) error {
    s := server.NewServer()

    // Create the plugin
    plugin, err := pgregistry.NewPostgresRegisterPlugin(
        context.Background(),
        pool,
        "tcp@localhost:8972",    // service address
        "examples/arith",        // service path
        "rpcx_services",         // table name
        30*time.Second,          // update interval
    )
    if err != nil {
        return err
    }

    err = plugin.Start(context.Background())
    if err != nil {
        return err
    }
    defer plugin.Stop()

    // Add plugin to server
    s.Plugins.Add(plugin)

    // Register service
    s.RegisterName("Arith", new(Arith), "")

    // Start server
    return s.Serve("tcp", ":8972")
}

// Client example
func runClient(pool *pgxpool.Pool) error {
    // Create service discovery
    discovery, err := pgregistry.NewPostgresDiscoveryWithPool(
        context.Background(),
        "examples/arith",         // service path
        pool,
        "tcp@localhost:8973",     // client address
        &pgregistry.PostgresDiscoveryOption{
            RetryCount: -1,       // infinite retries
            "rpcx_services",      // table name
        },
    )
    if err != nil {
        return err
    }
    defer discovery.Close()

    // Create rpcx client
    xclient := client.NewXClient("Arith", client.Failtry, client.RandomSelect, discovery, client.DefaultOption)
    defer xclient.Close()

    // Call service
    args := &Args{A: 10, B: 20}
    reply := &Reply{}

    err = xclient.Call(context.Background(), "Mul", args, reply)
    if err != nil {
        return err
    }

    log.Printf("%d * %d = %d", args.A, args.B, reply.C)
    return nil
}

func main() {
    flag.Parse()

    // Create PostgreSQL connection pool
    pool, err := pgxpool.New(context.Background(), *postgresURL)
    if err != nil {
        log.Fatalf("Failed to create pool: %v", err)
    }
    defer pool.Close()

    // Run server in a goroutine
    go func() {
        if err := runServer(pool); err != nil {
            log.Printf("Server error: %v", err)
        }
    }()

    // Wait for server to start
    time.Sleep(time.Second)

    // Run client
    if err := runClient(pool); err != nil {
        log.Printf("Client error: %v", err)
    }
}
```

## License
MIT License