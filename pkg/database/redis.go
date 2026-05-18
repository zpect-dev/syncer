package database

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ConnectRedis acepta tanto "host:port" como una URL completa
// ("redis://..." o "rediss://..." con TLS, formato Managed Redis).
// Devuelve un cliente con ping verificado.
func ConnectRedis(url string) (*redis.Client, error) {
	if url == "" {
		return nil, fmt.Errorf("URL de Redis vacía")
	}

	var opts *redis.Options
	if strings.HasPrefix(url, "redis://") || strings.HasPrefix(url, "rediss://") {
		parsed, err := redis.ParseURL(url)
		if err != nil {
			return nil, fmt.Errorf("error parseando REDIS_URL: %w", err)
		}
		opts = parsed
	} else {
		opts = &redis.Options{Addr: url}
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("error conectando a Redis: %w", err)
	}
	log.Println("Conectado a Redis")
	return client, nil
}
