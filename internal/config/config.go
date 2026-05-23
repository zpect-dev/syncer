package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	PostgresURL string
	ProfitDBURL string
	RedisURL    string
}

func Load() *Config {
	// Sólo intentamos cargar .env si existe en disco (dev local).
	// En contenedor las variables vienen inyectadas por docker-compose `env_file`.
	if _, err := os.Stat(".env"); err == nil {
		if err := godotenv.Load(); err != nil {
			log.Printf("Advertencia: error cargando .env existente: %v", err)
		}
	}

	return &Config{
		PostgresURL: getEnv("POSTGRES_URL", ""),
		ProfitDBURL: getEnv("PROFIT_DB_URL", ""),
		RedisURL:    getEnv("REDIS_URL", ""),
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	if fallback == "" {
		log.Printf("Advertencia: Variable de entorno %s no definida", key)
	}
	return fallback
}
