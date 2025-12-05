package api

import (
	"database/sql"
	"net/http"
	"profit-ecommerce/internal/api/handlers"
	"profit-ecommerce/internal/catalog"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
)

func NewRouter(db *sql.DB) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"}, // En producción pon el dominio real de React
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	catRepo := catalog.NewRepository(db)
	catHandler := handlers.NewCatalogHandler(catRepo)

	r.Route("/v1", func(r chi.Router) {
		r.Get("/products", catHandler.List)
	})

	return r
}
