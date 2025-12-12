package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"profit-ecommerce/internal/catalog"
	"strconv"

	"github.com/go-chi/chi/v5"
)

type CatalogHandler struct {
	repo *catalog.Repository
}

func NewCatalogHandler(repo *catalog.Repository) *CatalogHandler {
	return &CatalogHandler{repo: repo}
}

func (h *CatalogHandler) Single(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	if id == "" {
		http.Error(w, "ID es requerido", http.StatusBadRequest)
		return
	}

	product, err := h.repo.GetByID(id)
	if err != nil {
		fmt.Printf("Error: %s", err)
		http.Error(w, "Erro interno en el servidor", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data": product,
	})
}

func (h *CatalogHandler) List(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))

	if page < 1 {
		page = 1
	}
	if limit < 1 || limit > 100 {
		limit = 20
	}

	products, err := h.repo.ListProducts(page, limit, q)
	if err != nil {
		http.Error(w, "Error interno del servidor", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data":  products,
		"page":  page,
		"limit": limit,
		"total": len(products),
	})
}
