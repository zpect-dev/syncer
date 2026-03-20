package catalog_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"profit-ecommerce/internal/api/handlers"
	"profit-ecommerce/internal/catalog"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// mockCatalogService permite interceptar la llamada desde el handler y asertar los tipos de filtros extraidos.
type mockCatalogService struct {
	mock.Mock
}

func (m *mockCatalogService) ListProducts(ctx context.Context, page, limit int, search, category string, inStock, hasDiscount bool) ([]catalog.Product, error) {
	args := m.Called(ctx, page, limit, search, category, inStock, hasDiscount)
	return args.Get(0).([]catalog.Product), args.Error(1)
}

func (m *mockCatalogService) GetByID(ctx context.Context, id string) (catalog.Product, error) {
	return catalog.Product{}, nil
}

func (m *mockCatalogService) GetProductsByIDs(ctx context.Context, ids []string) ([]catalog.Product, error) {
	return nil, nil
}

func (m *mockCatalogService) ListCategories(ctx context.Context) ([]catalog.Category, error) {
	return nil, nil
}

func TestCatalogHandler_ListProducts(t *testing.T) {
	tests := []struct {
		name             string
		queryParams      string
		expectedPage     int
		expectedLimit    int
		expectedSearch   string
		expectedCategory string
		expectedInStock  bool
		expectedDiscount bool
	}{
		{
			name:             "Sin parámetros usa defaults",
			queryParams:      "",
			expectedPage:     1,
			expectedLimit:    20,
			expectedSearch:   "",
			expectedCategory: "",
			expectedInStock:  false,
			expectedDiscount: false,
		},
		{
			name:             "Filtros booleanos y params explícitos correctos",
			queryParams:      "?page=2&limit=50&category=MED&in_stock=true&has_discount=1",
			expectedPage:     2,
			expectedLimit:    50,
			expectedSearch:   "",
			expectedCategory: "MED",
			expectedInStock:  true,
			expectedDiscount: true,
		},
		{
			name:             "Filtros booleanos inválidos no explotan (fallback false)",
			queryParams:      "?in_stock=cualquiercosa",
			expectedPage:     1,
			expectedLimit:    20,
			expectedSearch:   "",
			expectedCategory: "",
			expectedInStock:  false,
			expectedDiscount: false,
		},
		{
			name:             "Búsqueda FTS con espacios y parámetros FTS",
			queryParams:      "?search=lubrix+chocolate",
			expectedPage:     1,
			expectedLimit:    20,
			expectedSearch:   "lubrix chocolate",
			expectedCategory: "",
			expectedInStock:  false,
			expectedDiscount: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockSvc := new(mockCatalogService)
			handler := handlers.NewCatalogHandler(mockSvc)

			// Registramos con la configuración exacta para verificar que los request parsing mapean idéntico a lo esperado.
			mockSvc.On("ListProducts", mock.Anything, tt.expectedPage, tt.expectedLimit, tt.expectedSearch, tt.expectedCategory, tt.expectedInStock, tt.expectedDiscount).
				Return([]catalog.Product{}, nil)

			req := httptest.NewRequest(http.MethodGet, "/v1/products"+tt.queryParams, nil)
			rr := httptest.NewRecorder()

			// Ejecución del endpoint
			handler.List(rr, req)

			// Evaluaciones (Asserts) primarias de negocio
			assert.Equal(t, http.StatusOK, rr.Code)
			
			// Evaluamos que la librería testify valide internamente el encuadre exacto parseado por query:
			mockSvc.AssertExpectations(t)
		})
	}
}
