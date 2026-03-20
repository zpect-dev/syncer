package catalog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// mockCatalogRepository es el dummy implementado para Test de Servicio.
type mockCatalogRepository struct {
	mock.Mock
}

func (m *mockCatalogRepository) ListProducts(ctx context.Context, page, limit int, search, category string, inStock, hasDiscount bool) ([]Product, error) {
	args := m.Called(ctx, page, limit, search, category, inStock, hasDiscount)
	return args.Get(0).([]Product), args.Error(1)
}

func (m *mockCatalogRepository) GetByID(ctx context.Context, id string) (Product, error) {
	args := m.Called(ctx, id)
	return args.Get(0).(Product), args.Error(1)
}

func (m *mockCatalogRepository) GetProductsByIDs(ctx context.Context, ids []string) ([]Product, error) {
	args := m.Called(ctx, ids)
	return args.Get(0).([]Product), args.Error(1)
}

func (m *mockCatalogRepository) ListCategories(ctx context.Context) ([]Category, error) {
	args := m.Called(ctx)
	return args.Get(0).([]Category), args.Error(1)
}

func TestCatalogService_ListProducts(t *testing.T) {
	// Definimos el mock y el subject
	mockRepo := new(mockCatalogRepository)
	svc := NewCatalogService(mockRepo)

	ctx := context.Background()
	expectedProducts := []Product{
		{CoArt: "ART001", ArtDes: "Producto Prueba"},
	}

	// Configuramos el behavior esperado
	mockRepo.On("ListProducts", ctx, 1, 20, "aceite", "L01", true, false).Return(expectedProducts, nil)

	// Ejecutamos pasarela
	products, err := svc.ListProducts(ctx, 1, 20, "aceite", "L01", true, false)

	// Aserciones
	assert.NoError(t, err)
	assert.Equal(t, expectedProducts, products)

	// Validamos que verdaderamente el servicio hizo pasarela (forward) al repo
	mockRepo.AssertExpectations(t)
}
