package catalog

import "context"

type catalogService struct {
	repo CatalogRepository
}

func NewCatalogService(repo CatalogRepository) CatalogService {
	return &catalogService{repo: repo}
}

func (s *catalogService) ListProducts(ctx context.Context, page, limit int, search, category string, inStock, hasDiscount bool) ([]Product, error) {
	return s.repo.ListProducts(ctx, page, limit, search, category, inStock, hasDiscount)
}

func (s *catalogService) GetByID(ctx context.Context, id string) (Product, error) {
	return s.repo.GetByID(ctx, id)
}

func (s *catalogService) GetProductsByIDs(ctx context.Context, ids []string) ([]Product, error) {
	return s.repo.GetProductsByIDs(ctx, ids)
}

func (s *catalogService) ListCategories(ctx context.Context) ([]Category, error) {
	return s.repo.ListCategories(ctx)
}
