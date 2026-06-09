package syncer

import "context"

// pageSize define el tamaño de página para lecturas masivas paginadas.
const pageSize = 2000

// SourceRepo define el contrato de lectura contra la BD origen (Profit/SQL Server).
// El Service depende de esta interfaz, no de la implementación concreta.
type SourceRepo interface {
	FetchLinArt(ctx context.Context) ([]LinArt, error)
	FetchCatArt(ctx context.Context) ([]CatArt, error)
	FetchColores(ctx context.Context) ([]Color, error)
	FetchSubLin(ctx context.Context) ([]SubLin, error)
	FetchAlmacen(ctx context.Context) ([]Almacen, error)
	FetchSubAlma(ctx context.Context) ([]SubAlma, error)
	FetchDescuentos(ctx context.Context) ([]Descuento, error)
	FetchTiposCli(ctx context.Context) ([]TipoCli, error)
	FetchArticlesPage(ctx context.Context, limit, offset int) ([]Article, error)
	FetchStAlmacPage(ctx context.Context, limit, offset int) ([]StAlmac, error)
	FetchClientesPage(ctx context.Context, limit, offset int) ([]Cliente, error)
	FetchDescPtc(ctx context.Context) ([]DescPtc, error)
	FetchDescProv(ctx context.Context) ([]DescProv, error)
	FetchProv(ctx context.Context) ([]Prov, error)
	FetchSegmento(ctx context.Context) ([]Segmento, error)
}

// CacheInvalidator define el contrato para invalidar las cachés que la API
// mantiene sobre datos recién reescritos por el sync. El service lo invoca al
// final del fast sync; si la implementación es nil se omite silenciosamente
// (útil para tests y entornos sin Redis).
type CacheInvalidator interface {
	// InvalidateDiscounts borra las entradas de caché derivadas de las tablas
	// de descuentos. Devuelve la cantidad de keys eliminadas y el último
	// error no fatal, si lo hubo.
	InvalidateDiscounts(ctx context.Context) (int, error)
}

// DestRepo define el contrato de escritura contra la BD destino (PostgreSQL).
// El Service depende de esta interfaz, no de la implementación concreta.
type DestRepo interface {
	UpsertLinArt(ctx context.Context, items []LinArt) (int, error)
	UpsertCatArt(ctx context.Context, items []CatArt) (int, error)
	UpsertColores(ctx context.Context, items []Color) (int, error)
	UpsertSubLin(ctx context.Context, items []SubLin) (int, error)
	UpsertAlmacen(ctx context.Context, items []Almacen) (int, error)
	UpsertSubAlma(ctx context.Context, items []SubAlma) (int, error)
	TruncateAndInsertDescuentos(ctx context.Context, items []Descuento) (int, error)
	UpsertTiposCli(ctx context.Context, items []TipoCli) (int, error)
	UpsertArticles(ctx context.Context, items []Article) (int, error)
	UpsertStAlmac(ctx context.Context, items []StAlmac) (int, error)
	UpsertClientes(ctx context.Context, items []Cliente) (int, error)
	RecalculateInventoryJSON(ctx context.Context) error
	TruncateAndInsertDescPtc(ctx context.Context, items []DescPtc) (int, error)
	TruncateAndInsertDescProv(ctx context.Context, items []DescProv) (int, error)
	UpsertProv(ctx context.Context, items []Prov) (int, error)
	UpsertSegmento(ctx context.Context, items []Segmento) (int, error)
}
