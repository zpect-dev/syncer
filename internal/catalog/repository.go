package catalog

import (
	"encoding/json"
	"fmt"

	"github.com/jmoiron/sqlx"
	// Importamos el driver de postgres, el _ es importante para que se registre
	_ "github.com/lib/pq"
)

// Agregamos los tags `db` que coinciden con los nombres de columnas o ALIAS en el SQL
type Category struct {
	Id     string `db:"co_lin" json:"id"`
	Titulo string `db:"lin_des" json:"titulo"`
}

type AlmacenResumen struct {
	Nombre            string  `json:"nombre"`
	StockTotal        float64 `json:"stock_total"`
	StockComprometido float64 `json:"stock_comprometido"`
	StockPorLlegar    float64 `json:"stock_por_llegar"`
}

type Product struct {
	CoArt         string  `db:"co_art" json:"co_art"`
	ArtDes        string  `db:"art_des" json:"art_des"`
	StockAct      float64 `db:"stock_act" json:"stock_act"`
	PrecVta1      float64 `db:"prec_vta1" json:"prec_vta1"`
	PrecVta2      float64 `db:"prec_vta2" json:"prec_vta2"`
	PrecVta3      float64 `db:"prec_vta3" json:"prec_vta3"`
	PrecVta4      float64 `db:"prec_vta4" json:"prec_vta4"`
	PrecVta5      float64 `db:"prec_vta5" json:"prec_vta5"`
	TipoImp       string  `db:"tipo_imp" json:"tipo_imp"`
	CoLin         string  `db:"co_lin" json:"co_lin"`
	CoCat         string  `db:"co_cat" json:"co_cat"`
	CoSubl        string  `db:"co_subl" json:"co_subl"`
	ImageUrl      string  `db:"image_url" json:"image_url"`
	DescArticulo  float64 `db:"desc_articulo" json:"desc_articulo"`
	DescCategoria float64 `db:"desc_categoria" json:"desc_categoria"`
	DescLinea     float64 `db:"desc_linea" json:"desc_linea"`

	// OJO: Este campo recibe el raw byte del SQL, el tag debe coincidir con el alias del query
	InventarioRaw []byte                    `db:"inventario_detallado" json:"-"`
	Inventario    map[string]AlmacenResumen `db:"-" json:"inventario"` // db:"-" ignora este campo en SQL
}

type Repository struct {
	db *sqlx.DB
}

// Ahora aceptamos *sqlx.DB desde el inicio para aprovechar sus métodos
func NewRepository(db *sqlx.DB) *Repository {
	return &Repository{db: db}
}

// Implementación del Batch Request
func (r *Repository) GetProductsByIDs(ids []string) ([]Product, error) {
	if len(ids) == 0 {
		return []Product{}, nil
	}

	// Usamos el query base compartido para no repetir código SQL
	baseQuery := getBaseProductQuery()
	// Añadimos el WHERE IN (?)
	query := fmt.Sprintf("%s WHERE a.co_art IN (?)", baseQuery)

	// 1. sqlx.In expande el slice a ?, ?, ?
	query, args, err := sqlx.In(query, ids)
	if err != nil {
		return nil, err
	}

	// 2. Rebind para Postgres ($1, $2...)
	query = r.db.Rebind(query)

	// 3. Select hace el query y el scan automáticamente
	var products []Product
	err = r.db.Select(&products, query, args...)
	if err != nil {
		return nil, err
	}

	// 4. Procesamos el JSON de inventario para cada producto
	// Lamentablemente esto hay que hacerlo manual porque SQL no mapea directo a Map
	for i := range products {
		if len(products[i].InventarioRaw) > 0 {
			_ = json.Unmarshal(products[i].InventarioRaw, &products[i].Inventario)
		}
	}

	return products, nil
}

func (r *Repository) GetByID(id string) (Product, error) {
	baseQuery := getBaseProductQuery()
	query := fmt.Sprintf("%s WHERE a.co_art = $1", baseQuery)

	var product Product
	// Get reemplaza a QueryRow + Scan
	err := r.db.Get(&product, query, id)

	if err != nil {
		// sql.ErrNoRows sigue existiendo en sqlx, pero es mejor retornarlo limpio
		return Product{}, err
	}

	// Deserializamos el JSON
	if len(product.InventarioRaw) > 0 {
		_ = json.Unmarshal(product.InventarioRaw, &product.Inventario)
	}

	return product, nil
}

func (r *Repository) ListProducts(page int, limit int, search string, category string) ([]Product, error) {
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * limit

	baseQuery := getBaseProductQuery()

	// Concatenamos el resto del query
	query := fmt.Sprintf(`
        %s 
        WHERE (a.art_des ILIKE $1 OR a.co_art ILIKE $1 OR a.campo4 ILIKE $1) 
        AND ($4 = '' OR a.co_lin = $4)
        ORDER BY a.art_des ASC
        LIMIT $2 OFFSET $3
    `, baseQuery)

	searchTerm := "%" + search + "%"

	var products []Product
	// Select reemplaza el loop de rows.Next() y Scan()
	err := r.db.Select(&products, query, searchTerm, limit, offset, category)
	if err != nil {
		return nil, fmt.Errorf("error obteniendo productos: %w", err)
	}

	// Procesamos JSONs
	for i := range products {
		if len(products[i].InventarioRaw) > 0 {
			_ = json.Unmarshal(products[i].InventarioRaw, &products[i].Inventario)
		}
	}

	return products, nil
}

func (r *Repository) ListCategories() ([]Category, error) {
	query := `SELECT co_lin, lin_des FROM lin_art WHERE co_lin NOT IN ('06', '07', '11', '12', '13', '14', '315')`

	var categories []Category
	// Select mapea automáticamente usando los tags db:"co_lin" y db:"lin_des"
	err := r.db.Select(&categories, query)
	if err != nil {
		return nil, fmt.Errorf("error listando categorias: %w", err)
	}

	return categories, nil
}

// Helper para no repetir el SELECT gigante
func getBaseProductQuery() string {
	// Nota el ALIAS "inventario_detallado". Es vital para que coincida con el struct tag `db:"inventario_detallado"`
	return `
       SELECT 
             a.co_art, 
             a.art_des,
             a.stock_act, 
             a.prec_vta1,
             a.prec_vta2,
             a.prec_vta3,
             a.prec_vta4,
             a.prec_vta5,
             a.tipo_imp,
             a.co_lin,
             a.co_cat,
             a.co_subl,
             COALESCE(a.image_url, '') AS image_url,
             COALESCE(a.inventory_json, '{}') AS inventario_detallado,
             COALESCE((SELECT porc1 FROM descuen WHERE co_desc = a.co_art LIMIT 1), 0) AS desc_articulo,
             COALESCE((SELECT porc1 FROM descuen WHERE co_desc = a.co_cat LIMIT 1), 0) AS desc_categoria,
             COALESCE((SELECT porc1 FROM descuen WHERE co_desc = a.co_lin LIMIT 1), 0) AS desc_linea
        FROM art a
    `
}
