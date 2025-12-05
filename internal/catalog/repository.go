package catalog

import (
	"database/sql"
	"fmt"
)

type Product struct {
	CoArt    string  `json:"co_art"`
	ArtDes   string  `json:"art_des"`
	StockAct float64 `json:"stock_act"`
	PrecVta1 float64 `json:"prec_vta1"`
	PrecVta2 float64 `json:"prec_vta2"`
	PrecVta3 float64 `json:"prec_vta3"`
	PrecVta4 float64 `json:"prec_vta4"`
	PrecVta5 float64 `json:"prec_vta5"`
	TipoImp  string  `json:"tipo_imp"`
	CoLin    string  `json:"co_lin"`
	CoCat    string  `json:"co_cat"`
	CoSubl   string  `json:"co_subl"`
	ImageUrl string  `json:"image_url"`
}

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) ListProducts(page, limit int, search string) ([]Product, error) {
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * limit

	query := `
			SELECT
				co_art, 
				art_des,
				stock_act,
				prec_vta1,
				prec_vta2,
				prec_vta3,
				prec_vta4,
				prec_vta5,
				tipo_imp,
				co_lin,
				co_cat,
				co_subl,
				COALESCE(image_url, '')
			FROM art
			WHERE (art_des ILIKE $1 OR co_art ILIKE $1)
			ORDER BY art_des ASC
			LIMIT $2 OFFSET $3
	`

	searchTerm := "%" + search + "%"
	rows, err := r.db.Query(query, searchTerm, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("Error en el query: %w", err)
	}
	defer rows.Close()

	products := []Product{}

	for rows.Next() {
		var p Product

		if err := rows.Scan(&p.CoArt, &p.ArtDes, &p.StockAct, &p.PrecVta1, &p.PrecVta2, &p.PrecVta3, &p.PrecVta4, &p.PrecVta5, &p.TipoImp, &p.CoLin, &p.CoCat, &p.CoSubl, &p.ImageUrl); err != nil {
			return nil, err
		}

		if p.ImageUrl == "" {
			p.ImageUrl = "https://via.placeholder.com/300?text=Sin+Foto"
		}

		products = append(products, p)
	}

	return products, nil
}
