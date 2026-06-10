package syncer

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
)

// SourceRepository lee datos del sistema Profit (SQL Server).
type SourceRepository struct {
	db *sqlx.DB
}

// NewSourceRepository crea un nuevo repositorio de lectura contra la BD origen.
func NewSourceRepository(db *sqlx.DB) *SourceRepository {
	return &SourceRepository{db: db}
}

// DestRepository escribe datos en PostgreSQL (BD destino).
type DestRepository struct {
	db *sqlx.DB
}

// NewDestRepository crea un nuevo repositorio de escritura contra PostgreSQL.
func NewDestRepository(db *sqlx.DB) *DestRepository {
	return &DestRepository{db: db}
}

func (r *SourceRepository) FetchLinArt(ctx context.Context) ([]LinArt, error) {
	var items []LinArt
	rows, err := r.db.QueryContext(ctx, "SELECT co_lin, lin_des FROM lin_art")
	if err != nil {
		return nil, fmt.Errorf("error fetching lin_art: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item LinArt
		if err := rows.Scan(&item.CoLin, &item.LinDes); err != nil {
			log.Printf("Error scan lin_art: %v", err)
			continue
		}
		item.CoLin = strings.TrimSpace(item.CoLin)
		item.LinDes = strings.TrimSpace(item.LinDes)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *SourceRepository) FetchCatArt(ctx context.Context) ([]CatArt, error) {
	var items []CatArt
	rows, err := r.db.QueryContext(ctx, "SELECT co_cat, cat_des FROM cat_art")
	if err != nil {
		return nil, fmt.Errorf("error fetching cat_art: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item CatArt
		if err := rows.Scan(&item.CoCat, &item.CatDes); err != nil {
			log.Printf("Error scan cat_art: %v", err)
			continue
		}
		item.CoCat = strings.TrimSpace(item.CoCat)
		item.CatDes = strings.TrimSpace(item.CatDes)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *SourceRepository) FetchColores(ctx context.Context) ([]Color, error) {
	var items []Color
	rows, err := r.db.QueryContext(ctx, "SELECT co_col, des_col FROM colores")
	if err != nil {
		return nil, fmt.Errorf("error fetching colores: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item Color
		if err := rows.Scan(&item.CoCol, &item.DesCol); err != nil {
			log.Printf("Error scan colores: %v", err)
			continue
		}
		item.CoCol = strings.TrimSpace(item.CoCol)
		item.DesCol = strings.TrimSpace(item.DesCol)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *SourceRepository) FetchSubLin(ctx context.Context) ([]SubLin, error) {
	var items []SubLin
	rows, err := r.db.QueryContext(ctx, "SELECT co_subl, subl_des, co_lin FROM sub_lin")
	if err != nil {
		return nil, fmt.Errorf("error fetching sub_lin: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item SubLin
		if err := rows.Scan(&item.CoSubl, &item.SublDes, &item.CoLin); err != nil {
			log.Printf("Error scan sub_lin: %v", err)
			continue
		}
		item.CoSubl = strings.TrimSpace(item.CoSubl)
		item.SublDes = strings.TrimSpace(item.SublDes)
		item.CoLin = strings.TrimSpace(item.CoLin)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *SourceRepository) FetchAlmacen(ctx context.Context) ([]Almacen, error) {
	var items []Almacen
	rows, err := r.db.QueryContext(ctx, "SELECT co_alma, alma_des FROM almacen")
	if err != nil {
		return nil, fmt.Errorf("error fetching almacen: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item Almacen
		if err := rows.Scan(&item.CoAlma, &item.AlmaDes); err != nil {
			log.Printf("Error scan almacen: %v", err)
			continue
		}
		item.CoAlma = strings.TrimSpace(item.CoAlma)
		item.AlmaDes = strings.TrimSpace(item.AlmaDes)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *SourceRepository) FetchSubAlma(ctx context.Context) ([]SubAlma, error) {
	var items []SubAlma
	rows, err := r.db.QueryContext(ctx, "SELECT co_sub, des_sub, co_alma FROM sub_alma")
	if err != nil {
		return nil, fmt.Errorf("error fetching sub_alma: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item SubAlma
		if err := rows.Scan(&item.CoSub, &item.DesSub, &item.CoAlma); err != nil {
			log.Printf("Error scan sub_alma: %v", err)
			continue
		}
		item.CoSub = strings.TrimSpace(item.CoSub)
		item.DesSub = strings.TrimSpace(item.DesSub)
		item.CoAlma = strings.TrimSpace(item.CoAlma)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *SourceRepository) FetchDescuentos(ctx context.Context) ([]Descuento, error) {
	var items []Descuento
	rows, err := r.db.QueryContext(ctx, "SELECT co_desc, tipo_cli, tipo_desc, porc1 FROM descuen")
	if err != nil {
		return nil, fmt.Errorf("error fetching descuen: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item Descuento
		if err := rows.Scan(&item.CoDesc, &item.TipoCli, &item.TipoDesc, &item.Porc1); err != nil {
			log.Printf("Error scan descuen: %v", err)
			continue
		}
		item.CoDesc = strings.TrimSpace(item.CoDesc)
		item.TipoCli = strings.TrimSpace(item.TipoCli)
		item.TipoDesc = strings.TrimSpace(item.TipoDesc)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *SourceRepository) FetchArticlesPage(ctx context.Context, limit, offset int) ([]Article, error) {
	query := `
		SELECT
			a.co_art,
			a.art_des,
			a.prec_agr1,
			a.prec_agr2,
			a.prec_agr3,
			a.prec_agr4,
			a.prec_agr5,
			a.tipo_imp,
			COALESCE(a.co_lin, ''),
			COALESCE(a.co_cat, ''),
			COALESCE(a.co_subl, ''),
			COALESCE(a.campo4, ''),
			COALESCE(a.campo5, ''),
			COALESCE(a.co_color, ''),
			COALESCE(a.co_prov, ''),
			COALESCE(p.prov_des, ''),
			COALESCE(a.garantia, '')
		FROM art a
		LEFT JOIN prov p ON a.co_prov = p.co_prov
		WHERE a.anulado = 0 AND a.art_des NOT LIKE '%NO USAR%'
		ORDER BY a.co_art
		OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
	`
	rows, err := r.db.QueryContext(ctx, query, sql.Named("offset", offset), sql.Named("limit", limit))
	if err != nil {
		return nil, fmt.Errorf("error fetching articles page (offset=%d): %w", offset, err)
	}
	defer rows.Close()

	var items []Article
	for rows.Next() {
		var item Article
		if err := rows.Scan(
			&item.CoArt, &item.ArtDes,
			&item.PrecVta1, &item.PrecVta2, &item.PrecVta3, &item.PrecVta4, &item.PrecVta5,
			&item.TipoImp, &item.CoLin, &item.CoCat, &item.CoSubl, &item.Campo4, &item.SinDsc, &item.CoColor, &item.CoProv, &item.ProvDes, &item.Garantia,
		); err != nil {
			log.Printf("Error scan article: %v", err)
			continue
		}
		item.CoArt = strings.TrimSpace(item.CoArt)
		item.ArtDes = strings.TrimSpace(item.ArtDes)
		item.TipoImp = strings.TrimSpace(item.TipoImp)
		item.CoLin = strings.TrimSpace(item.CoLin)
		item.CoCat = strings.TrimSpace(item.CoCat)
		item.CoSubl = strings.TrimSpace(item.CoSubl)
		item.Campo4 = strings.TrimSpace(item.Campo4)
		item.SinDsc = strings.TrimSpace(item.SinDsc)
		item.CoColor = strings.TrimSpace(item.CoColor)
		item.CoProv = strings.TrimSpace(item.CoProv)
		item.ProvDes = strings.TrimSpace(item.ProvDes)
		item.Garantia = strings.TrimSpace(item.Garantia)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *SourceRepository) FetchStAlmacPage(ctx context.Context, limit, offset int) ([]StAlmac, error) {
	query := `
		SELECT 
			s.co_alma, s.co_art, 
			s.stock_act, s.sstock_act, 
			s.stock_com, s.sstock_com, 
			s.stock_lle, s.sstock_lle, 
			s.stock_des, s.sstock_des 
		FROM st_almac s
		INNER JOIN art a ON s.co_art = a.co_art
		WHERE a.anulado = 0 AND a.art_des NOT LIKE '%NO USAR%'
		ORDER BY s.co_art, s.co_alma
		OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
	`
	rows, err := r.db.QueryContext(ctx, query, sql.Named("offset", offset), sql.Named("limit", limit))
	if err != nil {
		return nil, fmt.Errorf("error fetching st_almac page (offset=%d): %w", offset, err)
	}
	defer rows.Close()

	var items []StAlmac
	for rows.Next() {
		var item StAlmac
		if err := rows.Scan(
			&item.CoAlma, &item.CoArt,
			&item.StockAct, &item.SStockAct,
			&item.StockCom, &item.SStockCom,
			&item.StockLle, &item.SStockLle,
			&item.StockDes, &item.SStockDes,
		); err != nil {
			log.Printf("Error scan st_almac: %v", err)
			continue
		}
		item.CoAlma = strings.TrimSpace(item.CoAlma)
		item.CoArt = strings.TrimSpace(item.CoArt)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *SourceRepository) FetchTiposCli(ctx context.Context) ([]TipoCli, error) {
	var items []TipoCli
	rows, err := r.db.QueryContext(ctx, "SELECT tip_cli, des_tipo, precio_a FROM tipo_cli")
	if err != nil {
		return nil, fmt.Errorf("error fetching tipo_cli: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item TipoCli
		if err := rows.Scan(&item.TipCli, &item.DesTipo, &item.PrecioA); err != nil {
			log.Printf("Error scan tipo_cli: %v", err)
			continue
		}
		item.TipCli = strings.TrimSpace(item.TipCli)
		item.DesTipo = strings.TrimSpace(item.DesTipo)
		item.PrecioA = strings.TrimSpace(item.PrecioA)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *SourceRepository) FetchClientesPage(ctx context.Context, limit, offset int) ([]Cliente, error) {
	query := `
		SELECT co_cli, tipo, cli_des, rif, inactivo, login, mont_cre, direc1, telefonos, fax, desc_glob, nit, co_seg
		FROM clientes
		ORDER BY co_cli
		OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
	`
	rows, err := r.db.QueryContext(ctx, query, sql.Named("offset", offset), sql.Named("limit", limit))
	if err != nil {
		return nil, fmt.Errorf("error fetching clientes page (offset=%d): %w", offset, err)
	}
	defer rows.Close()

	var items []Cliente
	for rows.Next() {
		var item Cliente
		var loginStr, montCreStr, direc1Str, telefonosStr, faxStr, descGlobStr, sicmStr, coSegStr sql.NullString

		if err := rows.Scan(&item.CoCli, &item.Tipo, &item.CliDes, &item.Rif, &item.Inactivo, &loginStr, &montCreStr, &direc1Str, &telefonosStr, &faxStr, &descGlobStr, &sicmStr, &coSegStr); err != nil {
			log.Printf("Error scan cliente: %v", err)
			continue
		}
		item.CoCli = strings.TrimSpace(item.CoCli)
		item.Tipo = strings.TrimSpace(item.Tipo)
		item.CliDes = strings.TrimSpace(item.CliDes)
		item.Rif = strings.TrimSpace(item.Rif)

		if loginStr.Valid {
			if val, err := strconv.ParseFloat(strings.TrimSpace(loginStr.String), 64); err == nil {
				item.Login = val
			}
		}
		if montCreStr.Valid {
			if val, err := strconv.ParseFloat(strings.TrimSpace(montCreStr.String), 64); err == nil {
				item.MontCre = val
			}
		}
		if direc1Str.Valid {
			item.Direc1 = strings.TrimSpace(direc1Str.String)
		}
		if telefonosStr.Valid {
			item.Telefonos = strings.TrimSpace(telefonosStr.String)
		}
		if faxStr.Valid {
			item.Fax = strings.TrimSpace(faxStr.String)
		}
		if descGlobStr.Valid {
			if val, err := strconv.ParseFloat(strings.TrimSpace(descGlobStr.String), 64); err == nil {
				item.DescGlob = val
			}
		}
		if sicmStr.Valid {
			item.Sicm = strings.TrimSpace(sicmStr.String)
		}
		if coSegStr.Valid {
			item.CoSeg = strings.TrimSpace(coSegStr.String)
		}

		items = append(items, item)
	}
	return items, rows.Err()
}

// FetchProv lee la tabla maestra de proveedores desde Profit.
func (r *SourceRepository) FetchProv(ctx context.Context) ([]Prov, error) {
	var items []Prov
	rows, err := r.db.QueryContext(ctx, "SELECT co_prov, prov_des FROM prov")
	if err != nil {
		return nil, fmt.Errorf("error fetching prov: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item Prov
		if err := rows.Scan(&item.CoProv, &item.ProvDes); err != nil {
			log.Printf("Error scan prov: %v", err)
			continue
		}
		item.CoProv = strings.TrimSpace(item.CoProv)
		item.ProvDes = strings.TrimSpace(item.ProvDes)
		items = append(items, item)
	}
	return items, rows.Err()
}

// FetchSegmento lee la tabla maestra de segmentos desde Profit.
func (r *SourceRepository) FetchSegmento(ctx context.Context) ([]Segmento, error) {
	var items []Segmento
	rows, err := r.db.QueryContext(ctx, "SELECT co_seg, seg_des FROM segmento")
	if err != nil {
		return nil, fmt.Errorf("error fetching segmento: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item Segmento
		if err := rows.Scan(&item.CoSeg, &item.SegDes); err != nil {
			log.Printf("Error scan segmento: %v", err)
			continue
		}
		item.CoSeg = strings.TrimSpace(item.CoSeg)
		item.SegDes = strings.TrimSpace(item.SegDes)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *SourceRepository) FetchDescPtc(ctx context.Context) ([]DescPtc, error) {
	var items []DescPtc
	rows, err := r.db.QueryContext(ctx, `
		SELECT COALESCE(co_prov, ''),
		       COALESCE(hasta1, 0),  COALESCE(hasta2, 0),  COALESCE(hasta3, 0),  COALESCE(hasta4, 0),  COALESCE(hasta5, 0),
		       COALESCE(hasta6, 0),  COALESCE(hasta7, 0),  COALESCE(hasta8, 0),  COALESCE(hasta9, 0),  COALESCE(hasta10, 0),
		       COALESCE(porc1, 0),   COALESCE(porc2, 0),   COALESCE(porc3, 0),   COALESCE(porc4, 0),   COALESCE(porc5, 0),
		       COALESCE(porc6, 0),   COALESCE(porc7, 0),   COALESCE(porc8, 0),   COALESCE(porc9, 0),   COALESCE(porc10, 0)
		FROM desc_ptc
		WHERE co_prov IS NOT NULL AND LTRIM(RTRIM(co_prov)) <> ''
	`)
	if err != nil {
		return nil, fmt.Errorf("error fetching desc_ptc: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item DescPtc
		if err := rows.Scan(
			&item.CoProv,
			&item.Hasta1, &item.Hasta2, &item.Hasta3, &item.Hasta4, &item.Hasta5,
			&item.Hasta6, &item.Hasta7, &item.Hasta8, &item.Hasta9, &item.Hasta10,
			&item.Porc1, &item.Porc2, &item.Porc3, &item.Porc4, &item.Porc5,
			&item.Porc6, &item.Porc7, &item.Porc8, &item.Porc9, &item.Porc10,
		); err != nil {
			log.Printf("Error scan desc_ptc: %v", err)
			continue
		}
		item.CoProv = strings.TrimSpace(item.CoProv)
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *SourceRepository) FetchDescProv(ctx context.Context) ([]DescProv, error) {
	var items []DescProv
	rows, err := r.db.QueryContext(ctx, `
		SELECT co_prov, co_cli, porc1
		FROM desc_prov
	`)
	if err != nil {
		return nil, fmt.Errorf("error fetching desc_prov: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item DescProv
		if err := rows.Scan(
			&item.CoProv, &item.CoCli, &item.Porc1,
		); err != nil {
			log.Printf("Error scan desc_prov: %v", err)
			continue
		}
		item.CoProv = strings.TrimSpace(item.CoProv)
		item.CoCli = strings.TrimSpace(item.CoCli)
		items = append(items, item)
	}
	return items, rows.Err()
}

// batchSize define cuántas filas se insertan por query batch.
const batchSize = 500

// buildPlaceholders genera los placeholders ($1,$2),($3,$4)... para N filas con colsPerRow columnas.
func buildPlaceholders(numRows, colsPerRow int) string {
	rows := make([]string, 0, numRows)
	paramIdx := 1
	for i := 0; i < numRows; i++ {
		cols := make([]string, 0, colsPerRow)
		for j := 0; j < colsPerRow; j++ {
			cols = append(cols, fmt.Sprintf("$%d", paramIdx))
			paramIdx++
		}
		rows = append(rows, "("+strings.Join(cols, ",")+")")
	}
	return strings.Join(rows, ",")
}

// execBatchWithFallback intenta el batch INSERT. Si falla (ej. FK violation en 1 fila),
func (r *DestRepository) execBatchWithFallback(ctx context.Context, queryTemplate string, args []interface{}, colsPerRow int) int {
	// Intento 1: batch completo
	numRows := len(args) / colsPerRow
	batchPlaceholders := buildPlaceholders(numRows, colsPerRow)
	batchQuery := fmt.Sprintf(queryTemplate, batchPlaceholders)

	_, err := r.db.ExecContext(ctx, batchQuery, args...)
	if err == nil {
		return numRows
	}

	// Intento 2: fallback fila por fila para este chunk
	log.Printf("Batch falló, reintentando %d filas individualmente... (%v)", numRows, err)
	count := 0
	for row := 0; row < numRows; row++ {
		start := row * colsPerRow
		end := start + colsPerRow
		rowArgs := args[start:end]

		singlePlaceholders := buildPlaceholders(1, colsPerRow)
		query := fmt.Sprintf(queryTemplate, singlePlaceholders)

		_, err := r.db.ExecContext(ctx, query, rowArgs...)
		if err != nil {
			log.Printf("  Fila ignorada: %v", err)
		} else {
			count++
		}
	}
	return count
}

func (r *DestRepository) UpsertLinArt(ctx context.Context, items []LinArt) (int, error) {
	const cols = 2
	count := 0
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[i:end]


		args := make([]interface{}, 0, len(chunk)*cols)
		for _, item := range chunk {
			args = append(args, item.CoLin, item.LinDes)
		}

		queryTpl := `
			INSERT INTO lin_art (co_lin, lin_des) VALUES %s
			ON CONFLICT (co_lin) DO UPDATE SET lin_des = EXCLUDED.lin_des
		`
		count += r.execBatchWithFallback(ctx, queryTpl, args, cols)
	}
	return count, nil
}

func (r *DestRepository) UpsertColores(ctx context.Context, items []Color) (int, error) {
	const cols = 2
	count := 0
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[i:end]

		args := make([]interface{}, 0, len(chunk)*cols)
		for _, item := range chunk {
			args = append(args, item.CoCol, item.DesCol)
		}

		queryTpl := `
			INSERT INTO colores (co_col, des_col) VALUES %s
			ON CONFLICT (co_col) DO UPDATE SET des_col = EXCLUDED.des_col
		`
		count += r.execBatchWithFallback(ctx, queryTpl, args, cols)
	}
	return count, nil
}

func (r *DestRepository) UpsertCatArt(ctx context.Context, items []CatArt) (int, error) {
	const cols = 2
	count := 0
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[i:end]

		args := make([]interface{}, 0, len(chunk)*cols)
		for _, item := range chunk {
			args = append(args, item.CoCat, item.CatDes)
		}

		queryTpl := `
			INSERT INTO cat_art (co_cat, cat_des) VALUES %s
			ON CONFLICT (co_cat) DO UPDATE SET cat_des = EXCLUDED.cat_des
		`
		count += r.execBatchWithFallback(ctx, queryTpl, args, cols)
	}
	return count, nil
}

func (r *DestRepository) UpsertSubLin(ctx context.Context, items []SubLin) (int, error) {
	const cols = 3
	count := 0
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[i:end]

		args := make([]interface{}, 0, len(chunk)*cols)
		for _, item := range chunk {
			args = append(args, item.CoSubl, item.SublDes, item.CoLin)
		}

		queryTpl := `
			INSERT INTO sub_lin (co_subl, subl_des, co_lin) VALUES %s
			ON CONFLICT (co_subl) DO UPDATE SET subl_des = EXCLUDED.subl_des, co_lin = EXCLUDED.co_lin
		`
		count += r.execBatchWithFallback(ctx, queryTpl, args, cols)
	}
	return count, nil
}

func (r *DestRepository) UpsertAlmacen(ctx context.Context, items []Almacen) (int, error) {
	const cols = 2
	count := 0
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[i:end]

		args := make([]interface{}, 0, len(chunk)*cols)
		for _, item := range chunk {
			args = append(args, item.CoAlma, item.AlmaDes)
		}

		queryTpl := `
			INSERT INTO almacen (co_alma, alma_des) VALUES %s
			ON CONFLICT (co_alma) DO UPDATE SET alma_des = EXCLUDED.alma_des
		`
		count += r.execBatchWithFallback(ctx, queryTpl, args, cols)
	}
	return count, nil
}

func (r *DestRepository) UpsertSubAlma(ctx context.Context, items []SubAlma) (int, error) {
	const cols = 3
	count := 0
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[i:end]

		args := make([]interface{}, 0, len(chunk)*cols)
		for _, item := range chunk {
			args = append(args, item.CoSub, item.DesSub, item.CoAlma)
		}

		queryTpl := `
			INSERT INTO sub_alma (co_sub, des_sub, co_alma) VALUES %s
			ON CONFLICT (co_sub) DO UPDATE SET des_sub = EXCLUDED.des_sub, co_alma = EXCLUDED.co_alma
		`
		count += r.execBatchWithFallback(ctx, queryTpl, args, cols)
	}
	return count, nil
}

// TruncateAndInsertDescuentos ejecuta TRUNCATE + INSERT dentro de una transacción explícita.
// Si cualquier batch falla, se hace Rollback y la tabla queda intacta.
func (r *DestRepository) TruncateAndInsertDescuentos(ctx context.Context, items []Descuento) (int, error) {
	const cols = 4

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("error iniciando transacción descuentos: %w", err)
	}
	// Rollback seguro: si ya se hizo Commit, Rollback es un no-op
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, "TRUNCATE TABLE descuen")
	if err != nil {
		return 0, fmt.Errorf("error truncando descuen: %w", err)
	}

	count := 0
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[i:end]

		placeholders := buildPlaceholders(len(chunk), cols)
		query := fmt.Sprintf(`
			INSERT INTO descuen (co_desc, tipo_cli, tipo_desc, porc1) VALUES %s
		`, placeholders)

		args := make([]interface{}, 0, len(chunk)*cols)
		for _, item := range chunk {
			args = append(args, item.CoDesc, item.TipoCli, item.TipoDesc, item.Porc1)
		}

		_, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return count, fmt.Errorf("error batch descuen (filas %d-%d): %w", i, end-1, err)
		}
		count += len(chunk)
	}

	if err := tx.Commit(); err != nil {
		return count, fmt.Errorf("error commit descuentos: %w", err)
	}

	return count, nil
}

func (r *DestRepository) TruncateAndInsertDescPtc(ctx context.Context, items []DescPtc) (int, error) {
	const cols = 21

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("error iniciando transacción desc_ptc: %w", err)
	}
	defer tx.Rollback()

	if _, err = tx.ExecContext(ctx, "TRUNCATE TABLE desc_ptc"); err != nil {
		return 0, fmt.Errorf("error truncando desc_ptc: %w", err)
	}

	count := 0
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[i:end]

		placeholders := buildPlaceholders(len(chunk), cols)
		query := fmt.Sprintf(`
			INSERT INTO desc_ptc
			(co_prov,
			 hasta1, hasta2, hasta3, hasta4, hasta5,
			 hasta6, hasta7, hasta8, hasta9, hasta10,
			 porc1, porc2, porc3, porc4, porc5,
			 porc6, porc7, porc8, porc9, porc10)
			VALUES %s
		`, placeholders)

		args := make([]interface{}, 0, len(chunk)*cols)
		for _, item := range chunk {
			args = append(args,
				item.CoProv,
				item.Hasta1, item.Hasta2, item.Hasta3, item.Hasta4, item.Hasta5,
				item.Hasta6, item.Hasta7, item.Hasta8, item.Hasta9, item.Hasta10,
				item.Porc1, item.Porc2, item.Porc3, item.Porc4, item.Porc5,
				item.Porc6, item.Porc7, item.Porc8, item.Porc9, item.Porc10,
			)
		}

		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return count, fmt.Errorf("error batch desc_ptc (filas %d-%d): %w", i, end-1, err)
		}
		count += len(chunk)
	}

	if err := tx.Commit(); err != nil {
		return count, fmt.Errorf("error commit desc_ptc: %w", err)
	}
	return count, nil
}

func (r *DestRepository) TruncateAndInsertDescProv(ctx context.Context, items []DescProv) (int, error) {
	const cols = 3

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("error iniciando transacción desc_prov: %w", err)
	}
	defer tx.Rollback()

	if _, err = tx.ExecContext(ctx, "TRUNCATE TABLE desc_prov"); err != nil {
		return 0, fmt.Errorf("error truncando desc_prov: %w", err)
	}

	count := 0
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[i:end]

		placeholders := buildPlaceholders(len(chunk), cols)
		query := fmt.Sprintf(`
			INSERT INTO desc_prov
			(co_prov, co_cli, porc1)
			VALUES %s
		`, placeholders)

		args := make([]interface{}, 0, len(chunk)*cols)
		for _, item := range chunk {
			args = append(args,
				item.CoProv, item.CoCli, item.Porc1,
			)
		}

		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return count, fmt.Errorf("error batch desc_prov (filas %d-%d): %w", i, end-1, err)
		}
		count += len(chunk)
	}

	if err := tx.Commit(); err != nil {
		return count, fmt.Errorf("error commit desc_prov: %w", err)
	}
	return count, nil
}

func (r *DestRepository) UpsertArticles(ctx context.Context, items []Article) (int, error) {
	const cols = 17
	count := 0

	toNull := func(s string) sql.NullString {
		if s == "" {
			return sql.NullString{}
		}
		return sql.NullString{String: s, Valid: true}
	}

	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[i:end]

		args := make([]interface{}, 0, len(chunk)*cols)
		for _, item := range chunk {
			args = append(args,
				item.CoArt, item.ArtDes,
				item.PrecVta1, item.PrecVta2, item.PrecVta3, item.PrecVta4, item.PrecVta5,
				item.TipoImp,
				toNull(item.CoLin), toNull(item.CoCat), toNull(item.CoSubl), toNull(item.Campo4), item.SinDsc, toNull(item.CoColor), toNull(item.CoProv), item.ProvDes, toNull(item.Garantia),
				// item.ImageURL,
			)
		}

		queryTpl := `
			INSERT INTO art (co_art, art_des, prec_vta1, prec_vta2, prec_vta3, prec_vta4, prec_vta5, tipo_imp, co_lin, co_cat, co_subl, campo4, sin_dsc, co_color, co_prov, prov_des, garantia)
			VALUES %s
			ON CONFLICT (co_art) DO UPDATE SET
				art_des = EXCLUDED.art_des,
				prec_vta1 = EXCLUDED.prec_vta1,
				prec_vta2 = EXCLUDED.prec_vta2,
				prec_vta3 = EXCLUDED.prec_vta3,
				prec_vta4 = EXCLUDED.prec_vta4,
				prec_vta5 = EXCLUDED.prec_vta5,
				tipo_imp = EXCLUDED.tipo_imp,
				co_lin = EXCLUDED.co_lin,
				co_cat = EXCLUDED.co_cat,
				co_subl = EXCLUDED.co_subl,
				campo4 = EXCLUDED.campo4,
				sin_dsc = EXCLUDED.sin_dsc,
				co_color = EXCLUDED.co_color,
				co_prov = EXCLUDED.co_prov,
				prov_des = EXCLUDED.prov_des,
				garantia = EXCLUDED.garantia,
				last_sync = NOW()
		`
		count += r.execBatchWithFallback(ctx, queryTpl, args, cols)
	}
	return count, nil
}

func (r *DestRepository) UpsertStAlmac(ctx context.Context, items []StAlmac) (int, error) {
	const cols = 10
	count := 0
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[i:end]

		args := make([]interface{}, 0, len(chunk)*cols)
		for _, item := range chunk {
			args = append(args,
				item.CoAlma, item.CoArt,
				item.StockAct, item.SStockAct,
				item.StockCom, item.SStockCom,
				item.StockLle, item.SStockLle,
				item.StockDes, item.SStockDes,
			)
		}

		queryTpl := `
			INSERT INTO st_almac (co_alma, co_art, stock_act, sstock_act, stock_com, sstock_com, stock_lle, sstock_lle, stock_des, sstock_des) 
			VALUES %s
			ON CONFLICT (co_alma, co_art) DO UPDATE SET
				stock_act = EXCLUDED.stock_act,
				sstock_act = EXCLUDED.sstock_act,
				stock_com = EXCLUDED.stock_com,
				sstock_com = EXCLUDED.sstock_com,
				stock_lle = EXCLUDED.stock_lle,
				sstock_lle = EXCLUDED.sstock_lle,
				stock_des = EXCLUDED.stock_des,
				sstock_des = EXCLUDED.sstock_des
		`
		count += r.execBatchWithFallback(ctx, queryTpl, args, cols)
	}
	return count, nil
}

func (r *DestRepository) RecalculateInventoryJSON(ctx context.Context) error {
	queryJSON := `
		WITH inventory_agg AS (
			SELECT 
				st_agg.co_art,
				jsonb_object_agg(
					TRIM(a.co_alma),
					jsonb_build_object(
						'nombre', TRIM(a.alma_des),
						'stock_total', st_agg.total_act,
						'stock_comprometido', st_agg.total_com,
						'stock_por_llegar', st_agg.total_lle
					)
				) as json_data,
				SUM(st_agg.total_act) as suma_total_act
			FROM (
				SELECT 
					st.co_art,
					sa.co_alma,
					SUM(st.stock_act) as total_act,
					SUM(st.stock_com) as total_com,
					SUM(st.stock_lle) as total_lle
				FROM st_almac st
				JOIN sub_alma sa ON st.co_alma = sa.co_sub
				GROUP BY st.co_art, sa.co_alma
				HAVING SUM(st.stock_act) > 0
			) st_agg
			JOIN almacen a ON st_agg.co_alma = a.co_alma
			GROUP BY st_agg.co_art
		)
		UPDATE art p
		SET 
			inventory_json = COALESCE(agg.json_data, '{}'::jsonb),
			stock_act = COALESCE(agg.suma_total_act, 0)
		FROM art p2
		LEFT JOIN inventory_agg agg ON p2.co_art = agg.co_art
		WHERE p.co_art = p2.co_art
		  AND (
			  p.stock_act IS DISTINCT FROM COALESCE(agg.suma_total_act, 0)
			  OR 
			  p.inventory_json IS DISTINCT FROM COALESCE(agg.json_data, '{}'::jsonb)
		  );
	`
	_, err := r.db.ExecContext(ctx, queryJSON)
	if err != nil {
		return fmt.Errorf("error actualizando inventory JSON: %w", err)
	}
	return nil
}

func (r *DestRepository) UpsertTiposCli(ctx context.Context, items []TipoCli) (int, error) {
	const cols = 3
	count := 0
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[i:end]

		args := make([]interface{}, 0, len(chunk)*cols)
		for _, item := range chunk {
			args = append(args, item.TipCli, item.DesTipo, item.PrecioA)
		}

		queryTpl := `
			INSERT INTO tipo_cli (tip_cli, des_tipo, precio_a) VALUES %s
			ON CONFLICT (tip_cli) DO UPDATE SET 
				des_tipo = EXCLUDED.des_tipo, 
				precio_a = EXCLUDED.precio_a
		`
		count += r.execBatchWithFallback(ctx, queryTpl, args, cols)
	}
	return count, nil
}

func (r *DestRepository) UpsertClientes(ctx context.Context, items []Cliente) (int, error) {
	const cols = 13
	count := 0

	// co_seg es FK a segmento; un valor vacío debe persistirse como NULL para no violar la FK.
	toNull := func(s string) sql.NullString {
		if s == "" {
			return sql.NullString{}
		}
		return sql.NullString{String: s, Valid: true}
	}

	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[i:end]

		args := make([]interface{}, 0, len(chunk)*cols)
		for _, item := range chunk {
			args = append(args, item.CoCli, item.Tipo, item.CliDes, item.Rif, item.Inactivo, item.Login, item.MontCre, item.Direc1, item.Telefonos, item.Fax, item.DescGlob, item.Sicm, toNull(item.CoSeg))
		}

		queryTpl := `
			INSERT INTO clientes (co_cli, tipo, cli_des, rif, inactivo, login, mont_cre, direc1, telefonos, fax, desc_glob, sicm, co_seg) VALUES %s
			ON CONFLICT (co_cli) DO UPDATE SET
				tipo = EXCLUDED.tipo,
				cli_des = EXCLUDED.cli_des,
				rif = EXCLUDED.rif,
				inactivo = EXCLUDED.inactivo,
				login = EXCLUDED.login,
				mont_cre = EXCLUDED.mont_cre,
				direc1 = EXCLUDED.direc1,
				telefonos = EXCLUDED.telefonos,
				fax = EXCLUDED.fax,
				desc_glob = EXCLUDED.desc_glob,
				sicm = EXCLUDED.sicm,
				co_seg = EXCLUDED.co_seg
		`
		count += r.execBatchWithFallback(ctx, queryTpl, args, cols)
	}
	return count, nil
}

// UpsertSegmento inserta o actualiza el catálogo maestro de segmentos en PostgreSQL.
func (r *DestRepository) UpsertSegmento(ctx context.Context, items []Segmento) (int, error) {
	const cols = 2
	count := 0
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[i:end]

		args := make([]interface{}, 0, len(chunk)*cols)
		for _, item := range chunk {
			args = append(args, item.CoSeg, item.SegDes)
		}

		queryTpl := `
			INSERT INTO segmento (co_seg, seg_des) VALUES %s
			ON CONFLICT (co_seg) DO UPDATE SET seg_des = EXCLUDED.seg_des
		`
		count += r.execBatchWithFallback(ctx, queryTpl, args, cols)
	}
	return count, nil
}

// UpsertProv inserta o actualiza el catálogo maestro de proveedores en PostgreSQL.
func (r *DestRepository) UpsertProv(ctx context.Context, items []Prov) (int, error) {
	const cols = 2
	count := 0
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		chunk := items[i:end]

		args := make([]interface{}, 0, len(chunk)*cols)
		for _, item := range chunk {
			args = append(args, item.CoProv, item.ProvDes)
		}

		queryTpl := `
			INSERT INTO prov (co_prov, prov_des) VALUES %s
			ON CONFLICT (co_prov) DO UPDATE SET prov_des = EXCLUDED.prov_des
		`
		count += r.execBatchWithFallback(ctx, queryTpl, args, cols)
	}
	return count, nil
}
