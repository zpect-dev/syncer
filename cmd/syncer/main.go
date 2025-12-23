package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"profit-ecommerce/internal/config"
	"profit-ecommerce/internal/db"

	"github.com/jmoiron/sqlx"
)

func main() {
	cfg := config.Load()

	// 1. CONEXIONES
	profitDB, err := db.ConnectSQLServer(cfg.ProfitDBURL)
	if err != nil {
		log.Fatalf("Error conectando a Profit: %v", err) // Fatalf para salir si falla la DB crítica
	}
	defer profitDB.Close()

	pgDB, err := db.ConnectPostgres(cfg.PostgresURL)
	if err != nil {
		log.Fatalf("Error conectando a Postgres: %v", err)
	}
	defer pgDB.Close()

	runMigrations(pgDB)

	fmt.Println("Worker Iniciado. Esperando ciclos...")

	// 2. DEFINIR LOS RELOJES
	// Ticker Rápido: Stock y Precios (Cada 1 minuto)
	fastTicker := time.NewTicker(1 * time.Minute)
	defer fastTicker.Stop()

	// Ticker Lento: Estructura y Nombres (Cada 1 hora)
	slowTicker := time.NewTicker(1 * time.Hour)
	defer slowTicker.Stop()

	// 3. EJECUTAR TODO UNA VEZ AL ARRANCAR (Para no esperar 1 hora al inicio
	go func() {
		fmt.Println("Ejecución inicial de arranque...")
		runSlowSync(profitDB, pgDB)
		runFastSync(profitDB, pgDB)
	}()

	// 4. BUCLE INFINITO (El corazón del programa)
	for {
		select {
		case <-fastTicker.C:
			// Cada 1 minuto cae aquí
			fmt.Println("\n[TICKER] Iniciando Sync Rápido (Stock/Precios)...")
			runFastSync(profitDB, pgDB)

		case <-slowTicker.C:
			// Cada 1 hora cae aquí
			fmt.Println("\n[TICKER] Iniciando Sync Lento (Maestros)...")
			runSlowSync(profitDB, pgDB)
		}
	}
}

func runMigrations(db *sqlx.DB) {
	migrationDir := "db/migrations" // 👈 Tu carpeta según la imagen
	fmt.Printf("Buscando migraciones en: %s\n", migrationDir)

	// 1. Leer todos los archivos de la carpeta
	files, err := os.ReadDir(migrationDir)
	if err != nil {
		log.Fatal("Error leyendo carpeta de migraciones: ", err)
	}

	var upMigrations []string

	// 2. Filtrar solo los que terminan en ".up.sql"
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".up.sql") {
			upMigrations = append(upMigrations, f.Name())
		}
	}

	// 3. Ordenarlos numéricamente (000001, 000002, etc)
	sort.Strings(upMigrations)

	// 4. Ejecutar uno por uno
	for _, filename := range upMigrations {
		fmt.Printf("Ejecutando: %s... ", filename)

		fullPath := filepath.Join(migrationDir, filename)
		content, err := os.ReadFile(fullPath)
		if err != nil {
			log.Fatalf("\nError leyendo %s: %v", filename, err)
		}

		// Ejecutamos el SQL
		_, err = db.Exec(string(content))
		if err != nil {
			// Si el error es "ya existe", lo ignoramos (opcional, pero útil en dev)
			// Pero mejor dejar que falle si algo está mal en la sintaxis
			log.Fatalf("\nError ejecutando %s: %v", filename, err)
		}
		fmt.Println("OK")
	}

	fmt.Println("Todas las migraciones aplicadas.")
}

// --- GRUPOS DE SINCRONIZACIÓN ---

// runSlowSync: Tablas "estáticas" o de configuración
func runSlowSync(profitDB, pgDB *sqlx.DB) {
	// Manejo de pánico para que el worker no muera si falla una tabla
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("⚠Error recuperado en Slow Sync:", r)
		}
	}()

	syncLines(profitDB, pgDB)
	syncCategories(profitDB, pgDB)
	syncSubLines(profitDB, pgDB)
	syncAlmacen(profitDB, pgDB)
	syncSubAlma(profitDB, pgDB)
}

// runFastSync: Tablas críticas de venta
func runFastSync(profitDB, pgDB *sqlx.DB) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Error recuperado en Fast Sync:", r)
		}
	}()
	// Descuentos
	syncDescuento(profitDB, pgDB)

	// 1. Artícul	os
	syncArts(profitDB, pgDB)

	// 2. Stock Detallado
	syncStAlmac(profitDB, pgDB)

	// 3. ACTUALIZACIÓN DEL JSON (CORREGIDA)
	fmt.Print("Recalculando JSON de Inventario... ")

	// El Truco: Hacemos el SUM primero en una subconsulta (pre_calculated)
	// Y luego el jsonb_object_agg afuera.
	queryJSON := `
		UPDATE art p
		SET inventory_json = subquery.json_data
		FROM (
			SELECT 
				pre_calculated.co_art,
				jsonb_object_agg(
					TRIM(pre_calculated.co_alma),
					jsonb_build_object(
						'nombre', TRIM(pre_calculated.alma_des),
						'stock_total', pre_calculated.total_act,
						'stock_comprometido', pre_calculated.total_com,
						'stock_por_llegar', pre_calculated.total_lle
					)
				) as json_data
			FROM (
				-- CAPA INTERNA: Sumamos por Artículo Y Almacén
				SELECT 
					st.co_art,
					a.co_alma,
					a.alma_des,
					SUM(st.stock_act) as total_act,
					SUM(st.stock_com) as total_com,
					SUM(st.stock_lle) as total_lle
				FROM st_almac st
				JOIN sub_alma sa ON st.co_alma = sa.co_sub
				JOIN almacen a ON sa.co_alma = a.co_alma
				GROUP BY st.co_art, a.co_alma, a.alma_des
				HAVING SUM(st.stock_act) > 0
			) pre_calculated
			GROUP BY pre_calculated.co_art
		) AS subquery
		WHERE p.co_art = subquery.co_art;
	`

	_, err := pgDB.Exec(queryJSON)

	if err != nil {
		log.Printf("\nError actualizando JSON: %v", err)
	} else {
		fmt.Println("OK")
	}
}

func syncStAlmac(source, des *sqlx.DB) {
	fmt.Println("Sincronizando st-almac")
	rows, err := source.Query(`
		SELECT 
			s.co_alma, 
			s.co_art, 
			s.stock_act, s.sstock_act, 
			s.stock_com, s.sstock_com, 
			s.stock_lle, s.sstock_lle, 
			s.stock_des, s.sstock_des 
		FROM st_almac s
		INNER JOIN art a ON s.co_art = a.co_art
		WHERE a.anulado = 0 AND a.art_des NOT LIKE '%NO USAR%'
	`)
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	count := 0
	for rows.Next() {
		var co_alma, co_art string
		var stock_act, sstock_act, stock_com, sstock_com, stock_lle, sstock_lle, stock_des, sstock_des float64
		rows.Scan(&co_alma, &co_art, &stock_act, &sstock_act, &stock_com, &sstock_com, &stock_lle, &sstock_lle, &stock_des, &sstock_des)

		_, err := des.Exec(`
			INSERT INTO st_almac (co_alma, co_art, stock_act, sstock_act, stock_com, sstock_com, stock_lle, sstock_lle, stock_des, sstock_des) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (co_alma, co_art) DO UPDATE SET
				stock_act = EXCLUDED.stock_act,
				sstock_act = EXCLUDED.sstock_act,
				stock_com = EXCLUDED.stock_com,
				sstock_com = EXCLUDED.sstock_com,
				stock_lle = EXCLUDED.stock_lle,
				sstock_lle = EXCLUDED.sstock_lle,
				stock_des = EXCLUDED.stock_des,
				sstock_des = EXCLUDED.sstock_des
		`, strings.TrimSpace(co_alma), strings.TrimSpace(co_art), stock_act, sstock_act, stock_com, sstock_com, stock_lle, sstock_lle, stock_des, sstock_des)

		if err != nil {
			log.Printf("Error st_almac %s - %s: %v", co_alma, co_art, err)
		} else {
			count++
		}
	}
	fmt.Printf("OK (%d procesadas)\n", count)
}

func syncSubAlma(source, des *sqlx.DB) {
	fmt.Println("Sincronizando sub_alma")
	rows, err := source.Query("SELECT co_sub, des_sub, co_alma FROM sub_alma")
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	count := 0
	for rows.Next() {
		var co_sub, des_sub, co_alma string
		rows.Scan(&co_sub, &des_sub, &co_alma)

		_, err := des.Exec(`
			INSERT INTO sub_alma (co_sub, des_sub, co_alma) VALUES ($1, $2, $3)
			ON CONFLICT (co_sub) DO UPDATE SET des_sub = EXCLUDED.des_sub, co_alma = EXCLUDED.co_alma
		`, strings.TrimSpace(co_sub), strings.TrimSpace(des_sub), strings.TrimSpace(co_alma))

		if err != nil {
			log.Printf("Error sub-almacen %s: %v", co_sub, err)
		} else {
			count++
		}
	}
	fmt.Printf("OK (%d procesadas)\n", count)
}

func syncDescuento(source, des *sqlx.DB) {
	fmt.Println("Sincronizando descuentos")
	rows, err := source.Query("SELECT co_desc, tipo_cli, tipo_desc, porc1 FROM descuen")
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	_, _ = des.Exec("TRUNCATE TABLE descuen")

	count := 0
	for rows.Next() {
		var co_desc, tipo_cli, tipo_desc string
		var porc1 float64

		rows.Scan(&co_desc, &tipo_cli, &tipo_desc, &porc1)

		_, err := des.Exec(`
			INSERT INTO descuen (co_desc, tipo_cli, tipo_desc, porc1) VALUES ($1, $2, $3, $4)
		`,
			strings.TrimSpace(co_desc),
			strings.TrimSpace(tipo_cli),
			strings.TrimSpace(tipo_desc),
			porc1)

		if err != nil {
			log.Printf("Error descuento %s %s: %v", co_desc, tipo_cli, err)
		} else {
			count++
		}
	}

	fmt.Printf("OK (%d procesados)\n", count)
}

func syncAlmacen(source, des *sqlx.DB) {
	fmt.Println("Sincronizando almacenes")
	rows, err := source.Query("SELECT co_alma, alma_des FROM almacen")
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	count := 0
	for rows.Next() {
		var co_alma, alma_des string
		rows.Scan(&co_alma, &alma_des)

		_, err := des.Exec(`
			INSERT INTO almacen	(co_alma, alma_des) VALUES ($1, $2)
			ON CONFLICT (co_alma) DO UPDATE SET alma_des = EXCLUDED.alma_des
		`, strings.TrimSpace(co_alma), strings.TrimSpace(alma_des))

		if err != nil {
			log.Printf("Error almacen %s: %v", co_alma, err)
		} else {
			count++
		}
	}
	fmt.Printf("OK (%d procesadas)\n", count)
}

func syncLines(source, dest *sqlx.DB) {
	fmt.Println("Sincronizando lineas...")
	rows, err := source.Query("SELECT co_lin, lin_des FROM lin_art")
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	count := 0
	for rows.Next() {
		var co_lin, lin_des string
		rows.Scan(&co_lin, &lin_des)

		_, err := dest.Exec(`
			INSERT INTO lin_art (co_lin, lin_des) VALUES ($1, $2)
			ON CONFLICT (co_lin) DO UPDATE SET lin_des = EXCLUDED.lin_des
		`, strings.TrimSpace(co_lin), strings.TrimSpace(lin_des))

		if err != nil {
			log.Printf("Error linea %s: %v", co_lin, err)
		} else {
			count++
		}
	}
	fmt.Printf("OK (%d procesadas)\n", count)
}

func syncCategories(source, dest *sqlx.DB) {
	fmt.Println("Sincronizando Categorias...")

	rows, err := source.Query("SELECT co_cat, cat_des FROM cat_art")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var co_cat, cat_des string
		rows.Scan(&co_cat, &cat_des)

		_, err := dest.Exec(`
			INSERT INTO cat_art (co_cat, cat_des) VALUES ($1, $2)
			ON CONFLICT (co_cat) DO UPDATE SET cat_des = EXCLUDED.cat_des
		`, strings.TrimSpace(co_cat), strings.TrimSpace(cat_des))

		if err != nil {
			log.Printf("Error cat %s:%v", co_cat, cat_des)
		} else {
			count++
		}
	}
	fmt.Printf("OK (%d procesadas)\n", count)
}

func syncSubLines(source, dest *sqlx.DB) {
	fmt.Println("Sincronizando sub-lineas...")

	rows, err := source.Query("SELECT co_subl, subl_des, co_lin FROM sub_lin")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var co_subl, subl_des, co_lin string
		rows.Scan(&co_subl, &subl_des, &co_lin)

		_, err := dest.Exec(`
			INSERT INTO	sub_lin (co_subl, subl_des, co_lin) VALUES ($1, $2, $3)
			ON CONFLICT (co_subl) DO UPDATE SET subl_des = EXCLUDED.subl_des, co_lin = EXCLUDED.co_lin
		`, strings.TrimSpace(co_subl), strings.TrimSpace(subl_des), strings.TrimSpace(co_lin))

		if err != nil {
			log.Printf("Warning sublinea %s: %v", co_subl, err)
		} else {
			count++
		}
	}
	fmt.Printf("OK (%d procesadas)\n", count)
}

func syncArts(source, dest *sqlx.DB) {
	fmt.Print("Sincronizando Articulos")

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
			COALESCE(co_lin, ''),
			COALESCE(co_cat, ''),
			COALESCE(co_subl, ''),
			COALESCE(campo4, '')
		FROM art
		WHERE anulado = 0 AND art_des NOT LIKE '%NO USAR%'
	`

	rows, err := source.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var co_art, art_des, tipo_imp, co_lin, co_cat, co_subl, campo4 string
		var stock_act, prec_vta1, prec_vta2, prec_vta3, prec_vta4, prec_vta5 float64

		if err := rows.Scan(&co_art, &art_des, &stock_act, &prec_vta1, &prec_vta2, &prec_vta3, &prec_vta4, &prec_vta5, &tipo_imp, &co_lin, &co_cat, &co_subl, &campo4); err != nil {
			log.Println("Error scan:", err)
			continue
		}

		toNull := func(s string) sql.NullString {
			s = strings.TrimSpace(s)
			if s == "" {
				return sql.NullString{}
			}
			return sql.NullString{String: s, Valid: true}
		}

		cleanCoArt := strings.TrimSpace(co_art)
		var imageCo string
		if cleanCoArt[len(cleanCoArt)-1] == 'C' || cleanCoArt[len(cleanCoArt)-1] == 'A' {
			imageCo = cleanCoArt[:len(cleanCoArt)-1]
		} else {
			imageCo = cleanCoArt
		}

		// 2. Genera la URL en Go
		imageUrl := fmt.Sprintf("https://imagenes.cristmedicals.com/imagenes-v3/imagenes/%s.jpg", imageCo)

		_, err := dest.Exec(`
			INSERT INTO art (co_art, art_des, stock_act, prec_vta1, prec_vta2, prec_vta3, prec_vta4, prec_vta5, tipo_imp, co_lin, co_cat, co_subl, campo4, image_url, last_sync)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())
			ON CONFLICT (co_art) DO UPDATE SET
			        art_des = EXCLUDED.art_des,
			        stock_act = EXCLUDED.stock_act,
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
			    	image_url = EXCLUDED.image_url,
					last_sync = NOW();
		`,
			cleanCoArt,
			strings.TrimSpace(art_des),
			stock_act, prec_vta1, prec_vta2, prec_vta3, prec_vta4, prec_vta5,
			strings.TrimSpace(tipo_imp),
			toNull(co_lin),
			toNull(co_cat),
			toNull(co_subl),
			toNull(campo4),
			imageUrl,
		)

		if err != nil {
			log.Printf("Error art %s: %v", co_art, err)
		} else {
			count++
		}
	}
	fmt.Printf("\nTotal de articulos sincronizados: %d\n", count)
}
