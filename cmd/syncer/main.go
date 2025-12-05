package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/lib/pq"
	_ "github.com/microsoft/go-mssqldb"
)

const (
	ProfitConnStr = "server=192.168.4.20;user id=profit;password=profit;port=1433;database=CRISTM25"
	PgConnStr     = "postgres://postgres:secret@localhost:5432/profit_ecommerce?sslmode=disable"
)

func main() {
	profitDB := connectDB("sqlserver", ProfitConnStr)
	defer profitDB.Close()
	pgDB := connectDB("postgres", PgConnStr)
	defer pgDB.Close()

	fmt.Println("Iniciando sincronizacion...")

	syncLines(profitDB, pgDB)
	syncCategories(profitDB, pgDB)
	syncSubLines(profitDB, pgDB)
	syncArts(profitDB, pgDB)

	fmt.Println("Sincronizacion finalizada con exito")
}

func syncLines(source, dest *sql.DB) {
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

func syncCategories(source, dest *sql.DB) {
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

func syncSubLines(source, dest *sql.DB) {
	fmt.Println("Sincronizando lineas...")

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

func syncArts(source, dest *sql.DB) {
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
			COALESCE(co_subl, '')
		FROM art
		WHERE anulado = 0
	`

	rows, err := source.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var co_art, art_des, tipo_imp, co_lin, co_cat, co_subl string
		var stock_act, prec_vta1, prec_vta2, prec_vta3, prec_vta4, prec_vta5 float64

		if err := rows.Scan(&co_art, &art_des, &stock_act, &prec_vta1, &prec_vta2, &prec_vta3, &prec_vta4, &prec_vta5, &tipo_imp, &co_lin, &co_cat, &co_subl); err != nil {
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

		_, err := dest.Exec(`
			INSERT INTO art (co_art, art_des, stock_act, prec_vta1, prec_vta2, prec_vta3, prec_vta4, prec_vta5, tipo_imp, co_lin, co_cat, co_subl, last_sync)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
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
					last_sync = NOW();
		`,
			strings.TrimSpace(co_art),
			strings.TrimSpace(art_des),
			stock_act, prec_vta1, prec_vta2, prec_vta3, prec_vta4, prec_vta5,
			strings.TrimSpace(tipo_imp),
			toNull(co_lin),
			toNull(co_cat),
			toNull(co_subl),
		)

		if err != nil {
			log.Printf("Error art %s: %v", co_art, err)
		} else {
			count++
		}
	}
	fmt.Printf("\nTotal de articulos sincronizados: %d\n", count)
}

func connectDB(driver, connStr string) *sql.DB {
	db, err := sql.Open(driver, connStr)
	if err != nil {
		log.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}
	return db
}
