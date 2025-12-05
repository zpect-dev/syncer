package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"net/http"
	"profit-ecommerce/internal/api"
)

func main() {
	connStr := "postgres://postgres:secret@localhost:5432/profit_ecommerce?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal("No se pudo conectar a postgres")
	}

	router := api.NewRouter(db)

	port := ":8080"
	fmt.Printf("API Gateway listo en http://localhost%s\n", port)
	fmt.Println("Prueba: http://localhost:8080/v1/products")

	if err := http.ListenAndServe(port, router); err != nil {
		log.Fatal(err)
	}
}
