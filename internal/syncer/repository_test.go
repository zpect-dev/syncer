package syncer

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDestRepository_RecalculateInventoryJSON(t *testing.T) {
	// Reglas Globales: Table-Driven Tests y Aislamiento (sqlmock simula la BD sin req db real)
	tests := []struct {
		name          string
		setupMock     func(mock sqlmock.Sqlmock)
		cancelCtx     bool
		expectedError string
	}{
		{
			name: "Exito: Actualiza el inventario calculando el stock global de almacenes",
			setupMock: func(mock sqlmock.Sqlmock) {
				// ExpectExec machea con expresiones regulares, asi simplificamos ignorando tabs
				mock.ExpectExec(regexp.QuoteMeta("WITH inventory_agg AS")).
					WillReturnResult(sqlmock.NewResult(0, 150)) // 150 articulos actualizados
			},
			cancelCtx: false,
		},
		{
			name: "Error de BD: Falla en la capa de la base de datos",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("WITH inventory_agg AS").
					WillReturnError(errors.New("db timeout"))
			},
			cancelCtx: false,
			expectedError: "error actualizando inventory JSON: db timeout",
		},
		{
			name: "Cobertura de Contexto: Detiene la operacion ante un timeout del parent",
			setupMock: func(mock sqlmock.Sqlmock) {
				// Canceled context shortcut en database/sql previene que el driver sea llamado,
				// no seteamos expectation aca.
			},
			cancelCtx: true, // Forzamos cancelacion antes de ejecutar
			expectedError: "context canceled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			// Usar sqlx como exige NewDestRepository
			sqlxDB := sqlx.NewDb(db, "postgres")
			repo := NewDestRepository(sqlxDB)

			var ctx context.Context
			var cancel context.CancelFunc

			if tt.cancelCtx {
				ctx, cancel = context.WithCancel(context.Background())
				// Cancelamos inmediatamente para simular timeout/aborts
				cancel()
			} else {
				ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
			}

			tt.setupMock(mock)

			// Metodo que corre el SQL puro
			err = repo.RecalculateInventoryJSON(ctx)

			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
			}

			err = mock.ExpectationsWereMet()
			assert.NoError(t, err)
		})
	}
}

// clientesColumns son las columnas que FetchClientesPage espera leer desde Profit.
// El orden debe coincidir con el SELECT del repositorio (la última, nit, se mapea a Sicm).
var clientesColumns = []string{
	"co_cli", "tipo", "cli_des", "rif", "inactivo",
	"login", "mont_cre", "direc1", "telefonos", "fax", "desc_glob", "nit", "co_seg",
}

func TestSourceRepository_FetchClientesPage(t *testing.T) {
	// Regla de Testing: Table-Driven con casos de Exito, Error de BD y Cobertura de Contexto.
	tests := []struct {
		name          string
		setupMock     func(mock sqlmock.Sqlmock)
		cancelCtx     bool
		expectedError string
		assertItems   func(t *testing.T, items []Cliente)
	}{
		{
			name: "Exito: Mapea la columna nit del origen a Sicm y la recorta",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows(clientesColumns).
					AddRow("C001", "T1", "Cliente Uno", "J-123", false, "5", "1000.50", "Av Siempre", "0212", "0212", "10.0", "  SICM-001  ", "  S01  ").
					AddRow("C002", "T1", "Cliente Dos", "J-456", false, "0", "0", "", "", "", "0", nil, nil)
				mock.ExpectQuery("SELECT co_cli, tipo, cli_des").WillReturnRows(rows)
			},
			assertItems: func(t *testing.T, items []Cliente) {
				require.Len(t, items, 2)
				assert.Equal(t, "SICM-001", items[0].Sicm, "nit debe recortarse y mapear a Sicm")
				assert.Equal(t, "S01", items[0].CoSeg, "co_seg debe recortarse y mapear a CoSeg")
				assert.Equal(t, "", items[1].Sicm, "nit NULL debe quedar como string vacio")
				assert.Equal(t, "", items[1].CoSeg, "co_seg NULL debe quedar como string vacio")
			},
		},
		{
			name: "Error de BD: Falla en la consulta de clientes",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT co_cli, tipo, cli_des").
					WillReturnError(errors.New("db timeout"))
			},
			expectedError: "error fetching clientes page",
		},
		{
			name:          "Cobertura de Contexto: Detiene la operacion ante un timeout del parent",
			setupMock:     func(mock sqlmock.Sqlmock) {},
			cancelCtx:     true,
			expectedError: "context canceled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			sqlxDB := sqlx.NewDb(db, "sqlserver")
			repo := NewSourceRepository(sqlxDB)

			var ctx context.Context
			var cancel context.CancelFunc
			if tt.cancelCtx {
				ctx, cancel = context.WithCancel(context.Background())
				cancel()
			} else {
				ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
			}

			tt.setupMock(mock)

			items, err := repo.FetchClientesPage(ctx, 100, 0)

			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
				if tt.assertItems != nil {
					tt.assertItems(t, items)
				}
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDestRepository_UpsertClientes(t *testing.T) {
	// Verifica que el UPSERT incluya la nueva columna sicm en el INSERT.
	tests := []struct {
		name          string
		items         []Cliente
		setupMock     func(mock sqlmock.Sqlmock)
		cancelCtx     bool
		expectedCount int
	}{
		{
			name: "Exito: Inserta cliente con co_seg (FK a segmento)",
			items: []Cliente{
				{CoCli: "C001", Tipo: "T1", CliDes: "Cliente Uno", Rif: "J-123", Sicm: "SICM-001", CoSeg: "S01"},
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO clientes (co_cli, tipo, cli_des, rif, inactivo, login, mont_cre, direc1, telefonos, fax, desc_glob, sicm, co_seg)")).
					WithArgs("C001", "T1", "Cliente Uno", "J-123", false, float64(0), float64(0), "", "", "", float64(0), "SICM-001", "S01").
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			expectedCount: 1,
		},
		{
			name: "Exito: co_seg vacio se persiste como NULL para no violar la FK",
			items: []Cliente{
				{CoCli: "C002", Tipo: "T1", CliDes: "Cliente Dos", Rif: "J-456", CoSeg: ""},
			},
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(regexp.QuoteMeta("INSERT INTO clientes (co_cli, tipo, cli_des, rif, inactivo, login, mont_cre, direc1, telefonos, fax, desc_glob, sicm, co_seg)")).
					WithArgs("C002", "T1", "Cliente Dos", "J-456", false, float64(0), float64(0), "", "", "", float64(0), "", nil).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			expectedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			sqlxDB := sqlx.NewDb(db, "postgres")
			repo := NewDestRepository(sqlxDB)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			tt.setupMock(mock)

			count, err := repo.UpsertClientes(ctx, tt.items)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedCount, count)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSourceRepository_FetchSegmento(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(mock sqlmock.Sqlmock)
		cancelCtx     bool
		expectedError string
		assertItems   func(t *testing.T, items []Segmento)
	}{
		{
			name: "Exito: Lee y recorta co_seg y seg_des",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"co_seg", "seg_des"}).
					AddRow("  S01  ", "  Mayorista  ").
					AddRow("S02", "Detal")
				mock.ExpectQuery("SELECT co_seg, seg_des FROM segmento").WillReturnRows(rows)
			},
			assertItems: func(t *testing.T, items []Segmento) {
				require.Len(t, items, 2)
				assert.Equal(t, "S01", items[0].CoSeg)
				assert.Equal(t, "Mayorista", items[0].SegDes)
			},
		},
		{
			name: "Error de BD: Falla en la consulta de segmento",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT co_seg, seg_des FROM segmento").
					WillReturnError(errors.New("db timeout"))
			},
			expectedError: "error fetching segmento",
		},
		{
			name:          "Cobertura de Contexto: Detiene la operacion ante un timeout del parent",
			setupMock:     func(mock sqlmock.Sqlmock) {},
			cancelCtx:     true,
			expectedError: "context canceled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			sqlxDB := sqlx.NewDb(db, "sqlserver")
			repo := NewSourceRepository(sqlxDB)

			var ctx context.Context
			var cancel context.CancelFunc
			if tt.cancelCtx {
				ctx, cancel = context.WithCancel(context.Background())
				cancel()
			} else {
				ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
			}

			tt.setupMock(mock)

			items, err := repo.FetchSegmento(ctx)

			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
				if tt.assertItems != nil {
					tt.assertItems(t, items)
				}
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDestRepository_UpsertSegmento(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "postgres")
	repo := NewDestRepository(sqlxDB)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO segmento (co_seg, seg_des)")).
		WithArgs("S01", "Mayorista").
		WillReturnResult(sqlmock.NewResult(0, 1))

	count, err := repo.UpsertSegmento(ctx, []Segmento{{CoSeg: "S01", SegDes: "Mayorista"}})
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.NoError(t, mock.ExpectationsWereMet())
}
