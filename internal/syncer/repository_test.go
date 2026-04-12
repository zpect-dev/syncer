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
