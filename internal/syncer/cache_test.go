package syncer

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMiniRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

func TestRedisCacheInvalidator_InvalidateDiscounts_RemovesOnlyDiscountKeys(t *testing.T) {
	mr, rdb := newMiniRedis(t)

	// Keys que SÍ deben borrarse (los 3 niveles que mantiene la API).
	mr.HSet("discounts:prov:CLI01", "P-A", "15")
	mr.HSet("discounts:prov:CLI02", "P-B", "20")
	mr.HSet("discounts:01", "ART1", "10")
	mr.HSet("discounts:02", "CAT-A", "12")
	mr.HSet("discounts:scale:P-A", "desde1", "1", "porc1", "5")

	// Keys de otros dominios que NO deben tocarse.
	mr.Set("cart:CLI01", "payload")
	mr.HSet("favorites:CLI01", "ART1", "1")
	mr.Set("auth:session:xyz", "token")

	inv := NewRedisCacheInvalidator(rdb)
	require.NotNil(t, inv)

	n, err := inv.InvalidateDiscounts(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 5, n, "deben borrarse exactamente las 5 keys de discounts")

	// Verificamos que las de descuentos ya no existen.
	for _, k := range []string{
		"discounts:prov:CLI01",
		"discounts:prov:CLI02",
		"discounts:01",
		"discounts:02",
		"discounts:scale:P-A",
	} {
		assert.False(t, mr.Exists(k), "%s no debería existir", k)
	}

	// Y que las de otros dominios siguen intactas.
	assert.True(t, mr.Exists("cart:CLI01"))
	assert.True(t, mr.Exists("favorites:CLI01"))
	assert.True(t, mr.Exists("auth:session:xyz"))
}

func TestRedisCacheInvalidator_InvalidateDiscounts_NoKeysIsNoop(t *testing.T) {
	_, rdb := newMiniRedis(t)
	inv := NewRedisCacheInvalidator(rdb)

	n, err := inv.InvalidateDiscounts(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestRedisCacheInvalidator_NilClient_IsSafe(t *testing.T) {
	// El constructor debe rechazar un client nil devolviendo nil, y
	// llamar InvalidateDiscounts sobre un receptor nil debe ser no-op.
	inv := NewRedisCacheInvalidator(nil)
	assert.Nil(t, inv)

	n, err := inv.InvalidateDiscounts(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestRedisCacheInvalidator_ScanIteratesMultipleBatches(t *testing.T) {
	// Sembramos más keys que scanBatch para forzar al menos 2 iteraciones.
	mr, rdb := newMiniRedis(t)
	const total = scanBatch + 50
	for i := 0; i < total; i++ {
		mr.Set("discounts:bulk:"+itoa(i), "x")
	}

	inv := NewRedisCacheInvalidator(rdb)
	n, err := inv.InvalidateDiscounts(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, total, n)
	// Nada con prefijo discounts: debe sobrevivir.
	keys := mr.Keys()
	for _, k := range keys {
		assert.NotContains(t, k, "discounts:")
	}
}

// itoa local para evitar importar strconv sólo en este helper.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
