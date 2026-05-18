package syncer

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// discountCachePattern es el prefijo que comparten todas las keys de caché de
// descuentos que mantiene la API:
//   - discounts:prov:<coCli>      (nivel 1)
//   - discounts:<tipoCli>         (nivel 2, compartido con catalog)
//   - discounts:scale:<coProv>    (nivel 3)
//
// Tras un fast sync se reescriben las 3 tablas fuente, por lo que se invalida
// todo el conjunto de un solo barrido.
const discountCachePattern = "discounts:*"

// scanBatch es la pista (hint) de COUNT que se pasa a SCAN. No limita el
// resultado, sólo orienta cuánta clave se traerá por iteración.
const scanBatch = 500

// RedisCacheInvalidator implementa CacheInvalidator contra una instancia de
// Redis. Se construye con un cliente ya conectado.
type RedisCacheInvalidator struct {
	client *redis.Client
}

// NewRedisCacheInvalidator construye el invalidador. Si client es nil devuelve
// nil; los callers deben tolerarlo (ver Service: si el invalidador es nil,
// simplemente no se invalida).
func NewRedisCacheInvalidator(client *redis.Client) *RedisCacheInvalidator {
	if client == nil {
		return nil
	}
	return &RedisCacheInvalidator{client: client}
}

// InvalidateDiscounts barre con SCAN todas las keys que matchean
// "discounts:*" y las elimina con UNLINK (borrado asíncrono en background,
// no bloquea el hilo principal de Redis incluso si hay hashes grandes).
//
// Errores parciales en UNLINK no abortan el barrido: se loguean por el caller
// vía el error retornado al final. SCAN sí aborta porque sin cursor no se
// puede continuar.
func (r *RedisCacheInvalidator) InvalidateDiscounts(ctx context.Context) (int, error) {
	if r == nil || r.client == nil {
		return 0, nil
	}

	var (
		cursor  uint64
		total   int
		lastErr error
	)

	for {
		keys, next, err := r.client.Scan(ctx, cursor, discountCachePattern, scanBatch).Result()
		if err != nil {
			return total, fmt.Errorf("scan %q: %w", discountCachePattern, err)
		}

		if len(keys) > 0 {
			n, err := r.client.Unlink(ctx, keys...).Result()
			if err != nil {
				// Conservamos el último error pero seguimos: UNLINK puede
				// fallar para algunas keys sin que el resto deba quedar stale.
				lastErr = fmt.Errorf("unlink batch: %w", err)
			}
			total += int(n)
		}

		cursor = next
		if cursor == 0 {
			break
		}
	}

	return total, lastErr
}
