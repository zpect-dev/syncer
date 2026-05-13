BEGIN;

-- 1. El search_vector depende de cat_des, hay que dropearlo antes de eliminar la columna.
DROP INDEX IF EXISTS idx_art_search_vector;
ALTER TABLE art DROP COLUMN IF EXISTS search_vector;

-- 2. Eliminar la columna desnormalizada antigua.
ALTER TABLE art DROP COLUMN IF EXISTS cat_des;

-- 3. Crear la nueva columna desnormalizada para el proveedor.
ALTER TABLE art ADD COLUMN IF NOT EXISTS prov_des TEXT NOT NULL DEFAULT '';

-- 4. Backfill inicial: traer prov_des desde la tabla maestra prov vía co_prov.
UPDATE art a
SET prov_des = COALESCE(p.prov_des, '')
FROM prov p
WHERE a.co_prov = p.co_prov;

-- 5. Recrear el search_vector usando prov_des en lugar de cat_des (mismo peso 'C').
ALTER TABLE art ADD COLUMN search_vector tsvector GENERATED ALWAYS AS (
    setweight(to_tsvector('spanish', coalesce(co_art, '')), 'A') ||
    setweight(to_tsvector('spanish', coalesce(art_des, '')), 'B') ||
    setweight(to_tsvector('spanish', coalesce(prov_des, '')), 'C') ||
    setweight(to_tsvector('spanish', coalesce(campo4, '')), 'D')
) STORED;

CREATE INDEX IF NOT EXISTS idx_art_search_vector ON art USING GIN(search_vector);

COMMIT;
