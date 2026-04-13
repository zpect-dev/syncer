ALTER TABLE art ADD COLUMN IF NOT EXISTS cat_des TEXT NOT NULL DEFAULT '';

-- Update initially if data exists in another node, not needed if starting from scratch, but good to have
UPDATE art a SET cat_des = COALESCE(c.cat_des, '') FROM cat_art c WHERE a.co_cat = c.co_cat;

ALTER TABLE art DROP COLUMN IF EXISTS search_vector;

ALTER TABLE art ADD COLUMN search_vector tsvector GENERATED ALWAYS AS (
    setweight(to_tsvector('spanish', coalesce(co_art, '')), 'A') ||
    setweight(to_tsvector('spanish', coalesce(art_des, '')), 'B') ||
    setweight(to_tsvector('spanish', coalesce(cat_des, '')), 'C') ||
    setweight(to_tsvector('spanish', coalesce(campo4, '')), 'D')
) STORED;

CREATE INDEX IF NOT EXISTS idx_art_search_vector ON art USING GIN(search_vector);
