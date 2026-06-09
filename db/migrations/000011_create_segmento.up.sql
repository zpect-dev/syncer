BEGIN;

CREATE TABLE IF NOT EXISTS segmento (
    co_seg  VARCHAR(30)   NOT NULL PRIMARY KEY,
    seg_des VARCHAR(255)  NOT NULL
);

ALTER TABLE clientes ADD COLUMN IF NOT EXISTS co_seg VARCHAR(30);

ALTER TABLE clientes
    DROP CONSTRAINT IF EXISTS fk_clientes_segmento;

ALTER TABLE clientes
    ADD CONSTRAINT fk_clientes_segmento
    FOREIGN KEY (co_seg) REFERENCES segmento(co_seg) ON DELETE SET NULL;

COMMIT;
