CREATE TABLE IF NOT EXISTS desc_ptc (
    co_prov  VARCHAR(30)    NOT NULL,
    hasta1   DECIMAL(18,5)  DEFAULT 0,
    hasta2   DECIMAL(18,5)  DEFAULT 0,
    hasta3   DECIMAL(18,5)  DEFAULT 0,
    hasta4   DECIMAL(18,5)  DEFAULT 0,
    hasta5   DECIMAL(18,5)  DEFAULT 0,
    porc1    DECIMAL(18,5)  DEFAULT 0,
    porc2    DECIMAL(18,5)  DEFAULT 0,
    porc3    DECIMAL(18,5)  DEFAULT 0,
    porc4    DECIMAL(18,5)  DEFAULT 0,
    porc5    DECIMAL(18,5)  DEFAULT 0,

    PRIMARY KEY (co_prov, tipo_cli),

    CONSTRAINT fk_desc_ptc_cat_art FOREIGN KEY (co_prov) REFERENCES cat_art(co_cat) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS desc_prov (
    co_prov  VARCHAR(30)    NOT NULL,
    co_cli   VARCHAR(50)    NOT NULL,
    porc1    DECIMAL(18,5)  DEFAULT 0,

    PRIMARY KEY (co_prov, co_cli),

    CONSTRAINT fk_desc_prov_cat_art FOREIGN KEY (co_prov) REFERENCES cat_art(co_cat) ON DELETE RESTRICT,
    CONSTRAINT fk_desc_prov_clientes FOREIGN KEY (co_cli) REFERENCES clientes(co_cli) ON DELETE RESTRICT
);
