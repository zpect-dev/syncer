CREATE TABLE IF NOT EXISTS desc_ptc (
    co_prov  VARCHAR(30)    NOT NULL,
    tipo_cli VARCHAR(50)    NOT NULL,
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
    co_us_in VARCHAR(30),
    fe_us_in TIMESTAMP,
    co_sucu  VARCHAR(30),
    PRIMARY KEY (co_prov, tipo_cli)
);

CREATE TABLE IF NOT EXISTS desc_prov (
    co_prov  VARCHAR(30)    NOT NULL,
    co_cli   VARCHAR(50)    NOT NULL,
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
    co_us_in VARCHAR(30),
    fe_us_in TIMESTAMP,
    co_sucu  VARCHAR(30),
    PRIMARY KEY (co_prov, co_cli)
);
