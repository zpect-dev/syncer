CREATE TABLE almacen (
    co_alma VARCHAR(6) PRIMARY KEY,
    alma_des VARCHAR(60) NOT NULL
);

CREATE TABLE sub_alma (
    co_sub VARCHAR(6) PRIMARY KEY,
    des_sub VARCHAR(60) NOT NULL,
    co_alma VARCHAR(6) NOT NULL,

    CONSTRAINT fk_co_alma_almacen FOREIGN KEY (co_alma) REFERENCES almacen(co_alma)
);

CREATE TABLE st_almac (
    co_alma VARCHAR(6) NOT NULL,
    co_art VARCHAR(30) NOT NULL,
    stock_act DECIMAL(18, 5) NOT NULL,
    sstock_act DECIMAL(18, 5) NOT NULL,
    stock_com DECIMAL(18, 5) NOT NULL,
    sstock_com DECIMAL(18, 5) NOT NULL,
    stock_lle DECIMAL(18, 5) NOT NULL,
    sstock_lle DECIMAL(18, 5) NOT NULL,
    stock_des DECIMAL(18, 5) NOT NULL,
    sstock_des DECIMAL(18, 5) NOT NULL,

    PRIMARY KEY (co_alma, co_art),

    CONSTRAINT fk_co_alma_sub_alma FOREIGN KEY (co_alma) REFERENCES sub_alma(co_sub),
    CONSTRAINT fk_co_art_art FOREIGN KEY (co_art) REFERENCES art(co_art)
);