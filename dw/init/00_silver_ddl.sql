CREATE SCHEMA silver;

CREATE TABLE silver.oil_price (
    date        DATE PRIMARY KEY,
    close_usd   NUMERIC(10,2),
    extracted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE silver.wiki_views (
    date        DATE PRIMARY KEY,  -- disimpan sebagai tanggal hari pertama bulan
    views       INT NOT NULL,
    extracted_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE silver.ev_trends (
    date        DATE NOT NULL,
    keyword     VARCHAR(50) NOT NULL,
    interest    NUMERIC(5,1),
    extracted_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (date, keyword)
);

CREATE TABLE silver.dim_date (
    date        DATE PRIMARY KEY,
    year        INT NOT NULL,
    month       INT NOT NULL,
    month_name  VARCHAR(10),
    quarter     INT NOT NULL,
    day_of_week INT,
    is_weekend  BOOLEAN
);

-- Isi dengan generate_series() sekali saja
INSERT INTO silver.dim_date (date, year, month, month_name, quarter, day_of_week, is_weekend)
SELECT
    d::DATE AS date,
    EXTRACT(YEAR FROM d)::INT AS year,
    EXTRACT(MONTH FROM d)::INT AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    EXTRACT(ISODOW FROM d)::INT AS day_of_week,
    (EXTRACT(ISODOW FROM d) IN (6, 7)) AS is_weekend
FROM generate_series(
    '2000-01-01'::DATE,
    '2050-12-31'::DATE,
    '1 day'::INTERVAL
) AS d;