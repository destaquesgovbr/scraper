-- Tabela para rastrear resultados de execução do scraper por agência.
-- Permite detecção de falhas consecutivas, agências sem notícias, e relatórios de cobertura.

CREATE TABLE IF NOT EXISTS scrape_runs (
    id SERIAL PRIMARY KEY,
    agency_key VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,              -- 'success', 'error'
    error_category VARCHAR(50),               -- ErrorCategory enum value
    error_message TEXT,
    articles_scraped INTEGER DEFAULT 0,
    articles_saved INTEGER DEFAULT 0,
    execution_time_seconds REAL,
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Consultas por agência ordenadas por data (falhas consecutivas, histórico)
CREATE INDEX IF NOT EXISTS idx_scrape_runs_agency_scraped
    ON scrape_runs (agency_key, scraped_at DESC);

-- Consultas por status (cobertura, relatórios)
CREATE INDEX IF NOT EXISTS idx_scrape_runs_status
    ON scrape_runs (status, scraped_at DESC);
