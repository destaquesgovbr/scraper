# Plano: Otimizar scraper com "known URL fence" — parar cedo sem depender de datetime

## Contexto

Hoje o scraper processa **todos os artigos do dia** na listagem, mesmo que já tenha coletado a maioria deles em execuções anteriores. Com DAGs rodando a cada 10 minutos, isso significa re-processar dezenas de artigos já conhecidos. O gargalo é o **fetch da página do artigo** (request HTTP + parse HTML), que acontece para cada item da listagem — mesmo duplicatas.

A condição de parada atual depende de comparar **datas** (`datetime.date`) extraídas da listagem. Mas a listagem gov.br só tem data sem hora, então o scraper processa o dia inteiro até encontrar um artigo de um dia anterior.

## Abordagem: "Known URL Fence"

**Ideia:** Antes de iniciar o scraping, consultar o banco para obter as URLs de artigos recentes da agência. Usar esse conjunto como "cerca" durante a iteração:

1. Se a URL do item na listagem **NÃO está** no conjunto → é novo → buscar página do artigo normalmente
2. Se a URL **ESTÁ** no conjunto → pular o fetch (já temos esse artigo)
3. Se encontrar **N consecutivos** conhecidos → **STOP** (tudo abaixo também é conhecido)

### Vantagens sobre datetime

| Aspecto | Datetime | Known URL Fence |
|---------|----------|-----------------|
| Depende de parsing de data | Sim | Não |
| Lida com timezone | Precisa normalizar | N/A |
| Artigo sem data | Não consegue parar | Para normalmente |
| Artigo re-publicado | Pode reprocessar | Detecta pela URL |
| Custo | Zero (não consulta DB) | 1 SELECT por agência |

### Fluxo otimizado

```
1. ScrapeManager consulta DB: "URLs dos últimos 200 artigos da agência X"
   → known_urls = {"https://gov.br/mec/noticia-a", "https://gov.br/mec/noticia-b", ...}

2. WebScraper itera listagem:
   Artigo 1: url=".../noticia-nova"  → NÃO conhecida → fetch + processo ✓
   Artigo 2: url=".../noticia-b"     → CONHECIDA → skip (sem fetch)
   Artigo 3: url=".../noticia-a"     → CONHECIDA (2 consecutivos)
   Artigo 4: url=".../noticia-velha" → CONHECIDA (3 consecutivos) → STOP ■
```

### Limiar de parada (N=3)

`N=3` consecutivos conhecidos é um bom default:
- Protege contra o caso raro de uma notícia ser deletada e re-inserida (furo de 1)
- Não espera demais (3 artigos = ~3 requests poupados no máximo)
- Configurável caso precise ajustar

## Ordem de implementação: testes primeiro (TDD)

### Fase 1 — Testes (RED)

Escrever testes que definem o comportamento esperado antes de implementar.

#### `tests/unit/test_known_url_fence.py`

Testes para a lógica do WebScraper com known_urls:

1. **`test_new_article_is_processed`** — artigo com URL desconhecida é processado normalmente
2. **`test_known_article_is_skipped`** — artigo com URL conhecida é pulado (sem fetch)
3. **`test_consecutive_known_urls_stop`** — 3 consecutivos conhecidos → retorna False (stop)
4. **`test_new_article_resets_consecutive_counter`** — artigo novo entre conhecidos reseta o contador
5. **`test_empty_known_urls_processes_all`** — sem known_urls, comportamento idêntico ao atual
6. **`test_known_url_below_threshold_continues`** — 1-2 conhecidos consecutivos → continua (return True)

#### `tests/unit/test_postgres_manager_urls.py`

Testes para `get_recent_urls`:

1. **`test_get_recent_urls_returns_set`** — retorna set de strings
2. **`test_get_recent_urls_filters_by_agency`** — só retorna URLs da agência especificada
3. **`test_get_recent_urls_respects_limit`** — respeita o limite
4. **`test_get_recent_urls_empty_for_unknown_agency`** — agência desconhecida retorna set vazio

#### `tests/unit/test_api.py` (atualizar)

5. **`test_scrape_agencies_passes_known_urls`** — verificar que ScrapeManager consulta URLs antes de scraping

### Fase 2 — Implementação (GREEN)

Implementar o código para fazer os testes passarem.

#### 1. `src/govbr_scraper/storage/postgres_manager.py` — novo método

```python
def get_recent_urls(self, agency_key: str, limit: int = 200) -> set[str]:
    """Retorna URLs dos artigos mais recentes de uma agência."""
    query = """
        SELECT url FROM news
        WHERE agency_key = %s AND url IS NOT NULL
        ORDER BY published_at DESC
        LIMIT %s
    """
    conn = self.pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(query, (agency_key, limit))
            return {row[0] for row in cur.fetchall()}
    finally:
        self.pool.putconn(conn)
```

#### 2. `src/govbr_scraper/storage/storage_adapter.py` — expor método

```python
def get_recent_urls(self, agency_key: str, limit: int = 200) -> set[str]:
    """Retorna URLs recentes de uma agência para otimização de scraping."""
    return self.postgres.get_recent_urls(agency_key, limit)
```

#### 3. `src/govbr_scraper/scrapers/scrape_manager.py` — consultar antes de scraping

Alterar a criação de webscrapers para consultar URLs conhecidas:

```python
webscrapers = []
for agency_name, url in agency_urls.items():
    try:
        known_urls = self.storage.get_recent_urls(agency_name)
    except Exception:
        known_urls = set()  # Fallback: sem otimização
    webscrapers.append(
        (agency_name, WebScraper(min_date, url, max_date=max_date, known_urls=known_urls))
    )
```

#### 4. `src/govbr_scraper/scrapers/webscraper.py` — lógica de fence

**Construtor** — aceitar `known_urls`:
```python
def __init__(self, min_date, base_url, max_date=None, known_urls=None):
    # ... existente ...
    self.known_urls = known_urls or set()
    self._consecutive_known = 0
    self.KNOWN_URL_STOP_THRESHOLD = 3
```

**`extract_news_info`** — inserir check de known URL **após** check de data e **antes** do fetch:
```python
# Known URL fence: pular artigos já conhecidos
if url in self.known_urls:
    self._consecutive_known += 1
    logging.info(f"Skipping known article ({self._consecutive_known}/{self.KNOWN_URL_STOP_THRESHOLD}): {url}")
    if self._consecutive_known >= self.KNOWN_URL_STOP_THRESHOLD:
        logging.info(f"Known URL fence: {self.KNOWN_URL_STOP_THRESHOLD} consecutive known articles. Stopping.")
        return False
    return True  # Skip mas continua

# Reset contador se encontrar artigo novo
self._consecutive_known = 0

# Fetch da página do artigo (só para artigos novos)
content, image_url, published_dt, ... = self.get_article_content(url)
```

### Fase 3 — Refine (REFACTOR)

- Verificar que todos os testes passam (antigos + novos)
- `astro dev parse` — 157 DAGs, 0 erros

## Backward compatibility

- Se `known_urls` não for passado (ou for `set()`), o comportamento é idêntico ao atual
- A comparação por data continua funcionando como fallback
- Se a query ao DB falhar, `known_urls = set()` e o scraper opera normalmente

## Verificação

1. `poetry run pytest tests/` — todos os testes passam (antigos + novos)
2. `astro dev parse` — 157 DAGs, 0 erros
3. Teste manual: rodar scraper para uma agência e verificar nos logs:
   - `"Skipping known article (1/3)"` aparece para artigos já processados
   - `"Known URL fence: 3 consecutive known articles. Stopping."` aparece quando para cedo
4. Comparar quantidade de fetches antes e depois (esperar redução significativa em execuções recorrentes)
5. Na primeira execução (DB vazio ou agência nova), comportamento idêntico ao atual
