# DestaquesGovBr Scraper

Scraper standalone para notícias de sites governamentais brasileiros (~155 agências gov.br + EBC). Suporta três tipos de scraper (HTML, EBC, Plone6 API), publica eventos Pub/Sub após inserção, e inclui sistema completo de monitoramento com alertas. O sistema inclui 3 DAGs de manutenção.

## Visão Geral

O scraper coleta notícias publicadas em sites gov.br e da EBC (Agência Brasil, TV Brasil, etc.), extraindo título, conteúdo, data de publicação e metadados. Os dados são persistidos no PostgreSQL (upsert por unique_id, deduplicação por URL). Após persistência, eventos são publicados no Pub/Sub (`dgb.news.scraped`) para consumo downstream.

A coleta é orquestrada por **DAGs Airflow** (Airflow 3.1.8 próprio, na VM `destaquesgovbr-airflow`; o Cloud Composer foi desligado em 18/11/2026) que chamam a **API FastAPI** hospedada no Cloud Run. O sistema inclui 3 DAGs de manutenção para monitoramento, alertas e retenção de dados, mais o utilitário `notify.py`.

## Arquitetura

```
DAGs Airflow (Airflow próprio, VM destaquesgovbr-airflow)
│   ├── scrape_agencies (06h–22h de 2 em 2 h, 1 task mapeada por agência + summarize)
│   ├── scrape_ebc (06h–22h de 2 em 2 h)
│   ├── monitor_scraping_health (06h30–22h30 de 2 em 2 h)
│   ├── cleanup_old_scrape_runs (diário 03h)
│   └── scraper_coverage_report (diário 08h)
│
│   [IAM: google.oauth2.id_token → Bearer token]
│
└── HTTP POST → Cloud Run API (FastAPI)
        ├── POST /scrape/agencies → ScrapeManager
        │       ├── WebScraper (HTML) ──┐
        │       └── Plone6APIScraper ───┤
        ├── POST /scrape/ebc → EBCScrapeManager
        │       └── EBCWebScraper ──────┤
        ├── POST /verify/integrity      │
        │       └── integrity.service ──┤
        └── GET /health                 │
                                        ▼
                              ┌─── PostgreSQL ───┐
                              │  news            │
                              │  agencies        │
                              │  themes          │
                              │  scrape_runs     │
                              └──────────────────┘
                                        │
                                        ▼
                              Pub/Sub (dgb.news.scraped)
                              [opcional, graceful degradation]

    Credenciais: DATABASE_URL env → Secret Manager fallback
```

## Estrutura do Repositório

```
scraper/
├── src/govbr_scraper/
│   ├── api.py                         # FastAPI (Cloud Run) — 4 endpoints
│   ├── scrapers/
│   │   ├── webscraper.py              # Scraper HTML gov.br (~1382 linhas)
│   │   ├── scrape_manager.py          # Coordenador gov.br (routing por scraper_type)
│   │   ├── ebc_webscraper.py          # Scraper EBC (~624 linhas)
│   │   ├── ebc_scrape_manager.py      # Coordenador EBC
│   │   ├── plone6_api_scraper.py      # Scraper Plone6 API (~394 linhas)
│   │   ├── content_hash.py            # Deduplicação: normalize + SHA256[:16]
│   │   ├── unique_id.py              # IDs legíveis: slug + sufixo hex
│   │   ├── yaml_config.py            # Utilitário de carga YAML
│   │   └── config/
│   │       ├── site_urls.yaml         # ✓ Arquivo fonte (sempre edite este)
│   │       └── ebc_urls.yaml          # Endpoints EBC
│   ├── storage/
│   │   ├── storage_adapter.py         # Abstração de persistência
│   │   ├── postgres_manager.py        # PostgreSQL + connection pooling
│   │   └── event_publisher.py         # Pub/Sub (dgb.news.scraped)
│   ├── models/
│   │   ├── news.py                    # Modelos: News, Agency, Theme
│   │   └── monitoring.py             # ErrorCategory, is_retryable, ScrapeRunResult
│   ├── integrity/
│   │   ├── checker.py                 # Verificação de imagens e conteúdo
│   │   └── service.py                # Batch verification com deadline
│   └── monitoring/
│       ├── health_checks.py           # Queries SQL de saúde
│       └── structured_log.py         # Log estruturado + record_scrape_run
├── dags/
│   ├── scrape_agencies.py             # 1 DAG, 1 task mapeada por agência ativa + summarize
│   ├── scrape_ebc.py                  # 1 DAG para sites EBC
│   ├── monitor_scraping_health.py     # Monitoramento: sem execução + falhas + stale
│   ├── cleanup_old_scrape_runs.py     # Retenção: delete > N dias
│   ├── scraper_coverage_report.py     # Relatório diário + alerta cobertura
│   ├── notify.py                      # Utilitário: Telegram > Pub/Sub > webhook > log
│   └── config/
│       ├── README.md
│       └── site_urls.yaml             # → Cópia (sincronize manualmente)
├── docker/
│   └── Dockerfile
├── docs/
│   └── runbook.md                     # Procedimentos operacionais
├── tests/
│   ├── unit/                          # 30 arquivos de teste
│   └── integration/                   # 3 arquivos (requer DB real)
├── pyproject.toml
└── .github/workflows/
    ├── tests.yaml                     # pytest em PRs
    ├── scraper-api-deploy.yaml        # Build + deploy Cloud Run (push main)
    └── composer-deploy-dags.yaml      # Deploy DAGs → bucket do Airflow próprio (push main; nome antigo)
```

### ⚠️ Importante sobre site_urls.yaml

O arquivo `site_urls.yaml` existe em **dois locais** que devem ser mantidos sincronizados:

- **Sempre edite:** `src/govbr_scraper/scrapers/config/site_urls.yaml` (fonte)
- **Depois copie para:** `dags/config/site_urls.yaml` (cópia)

**Comando para sincronizar:**
```bash
cp src/govbr_scraper/scrapers/config/site_urls.yaml dags/config/site_urls.yaml
```

**Por que dois arquivos?**
- DAGs do Airflow são autocontidas e não importam código Python da API
- API Cloud Run empacota o arquivo de `src/` na imagem Docker
- Mantém separação de responsabilidades entre orquestração (DAGs) e execução (API)

**Validação:** O CI valida automaticamente que os arquivos estão sincronizados via `test_config_sync.py`. PRs com arquivos dessincronizados são bloqueados.

## Tipos de Scraper

O campo `scraper_type` no `site_urls.yaml` determina qual scraper é usado para cada agência gov.br:

| Tipo | Classe | Uso |
|------|--------|-----|
| `html` (default) | `WebScraper` | Sites gov.br com listagem HTML padrão |
| `plone6_api` | `Plone6APIScraper` | Sites Plone 6 com Volto (React SPA) via `++api++` |

**EBC é uma pipeline separada** — não usa `scraper_type`. Tem DAG própria (`scrape_ebc.py`), manager próprio (`EBCScrapeManager`), scraper próprio (`EBCWebScraper`) e config YAML próprio (`ebc_urls.yaml`).

**Quando usar `plone6_api`:** Agências que migraram para Plone 6 renderizam conteúdo via JavaScript (React/Volto). O HTML da página não contém os artigos — é necessário usar a REST API (`++api++`) do Plone para obter os dados. Identificar pela ausência de listagem de notícias no HTML e presença de `++api++` no site.

## Features

### Known URL Fence

Otimização que evita re-processar artigos já coletados. Antes de iniciar o scraping, o `ScrapeManager` consulta as últimas 200 URLs da agência via `get_recent_urls()`. Durante o scraping, URLs já conhecidas são contadas consecutivamente. Ao atingir **3 URLs conhecidas consecutivas**, o scraper para — assumindo que o restante já foi coletado em execuções anteriores.

### Itens pulados (não gravados)

O `ScrapeManager` descarta antes de persistir, loga cada item e soma em `articles_skipped` (resposta da API e linha `scrape_skipped agency=… invalid_content=N redated=M`):

- **Conteúdo inválido** (scraper#65, `content_validation.py`): `"Error retrieving content"`, tela de login do Plone (redirect para `require_login`/`acl_users` ou texto "necessário autenticar para visualizar") e conteúdo vazio. Log `skipped_invalid_content agency=… reason=<error_content|login_page|empty_content> url=…`. A URL não é gravada, então fica fora de `known_urls` e o artigo é tentado de novo enquanto estiver na janela.
- **Redatados** (`redated.py`): conteúdo antigo republicado com data nova, como na onda pós-defeso de 26-29/10/2026. É avaliado antes de baixar o artigo e antes da cerca de URLs conhecidas. Plone 6: `created` 30 dias ou mais antes do `effective`. HTML: pasta `/AAAA/MM/` ou `/AAAA/<mês>/` da URL anterior ao mês da listagem por mais de 1 mês. Log `skipped_redated agency=… signal=<plone_created|url_folder> … url=…`. Com `SCRAPER_REDATED_FILTER=observe` (env da API), o filtro só loga (`redated_observed`) e mantém o item. **Exceção, conteúdo retido no defeso:** item com `created` (Plone 6) ou pasta de data (HTML) dentro de `RETAINED_CONTENT_WINDOWS` (defeso eleitoral 2026: 04/07 a 25/10) não é pulado (log `redated_kept_retained`). Ele ficou privado durante o defeso e foi publicado pela primeira vez depois, então a data nova é a da primeira publicação. Para um defeso futuro, acrescente a janela em `redated.py`.

### Content Hash

Deduplicação por conteúdo via `compute_content_hash(title, content)`:
1. Normaliza texto (NFKD → ASCII, lowercase, remove pontuação, colapsa espaços)
2. Concatena `título\nconteúdo` normalizado
3. SHA256 dos primeiros 16 caracteres hex

Usado para detectar artigos duplicados com URLs diferentes e para verificação de integridade.

### Unique ID

IDs legíveis no formato `{slug}_{suffix}` (ex: `governo-anuncia-programa_a3f2e1`):
- Slug: título slugificado, máx 100 chars, truncado em fronteira de palavra
- Suffix: 6 chars hex de MD5(agency + date + title) para unicidade determinística

### Classificação de Erros

`ErrorCategory` (StrEnum) classifica falhas de scraping para monitoramento e decide se a DAG repete:

| Categoria | Padrões detectados | Retry |
|-----------|-------------------|-------|
| `NETWORK_ERROR` | HTTP 408, 429; timeout, connection error/refused, "failed to fetch page"; "erro de rede" (Plone 6: "Erro de rede ao acessar API de ...") | passageiro |
| `ANTI_BOT` | anti-bot, JS challenge, cloudflare | permanente |
| `EMPTY_CONTENT` | no news found | passageiro |
| `URL_BROKEN` | HTTP 404, 410 e qualquer outro 4xx que não seja 401/403/408/429 (400, 405...) | permanente |
| `ACCESS_DENIED` | HTTP 401, 403, listagem que redireciona para o login do Plone ("login redirect") | permanente |
| `HTML_CHANGED` | no articles found (estrutura mudou) | permanente |
| `UNKNOWN` | fallback | passageiro |

O status HTTP vem antes da mensagem; 5xx cai na mensagem. Os managers (gov.br e EBC) classificam com `classify_exception(e)`, que usa o `error_category` do `FallbackFailedError` ou o `http_status` do `PermanentHTTPError` quando houver, e a mesma categoria vai para `scrape_runs.error_category` e para `errors[].category` da API.

**Primário e fallback falharam** (`FallbackFailedError`, mensagem `Both scrapers failed. Primary: No articles found... Fallback: ...`): a mensagem leria sempre `html_changed`, então o `ScrapeManager` fixa a categoria. Falha de rede do Plone 6 (`network_error`: 5xx, 408/429 depois dos retries, timeout, DNS) vale como `network_error`, passageira, porque um retry pode raspar pelo fallback. Qualquer outra (404 da API, como a mec, ou resposta não-JSON, como a antaq: o site não serve a API do Plone 6) fica `html_changed`, permanente.

`PERMANENT_ERROR_CATEGORIES` = `url_broken`, `access_denied`, `html_changed`, `anti_bot`; `is_retryable(categoria)` é False para elas e para `unknown_agency` (agência fora do YAML ou inativa, `UnknownAgencyError` do `yaml_config`; só existe na resposta da API, sem linha em `scrape_runs`), True para o resto e para erro sem categoria.

### Política HTTP (falhar rápido)

`scrapers/http_policy.py` vale para WebScraper, Plone6APIScraper e EBCWebScraper (infra#223):

- 4xx, menos 408 e 429: `PermanentHTTPError` (`scrapers/errors.py`, subclasse de `ScrapingError` e não de `RequestException`) na primeira resposta, sem retry.
- Listagem que redireciona para o login do Plone (`is_login_redirect`): `LoginRedirectError` (subclasse de `PermanentHTTPError`), sem retry e sem fallback.
- 5xx, 408, 429, timeout e conexão: até 3 tentativas (`tries=3, delay=1, backoff=2`, jitter de 0 a 1 s; ~3,5 s de espera no total).
- `timeout=(5, 15)` (connect, read).

Antes, todo `RequestException` era repetido 5 vezes (`delay=2, backoff=3`, jitter de 1 a 3 s), inclusive 404 e 401: ~116 s de espera por chamada, 86% do tempo faturável do Cloud Run. No EBC o `@retry` envolvia um `try/except` que devolvia None, então nunca repetia; agora `fetch_page` ainda devolve None na falha, mas repete o erro transitório.

### Fallback Automático (HTML_CHANGED)

Quando WebScraper falha com erro `HTML_CHANGED` (estrutura HTML mudou), o sistema tenta automaticamente Plone6APIScraper como fallback. Este mecanismo é útil para agências que migraram para Plone 6 Volto mas ainda estão configuradas como `scraper_type: html`.

**Condições para acionar:**
- Scraper primário é WebScraper (não Plone6)
- Erro classificado como HTML_CHANGED ("No articles found on first page")
- Fallback disponível (instanciado apenas para `scraper_type: html`)

**Não aciona em:**
- NETWORK_ERROR, ANTI_BOT, URL_BROKEN, ACCESS_DENIED, EMPTY_CONTENT
- `PermanentHTTPError` (4xx permanente ou redirect para login), qualquer que seja a categoria
- Plone6 → WebScraper (tecnicamente inútil para SPAs)

**Campos de monitoramento adicionados:**

A feature de fallback automático adiciona **1 campo** à tabela `scrape_runs`:

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `fallback_triggered` | bool | Se fallback foi acionado (default FALSE) |

**Campos adicionais logados mas não persistidos no PostgreSQL:**

Os campos abaixo estão disponíveis nos logs estruturados (Cloud Logging) para debugging, mas não são persistidos no banco por serem redundantes ou inferíveis:

- `primary_scraper`: Inferível de `site_urls.yaml` configuração (`scraper_type: html` → webscraper, `scraper_type: plone6_api` → plone6_api)
- `fallback_scraper`: Constante `"plone6_api"` (único fallback implementado atualmente)
- `fallback_success`: Inferível via query: `status='success' AND fallback_triggered=true`

**Log de recomendação:** Se fallback bem-sucedido, o sistema loga:
```
{agency}: Plone6 API fallback SUCCEEDED. Found {N} articles.
RECOMMENDATION: Update site_urls.yaml to set scraper_type: plone6_api
```

**Overhead:** Zero em agências funcionais. Fallback só é executado quando WebScraper falha com HTML_CHANGED.

### Pub/Sub Events

Após persistir artigos, `EventPublisher` publica no tópico `dgb.news.scraped` os artigos novos e os existentes (casados por `(agency_key, url)`) cujo `content_hash` mudou **ou que ainda não têm tema** (`most_specific_theme_id IS NULL`, o mesmo critério do `is_already_enriched` do enrichment-worker). Re-scrape de artigo já enriquecido e sem mudança de conteúdo atualiza a linha (e conta em `articles_saved`), mas não republica: o enrichment-worker pularia (já enriquecido) e o bronze-writer reescreveria o mesmo objeto.

**Por que o artigo sem tema continua sendo republicado:** a republicação do re-scrape é a única nova tentativa automática de um enriquecimento que falhou (throttling/timeout do Bedrock, modelo fora, erro de DB, publish perdido). O enrichment-worker responde 200/ACK até em exceção e em `classification_failed` (o `max_delivery_attempts` da assinatura nunca atua), a DAG de reconciliação `enrich_news_llm` foi removida (data-science 8f10273) e não há Cloud Scheduler. Custo enquanto o modelo estiver fora: 1 chamada combinada por artigo sem tema a cada re-scrape (a cada execução da DAG, de 2 em 2 h, limitado à janela de re-scrape; o NER roda no máximo 1x por uid). Só dá para suprimir também esse caso depois que existir um retry agendado e limitado (ex.: `reenrich_combined_window --select null-theme` com governador) ou o worker devolver 5xx em erro transitório.

```json
{
  "unique_id": "governo-anuncia-programa_a3f2e1",
  "agency_key": "mec",
  "published_at": "2026-05-18T14:30:00-03:00",
  "scraped_at": "2026-05-18T17:45:12.123456+00:00"
}
```

Atributos da mensagem: `trace_id` (UUID por batch), `event_version` ("1.0").

**Graceful degradation:** Se `PUBSUB_TOPIC_NEWS_SCRAPED` não estiver definido ou o client falhar na inicialização, todos os publishes são no-ops. Artigos já estão persistidos no PostgreSQL.

## API Endpoints

A API roda no Cloud Run e é chamada pelas DAGs Airflow.

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/scrape/agencies` | Raspa sites gov.br (aceita lista de agências, datas) |
| `POST` | `/scrape/ebc` | Raspa sites EBC (Agência Brasil, TV Brasil) |
| `POST` | `/verify/integrity` | Verifica integridade de artigos (imagens, conteúdo) |
| `GET` | `/health` | Health check |

### Request: `/scrape/agencies`

```json
{
  "start_date": "2025-01-01",
  "end_date": "2025-01-02",
  "agencies": ["mec", "mds"],
  "allow_update": false,
  "sequential": true
}
```

### Request: `/scrape/ebc`

```json
{
  "start_date": "2025-01-01",
  "end_date": "2025-01-02",
  "agencies": ["agencia_brasil"],
  "allow_update": false,
  "sequential": true
}
```

### Response: `/scrape/agencies` e `/scrape/ebc`

```json
{
  "status": "partial",
  "start_date": "2026-11-19",
  "end_date": "2026-11-19",
  "articles_scraped": 12,
  "articles_saved": 12,
  "articles_new": 3,
  "articles_skipped": 0,
  "agencies_processed": ["mec"],
  "errors": [
    {"agency": "antaq", "error": "No articles found on first page of antaq ...",
     "category": "html_changed", "retryable": false}
  ],
  "message": "Completed with 1 error(s)"
}
```

- **Status HTTP inalterados:** 200 `completed`, 207 `partial`, 500 `failed` (todas as agências falharam) ou exceção (`{"detail": ...}`), 422 payload inválido.
- **`articles_saved`:** linhas gravadas em `news`, inseridas + atualizadas (re-scrape casado por `(agency_key, url)` e, com `allow_update`, o `ON CONFLICT DO UPDATE`). Mantém o significado de sempre.
- **`articles_new`:** só as linhas de fato inseridas: o `RETURNING` do `ON CONFLICT DO NOTHING` ou, com `allow_update=True`, as linhas com `xmax = 0` no `RETURNING unique_id, (xmax = 0) AS inserted` do `DO UPDATE` (o retry do `UniqueViolation` conta igual). `PostgresManager.insert` devolve `InsertResult(saved, articles, new)` e `StorageAdapter.insert`, `InsertCounts(saved, new)`. Em nov/2026: ~242 notícias novas/dia, contra 2.175 "salvas"/dia só na agencia_brasil.
- **`errors[].category`:** a mesma gravada em `scrape_runs.error_category`, ou `unknown_agency`; `null` em erro sem classificação (ex.: erro de config).
- **`errors[].retryable`:** `false` em erro permanente; as DAGs falham sem retry. Default `true` (API antiga, sem o campo, é tratada como passageira pelas DAGs).

### Request: `/verify/integrity`

```json
{
  "articles": [
    {
      "unique_id": "governo-anuncia-programa_a3f2e1",
      "url": "https://www.gov.br/mec/pt-br/...",
      "image_url": "https://www.gov.br/mec/pt-br/.../image.png",
      "content_hash": "a1b2c3d4e5f67890",
      "source_etag": "\"abc123\"",
      "check_content": true
    }
  ]
}
```

**SSRF mitigation:** URLs são validadas contra allowlist de domínios aceitos:
- `https://www.gov.br/`
- `https://agenciabrasil.ebc.com.br/`
- `https://imagens.ebc.com.br/`
- `https://memoria.ebc.com.br/`
- `https://tvbrasil.ebc.com.br/`
- `https://live.staticflickr.com/`
- `https://storage.googleapis.com/destaquesgovbr-thumbnails/`

**Batch com deadline:** Verificação paralela (até 20 workers). Deadline de 100s (< timeout HTTP de 120s da DAG). Artigos pendentes ao estourar o deadline são marcados como `image_status=timeout`.

**Response:** `{results: [...], summary: {total, images_ok, images_broken, images_timeout, content_unchanged, content_changed, ...}}`

## DAGs Airflow

| DAG | Schedule (BRT) | Descrição |
|-----|----------|-----------|
| `scrape_agencies` | `0 6-22/2 * * *` (06h–22h, 9x/dia) | 1 task mapeada (`scrape`) por agência gov.br ativa (~147) + `summarize` (única folha) |
| `scrape_ebc` | `0 6-22/2 * * *` | Sites EBC |
| `monitor_scraping_health` | `30 6-22/2 * * *` | Agências sem execução + falhas consecutivas + agências stale |
| `cleanup_old_scrape_runs` | Diário 03h | Delete registros > retention_days |
| `scraper_coverage_report` | Diário 08h | Cobertura 24h + alerta se < threshold |
| `notify.py` | (utilitário) | Telegram > Pub/Sub `dgb.ops.alerts` > webhook > log-only |

Os crons são avaliados no fuso padrão do Airflow: `core.default_timezone = America/Sao_Paulo`, definido por `AIRFLOW__CORE__DEFAULT_TIMEZONE` no env da VM (repo `infra`, `terraform/templates/airflow/airflow.env.tftpl`), como era no Composer.

### DAGs de scraping

Até nov/2026 eram ~147 DAGs `scrape_<agencia>`, cada uma a cada 10 min (96,7% das task runs do Composer, infra#223). Agora é uma DAG só, `scrape_agencies`:
- Uma task mapeada por agência ativa do `dags/config/site_urls.yaml` (lista calculada no parse: `_active_agencies(_read_agencies())`), com a sigla como map index na UI (`map_index_template="{{ agency_key }}"`, a task grava `agency_key` no contexto)
- No máximo `MAX_CONCURRENT_AGENCIES` agências ao mesmo tempo (`max_active_tis_per_dag`), lido no parse do env `SCRAPE_MAX_CONCURRENT_AGENCIES`: padrão 4, inválido vale 4, mínimo 1 (0 pararia a DAG). O limite protege o www.gov.br (as agências estão todas no mesmo host) e espalha a rajada de eventos no pipeline downstream; ~4 a 5 min por execução na VM (eram ~5 a 8 min no Composer)
- `max_active_runs=1`, `catchup=False`
- Cada task chama a API no Cloud Run via HTTP POST (`/scrape/agencies` com uma agência, `allow_update: false`, `sequential: true`), autenticada com ID token IAM (`google.oauth2.id_token`)
- A task lê o corpo antes do status HTTP (sem `raise_for_status`). Erro permanente (todos os `errors[]` da agência com `retryable: false`) levanta `AirflowFailException`: falha numa tentativa só, com agência e categoria na mensagem. Erro passageiro, corpo sem JSON, erro de rede do httpx e API antiga sem `retryable` levantam `AirflowException` e repetem. No sucesso a task devolve `{"agency", "articles_new", "articles_saved"}` (XCom)
- Task final `summarize` (`trigger_rule="all_done"`, `retries=0`), **única folha** (a cor do DagRun é a dela): itera o `LazyXComSequence` da `scrape` (só tem os resultados das tasks com sucesso, compactados; **nunca fatiar**, `results[:]` levanta TypeError no Airflow 3.0.1), deduz as falhas como ativas − ok, loga contagens, agências com falha e notícias novas. Só falha se nenhuma agência funcionou ou se a fração de falhas passar da Variable `scraper_max_failed_ratio` (padrão 0,10). Agência quebrada fica vermelha só na task mapeada dela
- Retry: 2x com delay fixo de 5min (agências e EBC), só para erro passageiro
  - **Nunca use `retry_exponential_backoff` nem `max_retry_delay` em task mapeada (nem em `default_args`):** no Airflow 3.1.0 do Composer, o scheduler lê `task.max_retry_delay` de uma task mapeada em retry, o `MappedOperator` não tem esse atributo e o `AttributeError` derruba o loop do scheduler inteiro (todas as DAGs paradas de 18/11/2026 02:00Z a 09:00Z). Teste de regressão por AST nas duas DAGs
- Timeout: 10min por agência / 30min (EBC)
- Janela: `_scrape_window(logical_date, lookback_hours=SCRAPE_LOOKBACK_HOURS)`, em datas de Brasília (`America/Sao_Paulo`), de 9 horas antes da execução até a execução. A execução das 06h precisa cobrir a madrugada sem scraping desde a das 22h do dia anterior (8 h + 1 h de folga): as das 06h e 08h incluem o dia anterior, as outras só o dia corrente. As listagens datam as notícias em BRT; com a janela em UTC, das 22:00 às 23:59 BRT a data já era o dia seguinte. No Airflow 3 o cron usa o `CronTriggerTimetable`, então a `logical_date` é o próprio horário agendado; disparo manual sem `logical_date` usa o `run_after` do DagRun.
- Params opcionais `start_date`/`end_date` (YYYY-MM-DD, os dois juntos) substituem a janela num disparo manual, para recuperar dados depois de uma queda (runbook, "Agências Sem Execução"). Formato inválido é recusado no disparo (`Param` com `format: date`); só um dos dois ou início depois do fim falham a task sem retry (`AirflowFailException`).
- `scrape_ebc` tem a mesma agenda, a mesma janela, os mesmos params e a mesma leitura da resposta (cópia das funções: cada arquivo de DAG é autocontido, não importa outro módulo de DAG nem o pacote `govbr_scraper`). Uma task para as duas fontes: se algum erro é passageiro, repete; se todos são permanentes, falha sem retry.
- Um DagRun zumbi agora para todas as agências de uma vez, e o monitor acusa na execução seguinte. Com uma DAG por agência, ele parava uma agência só, em silêncio (`scrape_esg`, sem execução desde 11/11/2026).

### DAGs de manutenção

**`monitor_scraping_health`:** Roda 30 min depois de cada execução do scraping (`30 6-22/2 * * *`; nunca na madrugada). Verifica agências ativas (no `dags/config/site_urls.yaml`) sem nenhum `scrape_run` em X horas (`scraper_silent_hours`, default 3 h = cadência de 2 h + 1 h de folga: a task mapeada da agência na `scrape_agencies` não chamou a API, ex. DAG pausada ou DagRun preso em `running`; ver runbook, "Agências Sem Execução"), agências com N falhas consecutivas (threshold 3, janela `scraper_failure_window_hours` de 6 h) ou com erro permanente no `scrape_run` mais recente (a task falha sem retry e grava 1 linha por execução; sem esse critério a agência quebrada levaria 3 execuções para entrar e sairia da seção toda manhã) e agências sem notícia nova em X horas (stale: `COALESCE(articles_new, articles_saved) > 0`, para as linhas anteriores à migration 037 valerem pelo `articles_saved`). Envia alertas via `notify.send_alert()`, sem repetir: o resumo das seções sem execução e falhas consecutivas (só as siglas) é comparado com o do último envio, na Variable `scraper_monitor_last_alert`; reenvia se mudou ou se passaram `scraper_alert_repeat_hours` (default 24 h). Mudança só nas agências stale não reenvia. As queries de agências silenciosas, stale e de falhas consecutivas, e seus defaults, ficam duplicados em `health_checks.py`, e testes garantem que não divergem.

**`cleanup_old_scrape_runs`:** Deleta registros da tabela `scrape_runs` mais antigos que `scraper_retention_days` (default 90). Com ~150 agências em 9 execuções por dia, mais retries e EBC, são ~1,4 mil rows/dia (eram ~22 mil com as execuções de 10 em 10 min).

**`scraper_coverage_report`:** Calcula cobertura 24h (agências com sucesso / total ativas), total de notícias novas (`SUM(COALESCE(articles_new, articles_saved))`) e top erros por categoria. Alerta se cobertura < `scraper_min_coverage_ratio` (default 0.8).

**`notify.py`:** Utilitário compartilhado. `send_alert(message, telegram_token, telegram_chat_id, webhook_url, pubsub_topic, title, severity, source)` tenta, nesta ordem: Telegram (token + chat), Pub/Sub (tópico), webhook, log. O canal Pub/Sub publica no `dgb.ops.alerts` pela API REST (`google.auth.default` + `AuthorizedSession`, sem dependência nova) o JSON genérico do alert-relay do data-platform (`{"text", "title", "severity", "source"}`), que repassa ao Telegram da equipe. O relay escapa o HTML, então o texto vai sem `<b>` e com no máximo 3400 caracteres, cortado em fim de linha. Tópico: Variable `scraper_alert_pubsub_topic`, com default `projects/$GCP_PROJECT_ID/topics/dgb.ops.alerts` (`default_alert_topic()`). Até nov/2026 os alertas do scraper só iam para o log, porque as Variables `scraper_telegram_*` nunca foram criadas.

## Schema do Banco

### Tabela `news`

| Coluna | Tipo | Notas |
|--------|------|-------|
| `id` | serial | PK interno (autoincrement) |
| `unique_id` | text unique | Formato: `{slug}_{suffix}` |
| `agency_id` | int FK | → agencies.id |
| `title` | text | |
| `url` | text | |
| `content` | text | Markdown |
| `image_url` | text | |
| `video_url` | text | |
| `category` | text | |
| `tags` | text[] | |
| `editorial_lead` | text | |
| `subtitle` | text | |
| `summary` | text | |
| `content_hash` | text | SHA256[:16] de título+conteúdo |
| `published_at` | timestamptz | |
| `updated_datetime` | timestamptz | |
| `extracted_at` | timestamptz | |
| `created_at` | timestamptz | |
| `updated_at` | timestamptz | |
| `agency_key` | text | Desnormalizado |
| `agency_name` | text | Desnormalizado |
| `theme_l1_id` ... `most_specific_theme_id` | int FK | → themes.id |
| `content_embedding` | vector(768) | Phase 4.7 |
| `embedding_generated_at` | timestamptz | Phase 4.7 |

**Constraints:** `unique_id` é unique. Deduplicação por `(agency_key, url)` via pré-check + SAVEPOINT.

**Nuance do `allow_update`:** Artigos com mesmo `(agency_key, url)` são **sempre** atualizados via pre-check, independentemente do `allow_update`. O parâmetro controla apenas o comportamento para artigos novos com conflito em `unique_id` (ON CONFLICT DO UPDATE vs DO NOTHING).

**Colunas do enriquecimento:** `summary`, `theme_*_id`/`most_specific_theme_id` e `content_embedding`/`embedding_generated_at` são gravadas pelos workers downstream, nunca pelo scraper. O UPDATE da Phase 1 (re-scrape casado por URL) usa `COALESCE(v.summary, news.summary)` e não toca nos temas nem no embedding. No `ON CONFLICT DO UPDATE` do `allow_update=True`, resumo e temas usam `COALESCE(EXCLUDED.col, news.col)` (`_ENRICHED_COLUMNS`). Antes disso, o re-scrape apagava o resumo e o embedding (~6.935 artigos de 02/06 a 06/10/2026).

**`image_url` também é gravada downstream:** o thumbnail-worker (data-platform) grava a miniatura em artigos com `video_url` e sem imagem (TV Brasil, re-raspada a cada execução do dia). O scraper entrega `''`/NULL nesses casos, então a Phase 1 usa `COALESCE(NULLIF(v.image_url, ''), news.image_url)` e o `ON CONFLICT` o equivalente com `EXCLUDED` (`_DOWNSTREAM_FILLED_COLUMNS`): vazio não apaga o valor gravado; uma imagem não vazia da fonte prevalece. Efeito colateral aceito: se a fonte remover a imagem, a URL antiga fica (o integrity check a marca se quebrar).

**Edição de conteúdo não regera nada downstream:** quando o `content_hash` muda, o artigo é republicado, mas o enrichment-worker pula o já enriquecido (`skipped/already_enriched`, sem `dgb.news.enriched`), então resumo, temas e embedding ficam os da primeira versão. Por isso o embedding **não é zerado** na edição: o vetor é de título + resumo (`embeddings_client/text_prep.py`), o resumo é preservado, e zerar sem caminho de regeneração (o embeddings-api só assina `dgb.news.enriched`; a DAG `generate_embeddings` saiu) tiraria o artigo da busca semântica para sempre. Só uma edição de título deixa o vetor um pouco desatualizado. Regerar resumo e embedding em edições é evolução downstream (re-enriquecer quando o conteúdo muda), não do scraper.

### Tabela `agencies`

| Coluna | Tipo |
|--------|------|
| `id` | serial PK |
| `key` | text unique |
| `name` | text |
| `type` | text |
| `parent_key` | text |
| `url` | text |
| `created_at` | timestamptz |

### Tabela `themes`

| Coluna | Tipo |
|--------|------|
| `id` | serial PK |
| `code` | text unique |
| `label` | text |
| `full_name` | text |
| `level` | int (1-3) |
| `parent_code` | text |
| `created_at` | timestamptz |

### Tabela `scrape_runs`

| Coluna | Tipo | Notas |
|--------|------|-------|
| `agency_key` | text | |
| `status` | text | "success" ou "error" |
| `error_category` | text | ErrorCategory value |
| `error_message` | text | Truncado em 500 chars |
| `articles_scraped` | int | |
| `articles_saved` | int | Linhas gravadas: inseridas + atualizadas (re-scrape) |
| `articles_new` | int | Só as inseridas (migration 037 do data-platform). NULL no modo não sequencial, em erro e nas linhas anteriores à 037 |
| `execution_time_seconds` | float | |
| `scraped_at` | timestamptz | |
| `fallback_triggered` | bool | Se fallback automático foi acionado (nullable, default FALSE) |

`record_scrape_run` grava `articles_new`; se o banco ainda não tem a coluna (scraper no ar antes da 037), o INSERT falha com `UndefinedColumn`, é refeito sem ela e o aviso sai uma vez por processo. A linha de `scrape_runs` nunca se perde por isso.

## Variáveis de Ambiente (API / Cloud Run)

```bash
DATABASE_URL=postgresql://...        # Connection string (prioridade 1)
LOG_LEVEL=INFO                       # Loguru level
PUBSUB_TOPIC_NEWS_SCRAPED=projects/{project}/topics/dgb.news.scraped  # Opcional
```

**Fallback para DATABASE_URL:** Se não definido, tenta Secret Manager (`destaquesgovbr-postgres-connection-string`). Se falhar, detecta Cloud SQL Proxy local.

## Airflow Variables

Vêm do secrets backend do Airflow (`CloudSecretManagerBackend`: secret `airflow-variables-<nome>` no Secret Manager) e, se não estiverem lá, do metastore (Postgres da VM). O `scraper_monitor_last_alert` é gravado pelo monitor no metastore.

| Variable | Obrigatória | Default | Uso |
|----------|-------------|---------|-----|
| `scraper_api_url` | Sim | — | URL base da API Cloud Run |
| `scraper_database_url` | Sim | — | Connection string para DAGs de monitoramento |
| `scraper_retention_days` | Não | 90 | Dias de retenção em scrape_runs |
| `scraper_telegram_bot_token` | Não | — | Token do bot Telegram para alertas |
| `scraper_telegram_monitor_chat_id` | Não | — | Chat ID do grupo de monitoramento |
| `scraper_alert_pubsub_topic` | Não | `projects/$GCP_PROJECT_ID/topics/dgb.ops.alerts` | Tópico do alert-relay (Telegram da equipe); vazio desliga o canal |
| `scraper_alert_webhook_url` | Não | — | Webhook fallback para alertas |
| `scraper_alert_repeat_hours` | Não | 24 | Horas até reenviar o mesmo alerta do monitor (0 = sem dedup) |
| `scraper_monitor_last_alert` | Não | — | Estado do dedup (JSON gravado pelo monitor no metastore); apagar força o reenvio |
| `scraper_consecutive_failure_threshold` | Não | 3 | Falhas consecutivas para alerta |
| `scraper_failure_window_hours` | Não | 6 | Janela de tempo para verificar falhas (3 execuções de 2 em 2 h) |
| `scraper_stale_hours` | Não | 24 | Horas sem notícias para considerar stale |
| `scraper_silent_hours` | Não | 3 | Horas sem nenhum scrape_run (sucesso ou erro) para considerar a agência ativa silenciosa (mínimo 1) |
| `scraper_min_coverage_ratio` | Não | 0.8 | Cobertura mínima antes de alertar |
| `scraper_max_failed_ratio` | Não | 0.10 | Fração de agências com falha (0 a 1) acima da qual a `summarize` derruba a execução da `scrape_agencies`; inválida vale 0.10 |

### Env dos componentes do Airflow

| Env | Default | Uso |
|-----|---------|-----|
| `SCRAPE_MAX_CONCURRENT_AGENCIES` | 4 | `max_active_tis_per_dag` da task `scrape`, lido no parse (inválido vale 4, mínimo 1). Vem do Terraform (repo `infra`) |

## IAM e Autenticação

DAGs autenticam no Cloud Run via ID token:
```python
import google.auth.transport.requests
import google.oauth2.id_token

auth_req = google.auth.transport.requests.Request()
token = google.oauth2.id_token.fetch_id_token(auth_req, scraper_api_url)
headers = {"Authorization": f"Bearer {token}"}
```

A SA do Airflow (`destaquesgovbr-composer`, nome herdado do Composer; hoje é a SA da VM `destaquesgovbr-airflow`) deve ter a role `roles/run.invoker` no serviço Cloud Run e, para os alertas via Pub/Sub, `roles/pubsub.publisher` no tópico `dgb.ops.alerts` (sem ela, o publish dá 403 e o alerta cai no webhook ou no log).

## CI/CD

| Workflow | Trigger | Ação |
|----------|---------|------|
| `tests.yaml` | PR | pytest com coverage (`-m "not integration"`) + testes `postgres` contra service container `pgvector/pgvector:pg16` (porta 55432) |
| `scraper-api-deploy.yaml` | push main | Build Docker + deploy Cloud Run |
| `composer-deploy-dags.yaml` | push main (`dags/**`) | `airflow-deploy-dags.yml@v3` (reusable-workflows): valida com Airflow 3.1.8 e sincroniza `dags/` → `gs://destaquesgovbr-airflow/dags/scraper/` |

## Testes

- **30 unit tests** (`tests/unit/`) — cobertura de API, scrapers, storage, monitoring, DAGs
- **3 integration tests** (`tests/integration/`) — requerem DB real, marcados com `@pytest.mark.integration`
- **Postgres descartável** (`tests/integration/test_postgres_rescrape.py`, `test_postgres_health_checks.py` e `test_postgres_scrape_runs.py`, este com `scrape_runs` antes e depois da migration 037) — marcados com `integration` e `postgres`; pulados sem `SCRAPER_TEST_POSTGRES_URL`. Cada execução cria e remove um schema próprio (`scraper_it_*`) com DDL, então a guarda (`tests/integration/postgres_guard.py`) é dupla, porque "host local" não basta (o Cloud SQL Proxy de produção escuta em `127.0.0.1:5432`):
  - antes de conectar: host local explícito, sem `hostaddr`/`service` no DSN nem `PGHOSTADDR`/`PGSERVICE` no ambiente, porta ≠ 5432 (explícita, omitida ou via `PGPORT`) e dbname que não seja `destaquesgovbr`/`govbrnews`;
  - depois de conectar e antes de qualquer DDL: `current_database()` que não seja de produção, sem o papel `cloudsqlsuperuser` (instância Cloud SQL) e sem tabela `news` fora dos schemas `scraper_it_*`.
  - No CI (`tests.yaml`) rodam num service container pgvector. Com a variável definida e sem pgvector, o fixture falha (não pula), para o CI não passar em silêncio.

```bash
# Rodar todos os testes unitários
poetry run pytest tests/unit/

# Rodar apenas integration (requer DATABASE_URL configurado)
poetry run pytest -m integration

# Rodar os testes contra Postgres descartável (pgvector)
docker run -d --rm --name scraper-it-pg -e POSTGRES_HOST_AUTH_METHOD=trust \
    -p 127.0.0.1:55432:5432 pgvector/pgvector:pg16
SCRAPER_TEST_POSTGRES_URL=postgresql://postgres@127.0.0.1:55432/postgres \
    PYTHONPATH=src poetry run pytest -m postgres --no-cov
docker rm -f scraper-it-pg

# Rodar todos com coverage
poetry run pytest --cov=govbr_scraper --cov-report=term-missing
```

## Deploy

| Componente | Destino | Workflow |
|-----------|---------|----------|
| **API** | Cloud Run (`destaquesgovbr-scraper-api`) | `scraper-api-deploy.yaml` |
| **DAGs** | Bucket do Airflow próprio (`gs://destaquesgovbr-airflow/dags/scraper/`) | `composer-deploy-dags.yaml` |
| **Infra** | Terraform (repo `infra/`) | SA, Cloud Run, Artifact Registry, IAM |

### Deploy da API

O workflow `scraper-api-deploy.yaml` faz build da imagem Docker e deploy no Cloud Run automaticamente em push para `main`.

### Deploy das DAGs

O workflow `composer-deploy-dags.yaml` chama a reusable `destaquesgovbr/reusable-workflows/.github/workflows/airflow-deploy-dags.yml@v3` com `dags_local_path: dags` e `dags_bucket_subdir: scraper`. Ela valida as DAGs com o Airflow 3.1.8 (o da imagem da VM) e sincroniza no bucket do Airflow próprio:

```bash
gcloud storage rsync --recursive --delete-unmatched-destination-objects dags/ gs://destaquesgovbr-airflow/dags/scraper/
```

Na VM, o container `dag-sync` espelha o bucket a cada 60 s e o dag-processor reparseia cada arquivo a cada 180 s: uma mudança numa DAG existente pode levar até ~4 min para valer. A lista de buckets é o default da reusable workflow (tag `v3`), e o bucket do Composer sai dela só lá; este repo não a define. O nome `composer-deploy-dags.yaml` ficou do Composer.

### Ordem de deploy (Passo 1 da rearquitetura da ingestão, `_plan/rearquitetura-ingestao.md`)

1. Migration 037 do data-platform (`scrape_runs.articles_new`) aplicada em produção (workflow `db-migrate`). Conferir antes do merge (somente leitura): `SELECT 1 FROM information_schema.columns WHERE table_name = 'scrape_runs' AND column_name = 'articles_new'` devolve uma linha. Só o INSERT do scraper-api tolera a falta da coluna. As DAGs a referenciam sem fallback e sobem no mesmo merge; sem a 037:
   - `check_stale_agencies` falha com `UndefinedColumn` (depois do retry) e a `send_alerts` (`trigger_rule` padrão `all_success`) fica `upstream_failed`: o **monitor inteiro fica mudo**, inclusive "Agências Sem Execução" e "Falhas consecutivas", não só a seção de stale;
   - o `scraper_coverage_report` falha (a query de cobertura soma `articles_new`).
2. Tag `v3` do reusable-workflows publicada.
3. Merge do scraper: a API (Cloud Run) e as DAGs sobem juntas. As DAGs novas também funcionam com a API antiga (sem `retryable`, tudo repete, como antes).

## Desenvolvimento Local

```bash
# Instalar dependências
poetry install

# Variáveis de ambiente mínimas (apenas DATABASE_URL é obrigatória)
export DATABASE_URL=postgresql://user:pass@localhost:5432/destaquesgovbr
# export PUBSUB_TOPIC_NEWS_SCRAPED=...  # opcional — graceful degradation se ausente

# Rodar API localmente
poetry run uvicorn govbr_scraper.api:app --reload
```

### Code Style

- **Formatter:** Ruff (line-length 100, target Python 3.12)
- **Linter:** Ruff (E, W, F, I, B, C4, UP — config em `pyproject.toml`)

```bash
poetry run ruff format .
poetry run ruff check . --fix
```

### Processo de PR

1. Crie branch a partir de `main`
2. Garanta que `poetry run pytest` passa
3. Abra PR — CI roda testes automaticamente
4. Após merge em `main`, deploy é automático (API + DAGs)

## Política de Commits

Commits NÃO devem incluir atribuições ao Claude Code ou co-autoria do Claude.
