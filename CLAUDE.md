# DestaquesGovBr Scraper

Scraper standalone para notícias de sites governamentais brasileiros (~155 agências gov.br + EBC). Suporta três tipos de scraper (HTML, EBC, Plone6 API), publica eventos Pub/Sub após inserção, e inclui sistema completo de monitoramento com alertas. O sistema inclui 3 DAGs de manutenção.

## Visão Geral

O scraper coleta notícias publicadas em sites gov.br e da EBC (Agência Brasil, TV Brasil, etc.), extraindo título, conteúdo, data de publicação e metadados. Os dados são persistidos no PostgreSQL (upsert por unique_id, deduplicação por URL). Após persistência, eventos são publicados no Pub/Sub (`dgb.news.scraped`) para consumo downstream.

A coleta é orquestrada por **DAGs Airflow** (Airflow 3.1.8 próprio, na VM `destaquesgovbr-airflow`; o Cloud Composer foi desligado em 18/11/2026) que chamam a **API FastAPI** hospedada no Cloud Run. O sistema inclui 3 DAGs de manutenção para monitoramento, alertas e retenção de dados, mais o utilitário `notify.py`.

## Arquitetura

```
DAGs Airflow (Airflow próprio, VM destaquesgovbr-airflow)
│   ├── scrape_agencies (06h–22h de 2 em 2 h, 1 task mapeada por agência + summarize)
│   ├── scrape_ebc (06h–22h de 2 em 2 h, 1 task mapeada por fonte)
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
        ├── GET /agencies/state ────────┤  (painel "Ingestão" do streamlit-infra-health)
        └── GET /health                 │
                                        ▼
                              ┌─── PostgreSQL ──────────┐
                              │  news                   │
                              │  agencies               │
                              │  themes                 │
                              │  scrape_runs            │
                              │  scrape_agency_state    │  (marca d'água + disjuntor)
                              └─────────────────────────┘
                                        │
                                        ▼
                              Pub/Sub (dgb.news.scraped)
                              [opcional, graceful degradation]
                              Pub/Sub (dgb.ops.alerts): abertura/fechamento do disjuntor

    Credenciais: DATABASE_URL env → Secret Manager fallback
```

## Estrutura do Repositório

```
scraper/
├── src/govbr_scraper/
│   ├── api.py                         # FastAPI (Cloud Run) — 5 endpoints
│   ├── logging_setup.py               # configure_logging: JSON no Cloud Logging (LOG_FORMAT)
│   ├── state/                         # Estado por agência (scrape_agency_state, migration 038)
│   │   ├── model.py                   # Tier, CircuitState, AgencyState, AttemptOutcome, Transition
│   │   ├── policy.py                  # Constantes: disjuntor, sondas, encurtamento, marca d'água
│   │   ├── transitions.py             # apply_attempt, effective_interval_minutes, circuit_gate
│   │   ├── window.py                  # watermark_window (datas BRT)
│   │   ├── slo.py                     # lag_minutes, within_slo
│   │   ├── repository.py              # SQL (conexão do chamador, sem commit); StateUnavailable
│   │   ├── report.py                  # Corpo de GET /agencies/state
│   │   └── alerts.py                  # publish_ops_alert (dgb.ops.alerts), alert_for_transition
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
│       └── structured_log.py         # Log estruturado, record_scrape_run e eventos (scrape_attempt, circuit_transition)
├── dags/
│   ├── scrape_agencies.py             # 1 DAG, 1 task mapeada por agência ativa + summarize
│   ├── scrape_ebc.py                  # 1 DAG, 1 task mapeada por fonte EBC
│   ├── monitor_scraping_health.py     # Monitoramento: sem execução + falhas + circuito aberto + stale
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

Otimização que evita re-processar artigos já coletados. Antes de iniciar o scraping, o `ScrapeManager` (e o `EBCScrapeManager`, para cada fonte) consulta as últimas 200 URLs da agência via `get_recent_urls()`. Durante o scraping, URLs já conhecidas são contadas consecutivamente e puladas sem baixar o artigo. Ao atingir **3 URLs conhecidas consecutivas** (`KNOWN_URL_STOP_THRESHOLD`, o mesmo nos três scrapers), o scraper para — assumindo que o restante já foi coletado em execuções anteriores. Se a consulta falhar, a raspagem segue sem cerca.

**EBC** (Passo 2 da rearquitetura da ingestão): antes, cada execução baixava todos os artigos desde 00:00 BRT de `min_date` e regravava cada um. Agora a cerca vale antes do `sleep` e do download do artigo (`EBCWebScraper.process_news_urls`); áudio (`radios.ebc.com.br`) não é baixado nem conta. Na EBC a data do artigo só aparece depois do download (no gov.br ela vem na listagem, e o item mais novo que `max_date` é pulado antes da cerca), então num backfill os primeiros itens da listagem, os mais novos e já conhecidos, parariam a fonte antes da janela: a cerca é **desligada quando `max_date` é anterior a hoje (BRT)** (`known_url_fence_applies`), e a consulta nem é feita. A chave do `ebc_urls.yaml` é a mesma de `news.agency_key` (vem do host do artigo); um link da TV Brasil na listagem da Agência Brasil fica fora do conjunto da `agencia_brasil` e só seria baixado de novo.

### Itens pulados (não gravados)

O `ScrapeManager` descarta antes de persistir, loga cada item e soma em `articles_skipped` (resposta da API e linha `scrape_skipped agency=… invalid_content=N redated=M`):

- **Conteúdo inválido** (scraper#65, `content_validation.py`): `"Error retrieving content"`, tela de login do Plone (redirect para `require_login`/`acl_users` ou texto "necessário autenticar para visualizar") e conteúdo vazio. Log `skipped_invalid_content agency=… reason=<error_content|login_page|empty_content> url=…`. A URL não é gravada, então fica fora de `known_urls` e o artigo é tentado de novo enquanto estiver na janela.
- **Redatados** (`redated.py`): conteúdo antigo republicado com data nova, como na onda pós-defeso de 26-29/10/2026. É avaliado antes de baixar o artigo e antes da cerca de URLs conhecidas. Plone 6: `created` 30 dias ou mais antes do `effective`. HTML: pasta `/AAAA/MM/` ou `/AAAA/<mês>/` da URL anterior ao mês da listagem por mais de 1 mês. Log `skipped_redated agency=… signal=<plone_created|url_folder> … url=…`. Com `SCRAPER_REDATED_FILTER=observe` (env da API), o filtro só loga (`redated_observed`) e mantém o item. **Exceção, conteúdo retido no defeso:** item com `created` (Plone 6) ou pasta de data (HTML) dentro de `RETAINED_CONTENT_WINDOWS` (defeso eleitoral 2026: 04/07 a 25/10) não é pulado (log `redated_kept_retained`). Ele ficou privado durante o defeso e foi publicado pela primeira vez depois, então a data nova é a da primeira publicação. Para um defeso futuro, acrescente a janela em `redated.py`.

### Estado por agência, marca d'água e disjuntor (Passo 2 da rearquitetura)

`_plan/rearquitetura-ingestao.md`, Passo 2. A tabela `scrape_agency_state` (migration 038 do data-platform, 1 linha por `agency_key` gov.br e EBC) é a fonte de verdade da ingestão por agência. As regras são funções puras em `src/govbr_scraper/state/` (testadas sem banco); o SQL fica em `state/repository.py`.

- **Quem grava:** `PostgresManager.record_scrape_run`, na mesma conexão e transação do INSERT em `scrape_runs`, dentro do `SAVEPOINT agency_state`: `ensure_rows` → `get_state(for_update=True)` → `apply_attempt` → `upsert_state`. Erro no estado (inclusive sem a 038, `StateUnavailable`, avisado uma vez por processo) desfaz só o savepoint: **a linha de `scrape_runs` nunca se perde**. Depois do commit, uma transição do disjuntor gera o evento `circuit_transition` e o alerta 🔴/🟢 em `dgb.ops.alerts` (`publish_ops_alert`, env `PUBSUB_TOPIC_OPS_ALERTS`; sem a env só loga).
- **Marca d'água:** `last_success_at` = início da última tentativa bem-sucedida (`scraped_at − execution_time_seconds`). Sem datas na requisição, a janela de cada agência é `agency_window(now, state)` em datas BRT: de `(last_success_at − 1 h).date()` até hoje, limitado a hoje − `watermark_max_days(rate_ewma_per_day)`; sem histórico (ou com o estado indisponível), ontem a hoje. O limite é de 7 dias (`WATERMARK_MAX_DAYS`) ou os dias que somam ~80 notícias da agência (`WATERMARK_MAX_ARTICLES`), o que for menor e no mínimo 1: `agencia_brasil` (~40/dia) volta 1 dia antes de hoje, `pf` 3, `tvbrasil` 4, as outras 7. Sete dias de uma agência de muito volume não cabem numa requisição (900 s no Cloud Run, 10 min por task) e viram uma rajada em `dgb.news.scraped`. Janela cortada pelo limite sai no log (`window_truncated: true` no `scrape_attempt`); os dias de fora não são raspados sozinhos e, depois do sucesso, a marca d'água passa por cima deles (recuperação com datas, runbook). Substitui o lookback fixo de 9 h das DAGs.
- **Janela explícita** (`start_date`/`end_date` na requisição: backfill ou recuperação): a tentativa grava `scrape_runs`, mas o estado não muda (só `updated_at`).
- **Disjuntor:** abre com 2 erros permanentes (`PERMANENT_ERROR_CATEGORIES`) desde o último sucesso, ou com 24 h de falhas seguidas de qualquer tipo (`failure_streak_started_at`). Aberto, a agência é testada por sondas (`next_due_at`): a primeira 2 h depois da abertura; depois de cada sonda que falha, 6 h, 24 h e daí em diante a cada 24 h. A primeira sonda com sucesso fecha. Sonda que falha não gera alerta. `unknown` e erro sem categoria são passageiros.
- **Skip na API:** em `/scrape/agencies` e `/scrape/ebc`, circuito `open`/`half_open` cujo `next_due_at` passa de `now + 7 min` (meio tick do futuro `/dispatch`) não é raspado nem grava `scrape_runs`: vai para `skipped` na resposta. Vencido, vira `half_open` (`mark_probe`) e é a sonda. `force: true` ignora o circuito (um sucesso fecha com o motivo `forced_success`).
- **Intervalo e encurtamento:** `next_due_at = início + intervalo efetivo − 5 min`; notícia nova (`articles_new > 0`) põe `boost_until = fim + 6 h`, e o intervalo efetivo cai pela metade nas faixas h2, h6 e d1 (a m30 fica no piso de 30 min). No Passo 2 as DAGs ainda são o relógio; `tier`, `poll_interval_minutes` e `next_due_at` passam a mandar no `/dispatch` (Passos 3 e 4).
- **SLO** (`state/slo.py`, painel): dentro de 2× o intervalo efetivo desde o último sucesso, sem contar a noite (23:00–06:00 BRT); circuito aberto fica fora (`within_slo = None`). Enquanto o `/dispatch` não está em `live` (Passo 2, `off` e `dry_run`), o intervalo do SLO tem piso de 120 min (`DAG_CADENCE_MINUTES`, `slo_interval_floor`): as DAGs de 2 h ainda são o relógio, e sem o piso as m30 e as agências com boost apareceriam como atrasadas sem estar.

### Logs estruturados (JSON no Cloud Logging)

`configure_logging()` (`logging_setup.py`, chamada no `__init__` do pacote) troca o sink do loguru por uma linha JSON por log no stderr: `{"severity", "message", "time", ...}` com os campos de `logger.bind(...)`/extra no nível raiz, o que no Cloud Run vira `jsonPayload.<campo>`. O `logging` da stdlib (managers, `api.py`) passa pelo loguru e também sai em JSON. **`LOG_FORMAT=text`** mantém o formato antigo do loguru (uso local; os testes o definem em `tests/conftest.py`). `LOG_LEVEL` continua definindo o nível.

Eventos fixos (os nomes de campo valem para as métricas baseadas em log do infra):

| Evento | Campos |
|--------|--------|
| `scrape_attempt` | `agency`, `outcome` (`success`/`failed`/`skipped`), `category` (ou `"none"`), `retryable`, `articles_new`, `duration_s`, `tier`, `explicit_window`, `trigger` (`dag`/`task`/`backfill`); mais `window_start`/`window_end`, `window_truncated`, `probe` e, no skip, `reason`/`next_probe_at`. Um por agência por tentativa, também quando pulada |
| `circuit_transition` | `agency`, `from_state`, `to_state`, `reason` (`permanent_errors`, `transient_24h`, `probe_success`, `forced_success`), `category` |

Ex.: `jsonPayload.event="scrape_attempt" AND jsonPayload.outcome="failed"`.

### Content Hash

Deduplicação por conteúdo via `compute_content_hash(title, content)`:
1. Normaliza texto (NFKD → ASCII, lowercase, remove pontuação, colapsa espaços)
2. Concatena `título\nconteúdo` normalizado
3. SHA256 dos primeiros 16 caracteres hex

Gravado em `news.content_hash`. Decide se o re-scrape de um artigo já gravado (mesmo `(agency_key, url)`) regrava a linha e a republica em `dgb.news.scraped` (ver "Nuance do `allow_update`" e "Pub/Sub Events"), e é usado na verificação de integridade.

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

O status HTTP vem antes da mensagem; 5xx cai na mensagem. Os managers (gov.br e EBC) classificam com `classify_exception(e)`, que usa o `error_category` do `FallbackFailedError` e do `ListingPageError` ou o `http_status` do `PermanentHTTPError` quando houver, e a mesma categoria vai para `scrape_runs.error_category` e para `errors[].category` da API.

**EBC, primeira página do índice** (`EBCWebScraper._scrape_first_index_page`): a falha não é mais engolida. Página não baixada depois dos retries (5xx, 408, 429, timeout, conexão) levanta `ListingPageError` com `network_error` (passageiro); página sem nenhum link de artigo, `ListingPageError` com `html_changed` (permanente); 4xx permanente sobe como `PermanentHTTPError` (`url_broken`/`access_denied`, como no gov.br). Antes, a EBC fora do ar ou com o HTML mudado saía como `success` com 0 artigos, e o disjuntor e a marca d'água do estado por agência não atuariam para ela. **Páginas seguintes** (`_scrape_next_index_page`): a página não baixada (depois dos retries, ou 4xx) também levanta `ListingPageError` com `network_error` (passageiro, porque a página 0 respondeu), e o que já foi coletado é descartado, como no gov.br. Antes, a falha só encerrava a paginação e a fonte saía como `success` com parte da janela: a marca d'água avançava sobre a parte não coberta e, com a cerca de URLs conhecidas, a execução seguinte parava nas notícias mais novas, já gravadas, sem nunca chegar nela. Página seguinte sem links continua sendo o fim da listagem. Um artigo que falha continua sendo só pulado: ele volta a ser tentado enquanto houver menos de 3 notícias conhecidas antes dele na listagem.

**Primário e fallback falharam** (`FallbackFailedError`, mensagem `Both scrapers failed. Primary: No articles found... Fallback: ...`): a mensagem leria sempre `html_changed`, então o `ScrapeManager` fixa a categoria. Falha de rede do Plone 6 (`network_error`: 5xx, 408/429 depois dos retries, timeout, DNS) vale como `network_error`, passageira, porque um retry pode raspar pelo fallback. Qualquer outra (404 da API, como a mec, ou resposta não-JSON, como a antaq: o site não serve a API do Plone 6) fica `html_changed`, permanente.

`PERMANENT_ERROR_CATEGORIES` = `url_broken`, `access_denied`, `html_changed`, `anti_bot`; `is_retryable(categoria)` é False para elas e para `unknown_agency` (agência fora do YAML ou inativa, `UnknownAgencyError` do `yaml_config`; só existe na resposta da API, sem linha em `scrape_runs`), True para o resto e para erro sem categoria.

### Política HTTP (falhar rápido)

`scrapers/http_policy.py` vale para WebScraper, Plone6APIScraper e EBCWebScraper (infra#223):

- 4xx, menos 408 e 429: `PermanentHTTPError` (`scrapers/errors.py`, subclasse de `ScrapingError` e não de `RequestException`) na primeira resposta, sem retry.
- Listagem que redireciona para o login do Plone (`is_login_redirect`): `LoginRedirectError` (subclasse de `PermanentHTTPError`), sem retry e sem fallback.
- 5xx, 408, 429, timeout e conexão: até 3 tentativas (`tries=3, delay=1, backoff=2`, jitter de 0 a 1 s; ~3,5 s de espera no total).
- `timeout=(5, 15)` (connect, read).

Antes, todo `RequestException` era repetido 5 vezes (`delay=2, backoff=3`, jitter de 1 a 3 s), inclusive 404 e 401: ~116 s de espera por chamada, 86% do tempo faturável do Cloud Run. No EBC o `@retry` envolvia um `try/except` que devolvia None, então nunca repetia; agora `fetch_page` ainda devolve None na falha, mas repete o erro transitório. As páginas do índice não usam `fetch_page`: a falha vira erro classificado (ver "Classificação de Erros").

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

Após persistir artigos, `EventPublisher` publica no tópico `dgb.news.scraped` os artigos novos e os existentes (casados por `(agency_key, url)`) cujo `content_hash` mudou. Re-scrape sem mudança de conteúdo não republica, tenha o artigo tema ou não: o enrichment-worker pularia o já enriquecido e o bronze-writer reescreveria o mesmo objeto. Com `allow_update=True`, as linhas devolvidas pelo `ON CONFLICT (unique_id) DO UPDATE` continuam sendo publicadas (regravação forçada).

**Artigo sem tema (enriquecimento que falhou):** até o Passo 2 da rearquitetura da ingestão (`_plan/rearquitetura-ingestao.md`), o re-scrape republicava o artigo sem tema (`most_specific_theme_id IS NULL`) a cada execução, porque era a única nova tentativa: o enrichment-worker responde 200/ACK até em exceção e em `classification_failed` (o `max_delivery_attempts` da assinatura nunca atua), a DAG `enrich_news_llm` foi removida (data-science 8f10273) e não havia Cloud Scheduler. Esse critério saiu. O retry passa a ser o reconciliador diário do `/dispatch` (Passo 3): na passada das 03h (modo `live`), republica até 20 notícias sem tema criadas entre 7 dias e 2 h atrás. **Até o Passo 3 estar em `live`, um enriquecimento que falhar não tem retry automático** (republicar à mão em `dgb.news.scraped`).

```json
{
  "unique_id": "governo-anuncia-programa_a3f2e1",
  "agency_key": "mec",
  "published_at": "2026-05-18T14:30:00-03:00",
  "scraped_at": "2026-05-18T17:45:12.123456+00:00"
}
```

Atributos da mensagem: `trace_id` (UUID por batch), `event_version` ("1.0").

**Publicação em lotes:** `publish_scraped` publica no máximo 20 mensagens de uma vez (`EventPublisher.BATCH_SIZE`), com 30 s entre os lotes (`BATCH_PAUSE_SECONDS`). O enrichment-worker cai por liveness com ~40 mensagens de uma vez e perde em silêncio o `dgb.news.enriched` (artigo com tema e sem embedding/Typesense). Uma recuperação pela marca d'água ou com datas grava dezenas de artigos de uma agência num INSERT só; na execução normal não há pausa. O limite vale por requisição: uma queda geral, recuperada por várias agências ao mesmo tempo (4 tasks simultâneas), ainda soma rajadas.

**Graceful degradation:** Se `PUBSUB_TOPIC_NEWS_SCRAPED` não estiver definido ou o client falhar na inicialização, todos os publishes são no-ops. Artigos já estão persistidos no PostgreSQL.

## API Endpoints

A API roda no Cloud Run e é chamada pelas DAGs Airflow.

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/scrape/agencies` | Raspa sites gov.br (aceita lista de agências, datas) |
| `POST` | `/scrape/ebc` | Raspa sites EBC (Agência Brasil, TV Brasil) |
| `POST` | `/verify/integrity` | Verifica integridade de artigos (imagens, conteúdo) |
| `GET` | `/agencies/state` | Estado de cada agência (`scrape_agency_state`), com atraso, SLO e resumo |
| `GET` | `/health` | Health check |

### Request: `/scrape/agencies`

```json
{
  "agencies": ["mec", "mds"],
  "allow_update": false,
  "sequential": true,
  "force": false
}
```

- **`start_date`/`end_date` opcionais**, validados como `date` (`YYYY-MM-DD`; inválido dá 422, e `end_date` sem `start_date` também). Sem datas, cada agência usa a janela da marca d'água; com `start_date` (`end_date` padrão = `start_date`), a janela é explícita e não mexe no estado da agência.
- Sem `agencies`, todas as ativas do YAML. A API raspa **agência por agência**, cada uma com a própria janela e o próprio disjuntor.
- **`sequential`** é aceito e ignorado: cada agência é raspada em modo sequencial. No modo em lote, o `scrape_run` de sucesso (que avança a marca d'água) era gravado antes do INSERT das notícias, e um erro no INSERT perderia a janela.
- **`force`** (padrão `false`): ignora o disjuntor.

### Request: `/scrape/ebc`

```json
{
  "agencies": ["agencia_brasil", "tvbrasil"],
  "allow_update": false,
  "sequential": true,
  "force": false
}
```

Mesmos campos. Sem `agencies`, as duas fontes ativas do `ebc_urls.yaml`, cada uma com a própria janela.

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
  "skipped": [
    {"agency": "mec", "reason": "circuit_open", "next_probe_at": "2026-11-19T16:00:00Z"}
  ],
  "message": "Completed with 1 error(s)"
}
```

- **Status HTTP:** 200 `completed`, 207 `partial`, 500 `failed` (todas as agências tentadas falharam) ou exceção (`{"detail": ...}`), 422 payload inválido (inclusive data inválida). Novo: só agências puladas pelo disjuntor dá 200 com `status: "skipped"`.
- **`skipped`:** agências com o circuito aberto cuja sonda ainda não venceu; não foram raspadas e não gravaram `scrape_runs`.
- **`start_date`/`end_date` da resposta:** a janela explícita ou, com a marca d'água, a menor e a maior data das janelas usadas (`null` se nenhuma agência foi raspada).
- **`articles_saved`:** linhas gravadas em `news`, inseridas + atualizadas. Re-scrape casado por `(agency_key, url)` só conta se o `content_hash` mudou; com `allow_update`, contam todos os casados por URL e o `ON CONFLICT DO UPDATE`. Até o Passo 2 da rearquitetura, todo artigo casado por URL era atualizado e contado, mesmo sem mudança (a agencia_brasil aparecia com ~2.175 "salvas"/dia).
- **`articles_new`:** só as linhas de fato inseridas: o `RETURNING` do `ON CONFLICT DO NOTHING` ou, com `allow_update=True`, as linhas com `xmax = 0` no `RETURNING unique_id, (xmax = 0) AS inserted` do `DO UPDATE` (o retry do `UniqueViolation` conta igual). `PostgresManager.insert` devolve `InsertResult(saved, articles, new)` e `StorageAdapter.insert`, `InsertCounts(saved, new)`. Em nov/2026, antes do Passo 2: ~242 notícias novas/dia, contra 2.175 "salvas"/dia só na agencia_brasil.
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

### `GET /agencies/state`

Só leitura, para o painel "Ingestão" do streamlit-infra-health (que chama com ID token). Devolve `{"generated_at", "dispatch_mode", "agencies": [...], "summary": {...}}`:

- `agencies`: uma entrada por linha de `scrape_agency_state`, com as colunas, `active` (ativa no `site_urls.yaml` ou no `ebc_urls.yaml`), `effective_interval_minutes`, `lag_minutes` e `within_slo`;
- `summary` (só ativas): `total`, `active`, `by_tier`, `circuit_open`, `overdue` (circuito fechado e fora do SLO) e `slo_ok_ratio`. Fora do modo `live`, `within_slo` usa o piso de 120 min da cadência das DAGs;
- `dispatch_mode`: env `SCRAPE_DISPATCH_MODE` (padrão `off`);
- 503 `{"detail": "scrape_agency_state indisponível"}` sem a tabela (038 não aplicada).

## DAGs Airflow

| DAG | Schedule (BRT) | Descrição |
|-----|----------|-----------|
| `scrape_agencies` | `0 6-22/2 * * *` (06h–22h, 9x/dia) | 1 task mapeada (`scrape`) por agência gov.br ativa (~147) + `summarize` (única folha) |
| `scrape_ebc` | `0 6-22/2 * * *` | 1 task mapeada por fonte EBC (`agencia_brasil`, `tvbrasil`) |
| `monitor_scraping_health` | `30 6-22/2 * * *` | Agências sem execução + falhas consecutivas + circuito aberto + agências stale |
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
- A task lê o corpo antes do status HTTP (sem `raise_for_status`). Agência em `skipped[]` (circuito aberto) termina skipped. Erro permanente (todos os `errors[]` da agência com `retryable: false`) levanta `AirflowFailException`: falha numa tentativa só, com agência e categoria na mensagem. Erro passageiro, corpo sem JSON, erro de rede do httpx e API antiga sem `retryable` levantam `AirflowException` e repetem. No sucesso a task devolve `{"agency", "articles_new", "articles_saved"}` (XCom)
- Task final `summarize` (`trigger_rule="all_done"`, `retries=0`), **única folha** (a cor do DagRun é a dela): itera o `LazyXComSequence` da `scrape` (só tem os resultados das tasks com sucesso, compactados; **nunca fatiar**, `results[:]` levanta TypeError no Airflow 3.0.1), deduz as falhas como ativas − ok − puladas (XCom `skipped`), loga contagens, agências com falha, puladas e notícias novas. Só falha se nenhuma agência funcionou (só puladas fica verde) ou se a fração de falhas entre as tentadas passar da Variable `scraper_max_failed_ratio` (padrão 0,10). Agência quebrada fica vermelha só na task mapeada dela, até o disjuntor abrir; daí em diante a task fica skipped
- Retry: 2x com delay fixo de 5min (agências e EBC), só para erro passageiro
  - **Nunca use `retry_exponential_backoff` nem `max_retry_delay` em task mapeada (nem em `default_args`):** no Airflow 3.1.0 do Composer, o scheduler lê `task.max_retry_delay` de uma task mapeada em retry, o `MappedOperator` não tem esse atributo e o `AttributeError` derruba o loop do scheduler inteiro (todas as DAGs paradas de 18/11/2026 02:00Z a 09:00Z). Teste de regressão por AST nas duas DAGs
- Timeout: 10 min por agência / 16 min por fonte EBC (a requisição tem 900 s no Cloud Run e no httpx)
- Janela: **a DAG não manda datas** (Passo 2 da rearquitetura). A API usa a marca d'água de cada agência (`scrape_agency_state.last_success_at`, até 7 dias ou ~80 notícias da agência; ver "Estado por agência, marca d'água e disjuntor"). O `_scrape_window` de 9 h saiu.
- Params opcionais `start_date`/`end_date` (YYYY-MM-DD, os dois juntos) mandam uma janela explícita num disparo manual, para recuperar um período (runbook, "Agências Sem Execução"); a janela explícita não mexe no estado da agência. Formato inválido é recusado no disparo (`Param` com `format: date`); só um dos dois ou início depois do fim falham a task sem retry (`AirflowFailException`). Param `force` (boolean, padrão `false`): ignora o disjuntor.
- **Agência pulada pelo disjuntor** (em `skipped[]` da resposta): a task grava a XCom `skipped` (`{agency, next_probe_at}`) e levanta `AirflowSkipException`. A `summarize` recebe `scraped["skipped"]` (um `LazyXComSequence` dessa chave nas tasks mapeadas, que inclui as puladas) e as conta à parte: pulada não é falha, a fração de falhas é sobre as agências tentadas, e só puladas fica verde.
- `scrape_ebc` tem a mesma agenda, os mesmos params e a mesma leitura da resposta (cópia das funções: cada arquivo de DAG é autocontido, não importa outro módulo de DAG nem o pacote `govbr_scraper`). Uma task mapeada por fonte (`EBC_SOURCES = ["agencia_brasil", "tvbrasil"]`, a sigla como map index), cada uma com a própria chamada a `/scrape/ebc` (`agencies: [fonte]`) e a mesma leitura da `scrape_agencies`: erro permanente falha sem retry, passageiro repete, circuito aberto dá skipped. Com as duas fontes numa requisição só, a API as raspava em série dentro do mesmo limite de 900 s do Cloud Run, e uma recuperação de alguns dias cortava a segunda sem gravar nada. `execution_timeout` de 16 min, logo acima dos 900 s da requisição (httpx e Cloud Run).
- Um DagRun zumbi agora para todas as agências de uma vez, e o monitor acusa na execução seguinte. Com uma DAG por agência, ele parava uma agência só, em silêncio (`scrape_esg`, sem execução desde 11/11/2026).

### DAGs de manutenção

**`monitor_scraping_health`:** Roda 30 min depois de cada execução do scraping (`30 6-22/2 * * *`; nunca na madrugada). Verifica agências ativas (no `dags/config/site_urls.yaml`) sem nenhum `scrape_run` em X horas (`scraper_silent_hours`, default 3 h = cadência de 2 h + 1 h de folga: a task mapeada da agência na `scrape_agencies` não chamou a API, ex. DAG pausada ou DagRun preso em `running`; ver runbook, "Agências Sem Execução"), agências com N falhas consecutivas (threshold 3, janela `scraper_failure_window_hours` de 6 h) ou com erro permanente no `scrape_run` mais recente (a task falha sem retry e grava 1 linha por execução; sem esse critério a agência quebrada levaria 3 execuções para entrar e sairia da seção toda manhã) e agências sem notícia nova em X horas (stale: `COALESCE(articles_new, articles_saved) > 0`, para as linhas anteriores à migration 037 valerem pelo `articles_saved`). Envia alertas via `notify.send_alert()`, sem repetir: o resumo das seções sem execução, falhas consecutivas e circuito aberto (só as siglas) é comparado com o do último envio, na Variable `scraper_monitor_last_alert`; reenvia se mudou ou se passaram `scraper_alert_repeat_hours` (default 24 h). Mudança só nas agências stale não reenvia. Agências com o disjuntor aberto em `scrape_agency_state` (`circuit_state <> 'closed'`) saem de "sem execução" e de "falhas consecutivas" (a API as pula sem gravar `scrape_runs`) e vão para a seção "Circuito Aberto" (task `check_open_circuits`: só as ativas, gov.br do YAML e `_EBC_SOURCES`, com último erro, abertura e próxima sonda). Ela entra no resumo do dedup: o alerta 🔴 do scraper-api é publicado uma vez só, sem retry, e sem a seção a agência ficaria fora da raspagem regular sem nenhum aviso se ele se perdesse. Sem a tabela (038 não aplicada), `UndefinedTable` faz rollback e nada muda. As queries de agências silenciosas, stale, de falhas consecutivas e de circuito aberto, e seus defaults, ficam duplicados em `health_checks.py`, e testes garantem que não divergem.

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

**Nuance do `allow_update`:** Artigo com o mesmo `(agency_key, url)` (pré-check da Phase 1) é atualizado quando o `content_hash` mudou (hash gravado NULL conta como mudança). Com o mesmo hash e `allow_update=False` (o das DAGs), não há UPDATE, a linha não conta em `articles_saved` e não é republicada; os campos fora do hash (`image_url`, `tags`, `category`, `updated_datetime`, `extracted_at`...) ficam como estão, e `extracted_at` passa a ser a primeira extração. Com `allow_update=True` (regravação forçada, ex.: depois de corrigir o parser), todo artigo casado é atualizado, mas só o de hash diferente é republicado. O parâmetro também controla o conflito em `unique_id` dos artigos novos (ON CONFLICT DO UPDATE vs DO NOTHING).

**Colunas do enriquecimento:** `summary`, `theme_*_id`/`most_specific_theme_id` e `content_embedding`/`embedding_generated_at` são gravadas pelos workers downstream, nunca pelo scraper. O UPDATE da Phase 1 (re-scrape casado por URL) usa `COALESCE(v.summary, news.summary)` e não toca nos temas nem no embedding. No `ON CONFLICT DO UPDATE` do `allow_update=True`, resumo e temas usam `COALESCE(EXCLUDED.col, news.col)` (`_ENRICHED_COLUMNS`). Antes disso, o re-scrape apagava o resumo e o embedding (~6.935 artigos de 02/06 a 06/10/2026).

**`image_url` também é gravada downstream:** o thumbnail-worker (data-platform) grava a miniatura em artigos com `video_url` e sem imagem (TV Brasil). A proteção vale no UPDATE do re-scrape com conteúdo alterado ou com `allow_update=True`; com o mesmo conteúdo e `allow_update=False` a linha nem é regravada. O scraper entrega `''`/NULL nesses casos, então a Phase 1 usa `COALESCE(NULLIF(v.image_url, ''), news.image_url)` e o `ON CONFLICT` o equivalente com `EXCLUDED` (`_DOWNSTREAM_FILLED_COLUMNS`): vazio não apaga o valor gravado; uma imagem não vazia da fonte prevalece. Efeito colateral aceito: se a fonte remover a imagem, a URL antiga fica (o integrity check a marca se quebrar).

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
| `articles_saved` | int | Linhas gravadas: inseridas + atualizadas (re-scrape com `content_hash` diferente; com `allow_update`, todo re-scrape) |
| `articles_new` | int | Só as inseridas (migration 037 do data-platform). NULL no modo não sequencial, em erro e nas linhas anteriores à 037 |
| `execution_time_seconds` | float | |
| `scraped_at` | timestamptz | |
| `fallback_triggered` | bool | Se fallback automático foi acionado (nullable, default FALSE) |

`record_scrape_run` grava `articles_new`; se o banco ainda não tem a coluna (scraper no ar antes da 037), o INSERT falha com `UndefinedColumn`, é refeito sem ela e o aviso sai uma vez por processo. A linha de `scrape_runs` nunca se perde por isso.

### Tabela `scrape_agency_state` (migration 038 do data-platform)

Uma linha por `agency_key` (sem FK para `agencies`: `ansn` e `povosindigenas` não estão lá). Escrita pelo `record_scrape_run` (SAVEPOINT na transação do `scrape_runs`) e, no Passo 3, pelo `/dispatch`. O DDL é copiado à mão em `tests/integration/migration_038.py` (`MIGRATION_038`), usado pelos testes `postgres`.

| Coluna | Notas |
|--------|-------|
| `agency_key` | PK |
| `tier`, `poll_interval_minutes` | `m30`/30, `h2`/120 (padrão), `h6`/360, `d1`/1440 |
| `rate_ewma_per_day`, `tier_updated_at` | Taxa de notícias (EWMA de 28 dias) e quando a faixa foi recalculada |
| `next_due_at` | Próxima raspagem (ou a próxima sonda, com o circuito aberto) |
| `boost_until` | Intervalo pela metade até aqui (notícia nova; h2/h6/d1) |
| `last_dispatched_at` | Último despacho pelo `/dispatch` (Passo 3) |
| `last_attempt_at`, `last_success_at` | Início da última tentativa; marca d'água (início da última com sucesso) |
| `last_new_article_at` | Fim da última tentativa com notícia nova |
| `consecutive_permanent_failures`, `consecutive_transient_failures`, `failure_streak_started_at` | Falhas desde o último sucesso |
| `circuit_state`, `circuit_opened_at`, `circuit_failed_probes` | `closed`/`open`/`half_open` |
| `last_error_category`, `last_error_message` | Último erro (500 caracteres); ficam depois do sucesso |
| `created_at`, `updated_at` | |

`scrape_dispatch_meta` (chave-valor do `/dispatch`) vem na mesma migration.

## Variáveis de Ambiente (API / Cloud Run)

```bash
DATABASE_URL=postgresql://...        # Connection string (prioridade 1)
LOG_LEVEL=INFO                       # Loguru level
LOG_FORMAT=json                      # json (padrão, Cloud Logging) ou text (local e testes)
PUBSUB_TOPIC_NEWS_SCRAPED=projects/{project}/topics/dgb.news.scraped  # Opcional
PUBSUB_TOPIC_OPS_ALERTS=projects/{project}/topics/dgb.ops.alerts      # Alertas do disjuntor; sem ela, só log
SCRAPE_DISPATCH_MODE=off             # off (padrão), dry_run ou live; no Passo 2 só aparece em GET /agencies/state
```

`PUBSUB_TOPIC_OPS_ALERTS` vem do Terraform (repo `infra`, PR I-1), com `roles/pubsub.publisher` da SA do scraper-api no tópico `dgb.ops.alerts`.

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
- **Postgres descartável** (`tests/integration/test_postgres_rescrape.py`, `test_postgres_health_checks.py`, `test_postgres_scrape_runs.py`, este com `scrape_runs` antes e depois da migration 037, `test_postgres_agency_state.py`, com e sem a 038, e `test_postgres_ebc.py`, a EBC de ponta a ponta, inclusive pela rota `/scrape/ebc` com o estado por agência) — marcados com `integration` e `postgres`; pulados sem `SCRAPER_TEST_POSTGRES_URL`. Cada execução cria e remove um schema próprio (`scraper_it_*`) com DDL, então a guarda (`tests/integration/postgres_guard.py`) é dupla, porque "host local" não basta (o Cloud SQL Proxy de produção escuta em `127.0.0.1:5432`):
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

Na VM, o container `dag-sync` espelha o bucket a cada 60 s e o dag-processor reparseia cada arquivo a cada 180 s: uma mudança numa DAG existente pode levar até ~4 min para valer. A lista de buckets é o default da reusable workflow (tag `v3`); este repo não a define. O bucket do Composer sai desse default na reusable-workflows#7: até a tag `v3` apontar para essa versão, o deploy também publica nele, e apagar o bucket antes disso faz o deploy falhar. O nome `composer-deploy-dags.yaml` ficou do Composer.

### Ordem de deploy (Passo 1 da rearquitetura da ingestão, `_plan/rearquitetura-ingestao.md`)

1. Migration 037 do data-platform (`scrape_runs.articles_new`) aplicada em produção (workflow `db-migrate`). Conferir antes do merge (somente leitura): `SELECT 1 FROM information_schema.columns WHERE table_name = 'scrape_runs' AND column_name = 'articles_new'` devolve uma linha. Só o INSERT do scraper-api tolera a falta da coluna. As DAGs a referenciam sem fallback e sobem no mesmo merge; sem a 037:
   - `check_stale_agencies` falha com `UndefinedColumn` (depois do retry) e a `send_alerts` (`trigger_rule` padrão `all_success`) fica `upstream_failed`: o **monitor inteiro fica mudo**, inclusive "Agências Sem Execução" e "Falhas consecutivas", não só a seção de stale;
   - o `scraper_coverage_report` falha (a query de cobertura soma `articles_new`).
2. Tag `v3` do reusable-workflows publicada.
3. Merge do scraper: a API (Cloud Run) e as DAGs sobem juntas. As DAGs novas também funcionam com a API antiga (sem `retryable`, tudo repete, como antes).

### Ordem de deploy (Passo 2: estado por agência)

1. Migration 038 do data-platform (`scrape_agency_state`) aplicada em produção (`db-migrate`, dry-run e depois apply). Sem ela o scraper-api tolera (só o savepoint do estado é desfeito, `scrape_runs` continua sendo gravado, a janela cai para ontem a hoje e não há disjuntor), mas nada do Passo 2 funciona.
2. infra PR I-1 aplicado: `PUBSUB_TOPIC_OPS_ALERTS` e `roles/pubsub.publisher` do scraper-api em `dgb.ops.alerts`. Sem ele os alertas do disjuntor só vão para o log.
3. Merge do scraper fora da janela :00–:10 das horas pares: a API e as DAGs sobem no mesmo push. Uma DAG nova contra a API antiga recebe 422 (falta `start_date`) e entra em retry (5 min) até a API nova subir.
4. Na execução seguinte, conferir `scrape_agency_state` atualizada e `mec`/`antaq` abrindo o circuito depois de 2 execuções, com 2 alertas 🔴.
5. A partir deste merge o re-scrape não republica mais o artigo sem tema (ver "Pub/Sub Events"): até o reconciliador do `/dispatch` (Passo 3) estar em `live`, um enriquecimento que falhar só é refeito à mão.

## Desenvolvimento Local

```bash
# Instalar dependências
poetry install

# Variáveis de ambiente mínimas (apenas DATABASE_URL é obrigatória)
export DATABASE_URL=postgresql://user:pass@localhost:5432/destaquesgovbr
export LOG_FORMAT=text  # logs legíveis no terminal (o padrão é JSON, para o Cloud Logging)
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
