"""
DAG de monitoramento de saude do scraper.

Roda 30 min depois de cada execucao do scraping (``30 6-22/2 * * *``: 06h30, 08h30 ...
22h30, em horario de Brasilia; nao roda na madrugada sem scraping) e verifica:
1. Agencias ativas sem nenhuma execucao (scrape_run) em X horas: a task mapeada da
   agencia na DAG scrape_agencies nao chamou a API (DAG pausada ou ausente, DagRun
   travado, task que nao rodou)
2. Agencias com N falhas consecutivas, ou com erro permanente (sem retry) na ultima execucao
3. Agencias sem noticias em X horas

4. Agencias com o disjuntor aberto em scrape_agency_state (circuito open/half_open;
   migration 038 do data-platform)

As agencias da secao 4 saem das secoes 1 e 2: a API pula a agencia (sem scrape_run). O
alerta de abertura do circuito (dgb.ops.alerts, pelo scraper-api) sai uma vez so, sem
retry; a secao 4 entra no resumo do dedup, entao a agencia fora da raspagem regular volta a
ser lembrada a cada scraper_alert_repeat_hours. Sem a tabela, nada muda.

Envia o alerta agregado via notify.send_alert (Telegram > Pub/Sub dgb.ops.alerts >
webhook > log), sem repetir o mesmo alerta a cada execucao: ver send_alerts.
"""

import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone

import yaml
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

# Mesmo arquivo que dags/scrape_agencies.py usa para gerar as tasks mapeadas.
_AGENCIES_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config", "site_urls.yaml")
# O scraping roda a cada 2 h; 3 h = 2 h + 1 h de folga. As 06h30 a ultima execucao antes
# da das 06h e a das 22h do dia anterior, entao ali basta perder a das 06h para alertar.
_DEFAULT_SILENT_HOURS = 3
# Janela para as falhas consecutivas: cobre 3 execucoes de 2 em 2 h. Erro passageiro grava
# ate 3 scrape_runs de erro por execucao, um por tentativa; erro permanente grava 1 (a task
# falha sem retry) e por isso basta na execucao mais recente (_CONSECUTIVE_FAILURES_QUERY).
_DEFAULT_FAILURE_WINDOW_HOURS = 6
_DEFAULT_FAILURE_THRESHOLD = 3
# Dedup: o mesmo alerta (mesmas agencias sem execucao e com falhas) so e reenviado
# depois deste intervalo.
_DEFAULT_ALERT_REPEAT_HOURS = 24
# Estado do ultimo alerta enviado, em JSON. Fica no metastore: com o Secret Manager como
# backend de Variables, o secret inexistente cai no metastore.
_ALERT_STATE_VARIABLE = "scraper_monitor_last_alert"
_ALERT_TITLE = "Scraper: saúde das agências"
_ALERT_SOURCE = "monitor_scraping_health (Airflow)"
# Uma pane global (ex.: Composer sem acesso a API) poe as ~150 agencias numa secao.
_MAX_LINES_PER_SECTION = 25
# O Telegram recusa texto acima de 4096 caracteres e, sem Telegram, a linha
# "Alert (log-only): ..." e cortada em 4096 no Cloud Logging (visto em out/2026,
# quando o fim do alerta sumia). 4000 deixa folga para o prefixo.
_MAX_MESSAGE_CHARS = 4000
_TRUNCATED_MARKER = "... (alerta truncado; lista completa no log da task)"

# Última notícia NOVA de cada agência (articles_new, migration 037 do data-platform; linhas
# anteriores a ela têm NULL e caem no articles_saved, que soma as atualizadas por re-scrape).
#
# NOTE: This query is duplicated in src/govbr_scraper/monitoring/health_checks.py
# (STALE_AGENCIES_QUERY). tests/unit/test_dag_monitor_health.py checks they match.
_STALE_AGENCIES_QUERY = """
    SELECT
        agency_key,
        MAX(scraped_at) FILTER (
            WHERE status = 'success' AND COALESCE(articles_new, articles_saved) > 0
        ) AS last_success_at
    FROM scrape_runs
    WHERE scraped_at > NOW() - INTERVAL '90 days'
    GROUP BY agency_key
    HAVING MAX(scraped_at) FILTER (
               WHERE status = 'success' AND COALESCE(articles_new, articles_saved) > 0
           ) < NOW() - INTERVAL '1 hour' * %(stale_hours)s
       AND MAX(scraped_at) FILTER (
               WHERE status = 'success' AND COALESCE(articles_new, articles_saved) > 0
           ) IS NOT NULL
"""

# Agencias cujos ultimos `threshold` scrape_runs da janela sao todos erro (erro passageiro:
# ate 3 linhas por execucao, uma por tentativa) OU cujo scrape_run mais recente e um erro
# permanente (url_broken, access_denied, html_changed, anti_bot: a task falha sem retry e grava
# 1 linha por execucao; sem esse ramo a agencia quebrada so entraria depois de 3 execucoes e
# sumiria toda manha, com 1 linha as 06h30 e 2 as 08h30). A lista e a
# PERMANENT_ERROR_CATEGORIES de src/govbr_scraper/models/monitoring.py.
#
# NOTE: This query is duplicated in src/govbr_scraper/monitoring/health_checks.py
# (CONSECUTIVE_FAILURES_QUERY). tests/unit/test_dag_monitor_health.py checks they match.
_CONSECUTIVE_FAILURES_QUERY = """
    WITH ranked AS (
        SELECT agency_key, status, error_category, scraped_at,
               ROW_NUMBER() OVER (PARTITION BY agency_key ORDER BY scraped_at DESC) AS rn
        FROM scrape_runs
        WHERE scraped_at > NOW() - INTERVAL '1 hour' * %(window_hours)s
    ),
    recent AS (
        SELECT agency_key, status, error_category, scraped_at, rn
        FROM ranked
        WHERE rn <= %(threshold)s
    )
    SELECT
        agency_key,
        COALESCE(MIN(rn) FILTER (WHERE status <> 'error') - 1, COUNT(*)) AS consecutive_failures,
        MAX(error_category) FILTER (WHERE rn = 1) AS last_error,
        MAX(scraped_at) AS last_failure_at
    FROM recent
    GROUP BY agency_key
    HAVING (COUNT(*) = %(threshold)s AND COUNT(*) FILTER (WHERE status = 'error') = %(threshold)s)
        OR BOOL_OR(
               rn = 1 AND status = 'error'
               AND error_category IN ('url_broken', 'access_denied', 'html_changed', 'anti_bot')
           )
"""

# NOTE: This query is duplicated in src/govbr_scraper/monitoring/health_checks.py
# (SILENT_AGENCIES_QUERY). tests/unit/test_dag_monitor_health.py checks they match.
_SILENT_AGENCIES_QUERY = """
    SELECT k.agency_key, last_run.scraped_at AS last_run_at
    FROM unnest(%(agencies)s::text[]) AS k(agency_key)
    LEFT JOIN LATERAL (
        SELECT sr.scraped_at
        FROM scrape_runs sr
        WHERE sr.agency_key = k.agency_key
        ORDER BY sr.scraped_at DESC LIMIT 1
    ) AS last_run ON TRUE
    WHERE last_run.scraped_at IS NULL
       OR last_run.scraped_at < NOW() - INTERVAL '1 hour' * %(silent_hours)s
    ORDER BY k.agency_key
"""


# Agencias com o disjuntor aberto (open ou half_open) em scrape_agency_state (migration 038
# do data-platform). Saem de "Agencias Sem Execucao" e das falhas consecutivas (a API pula a
# agencia sem gravar scrape_run) e vao para a secao "Circuito Aberto". Sem a tabela, nada
# muda (_open_circuits devolve vazio).
#
# NOTE: This query is duplicated in src/govbr_scraper/monitoring/health_checks.py
# (OPEN_CIRCUIT_AGENCIES_QUERY). tests/unit/test_dag_monitor_health.py checks they match.
_OPEN_CIRCUIT_AGENCIES_QUERY = """
    SELECT agency_key, circuit_state, circuit_opened_at, next_due_at, last_error_category
    FROM scrape_agency_state
    WHERE circuit_state <> 'closed'
    ORDER BY agency_key
"""

# Fontes EBC ativas: as mesmas de dags/scrape_ebc.py (EBC_SOURCES). Tambem tem linha em
# scrape_agency_state; o site_urls.yaml so tem as gov.br.
_EBC_SOURCES = ("agencia_brasil", "tvbrasil")


def _open_circuits(conn) -> list[dict]:
    """Agencias com o disjuntor aberto (linhas da query); vazio sem scrape_agency_state.

    Tabela ausente (038 nao aplicada): rollback da transacao abortada e nenhuma exclusao.
    """
    import psycopg2.errors
    from psycopg2.extras import RealDictCursor

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(_OPEN_CIRCUIT_AGENCIES_QUERY)
            return [dict(row) for row in cur.fetchall()]
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        logger.info("scrape_agency_state ausente (migration 038): sem filtro de circuito aberto")
        return []


def _open_circuit_agencies(conn) -> set[str]:
    """Siglas das agencias com o disjuntor aberto; vazio sem scrape_agency_state."""
    return {row["agency_key"] for row in _open_circuits(conn)}


def _without_open_circuits(rows: list[dict], open_circuits: set[str], section: str) -> list[dict]:
    """Tira das linhas as agencias com o disjuntor aberto (vao para "Circuito Aberto")."""
    kept = [r for r in rows if r["agency_key"] not in open_circuits]
    removed = sorted({r["agency_key"] for r in rows} & open_circuits)
    if removed:
        logger.info(
            "%s: %d agencia(s) com o circuito aberto fora do alerta: %s",
            section,
            len(removed),
            ", ".join(_sanitize(k) for k in removed),
        )
    return kept


def _sanitize(value) -> str:
    """Remove control characters from a value before using in logs/messages."""
    if not value:
        return ""
    return re.sub(r"[\x00-\x1f\x7f-\x9f]", "", str(value))


def _load_active_agencies(config_path: str = _AGENCIES_CONFIG_PATH) -> list[str]:
    """Agencias ativas do site_urls.yaml, as mesmas que viram task mapeada em scrape_agencies.py."""
    with open(config_path) as f:
        agencies = yaml.safe_load(f)["agencies"]
    return sorted(key for key, data in agencies.items() if data.get("active", True))


def _get_silent_hours() -> int:
    """Janela (horas) sem nenhum scrape_run para considerar a agencia silenciosa."""
    from airflow.models import Variable

    hours = int(Variable.get("scraper_silent_hours", default_var=_DEFAULT_SILENT_HOURS))
    # Janela 0 marcaria todas as agencias como silenciosas.
    return max(1, hours)


def _get_repeat_hours() -> int:
    """Horas ate reenviar um alerta igual ao ultimo enviado (0 = sem dedup)."""
    from airflow.models import Variable

    raw = Variable.get("scraper_alert_repeat_hours", default_var=_DEFAULT_ALERT_REPEAT_HOURS)
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        logger.warning(
            "scraper_alert_repeat_hours invalida (%r); usando %dh", raw, _DEFAULT_ALERT_REPEAT_HOURS
        )
        return _DEFAULT_ALERT_REPEAT_HOURS


def _alert_digest(
    failures: list[dict], silent: list[dict], open_circuits: list[dict] | None = None
) -> dict[str, list[str]]:
    """Resumo do que dispara um envio: secao -> chaves de agencia ordenadas, sem horarios.

    So entram as secoes acionaveis (sem execucao, falhas consecutivas e circuito aberto). As
    agencias sem noticias (~48 cronicas) vao na mensagem, mas mudanca so nelas nao reenvia o
    alerta.
    """
    return {
        "silent": sorted({_sanitize(r["agency_key"]) for r in silent or []}),
        "failures": sorted({_sanitize(r["agency_key"]) for r in failures or []}),
        "open_circuits": sorted({_sanitize(r["agency_key"]) for r in open_circuits or []}),
    }


def _should_send(
    digest: dict, state: dict | None, now: datetime, repeat_hours: int = _DEFAULT_ALERT_REPEAT_HOURS
) -> bool:
    """Envia se o resumo mudou desde o ultimo envio ou se o ultimo envio tem repeat_hours+.

    Estado ausente, vazio ou ilegivel conta como "nunca enviado".
    """
    if not state or state.get("digest") != digest:
        return True
    try:
        sent_at = datetime.fromisoformat(state["sent_at"])
    except (KeyError, TypeError, ValueError):
        return True
    if sent_at.tzinfo is None:
        sent_at = sent_at.replace(tzinfo=timezone.utc)  # noqa: UP017
    return now - sent_at >= timedelta(hours=repeat_hours)


def _read_alert_state() -> dict | None:
    """Estado do ultimo alerta enviado ({"digest", "sent_at"}), ou None.

    Erro de leitura nao pode impedir o alerta: loga e devolve None (envia).
    """
    from airflow.models import Variable

    try:
        raw = Variable.get(_ALERT_STATE_VARIABLE, default_var=None)
        if not raw:
            return None
        state = json.loads(raw)
        return state if isinstance(state, dict) else None
    except Exception as e:
        logger.warning(
            "Falha ao ler %s (%s); o alerta sera enviado", _ALERT_STATE_VARIABLE, type(e).__name__
        )
        return None


def _write_alert_state(state: dict) -> None:
    """Grava o estado do ultimo alerta. Erro de escrita so e logado."""
    from airflow.models import Variable

    try:
        Variable.set(_ALERT_STATE_VARIABLE, json.dumps(state, sort_keys=True))
    except Exception as e:
        logger.warning("Falha ao gravar %s (%s)", _ALERT_STATE_VARIABLE, type(e).__name__)


def _capped(lines: list[str]) -> list[str]:
    if len(lines) <= _MAX_LINES_PER_SECTION:
        return lines
    hidden = len(lines) - _MAX_LINES_PER_SECTION
    return lines[:_MAX_LINES_PER_SECTION] + [f"... e mais {hidden}"]


def _fit_message(message: str) -> str:
    """Corta a mensagem em fim de linha para caber em _MAX_MESSAGE_CHARS.

    Cada linha fecha as proprias tags <b>, entao cortar entre linhas nao quebra
    o HTML do Telegram.
    """
    if len(message) <= _MAX_MESSAGE_CHARS:
        return message
    budget = _MAX_MESSAGE_CHARS - len(_TRUNCATED_MARKER) - 1
    kept: list[str] = []
    size = 0
    for line in message.split("\n"):
        extra = len(line) + (1 if kept else 0)
        if size + extra > budget:
            break
        kept.append(line)
        size += extra
    return "\n".join(kept + [_TRUNCATED_MARKER])


def _build_alert_message(
    failures: list[dict],
    stale: list[dict],
    silent: list[dict] | None = None,
    silent_hours: int = _DEFAULT_SILENT_HOURS,
    open_circuits: list[dict] | None = None,
) -> str | None:
    """Monta o alerta agregado (HTML do Telegram, so com <b>), ou None se nao ha o que alertar.

    A secao de agencias silenciosas vem primeiro: e a unica em que nada esta sendo
    raspado e nenhuma outra checagem enxerga o problema, e o fim de um alerta longo
    se perde (limite de 4096 caracteres).
    """
    parts = []
    if silent:
        header = (
            "<b>Alerta: Agencias Sem Execucao</b>\n"
            f"{len(silent)} agencia(s) ativa(s) sem nenhum scrape_run nas ultimas "
            f"{silent_hours}h: a task mapeada da agencia na DAG scrape_agencies nao "
            "chamou a API (DAG pausada ou ausente, DagRun travado em running, task que "
            "nao rodou ou falhou antes da chamada) ou a chamada nao chega a gravar o "
            "resultado. Ver docs/runbook.md, secao 'Agencias Sem Execucao (scrape_agencies)'."
        )
        lines = []
        for r in silent:
            key = _sanitize(r["agency_key"])
            if r.get("last_run_at") is None:
                lines.append(f"- <b>{key}</b>: nenhuma execucao registrada em scrape_runs")
            else:
                lines.append(f"- <b>{key}</b>: ultima execucao em {r['last_run_at']}")
        parts.append(header + "\n\n" + "\n".join(_capped(lines)))

    if failures:
        lines = [
            f"- <b>{_sanitize(r['agency_key'])}</b>: {_sanitize(r['last_error'])} "
            f"(ultima falha: {r['last_failure_at']})"
            for r in failures
        ]
        parts.append(
            "<b>Alerta: Falhas Consecutivas no Scraper</b>\n\n" + "\n".join(_capped(lines))
        )

    if open_circuits:
        header = (
            "<b>Alerta: Circuito Aberto (disjuntor)</b>\n"
            f"{len(open_circuits)} agencia(s) fora da raspagem regular: a API as pula e so as "
            "sondas (2h, 6h, 24h e depois a cada 24h) tentam de novo. Ver docs/runbook.md, "
            "secao 'Disjuntor e Marca d'Agua'."
        )
        lines = []
        for r in open_circuits:
            line = (
                f"- <b>{_sanitize(r['agency_key'])}</b>: "
                f"{_sanitize(r.get('last_error_category')) or 'sem categoria'}"
            )
            if r.get("circuit_opened_at") is not None:
                line += f", aberto em {r['circuit_opened_at']}"
            if r.get("next_due_at") is not None:
                line += f", proxima sonda em {r['next_due_at']}"
            lines.append(line)
        parts.append(header + "\n\n" + "\n".join(_capped(lines)))

    if stale:
        lines = [
            f"- <b>{_sanitize(r['agency_key'])}</b>: ultima noticia em {r['last_success_at']}"
            for r in stale
        ]
        parts.append("<b>Alerta: Agencias Sem Noticias</b>\n\n" + "\n".join(_capped(lines)))

    if not parts:
        return None
    return _fit_message("\n\n".join(parts))


@dag(
    dag_id="monitor_scraping_health",
    description=(
        "Monitora saude do scraper: agencias sem execucao, falhas consecutivas, "
        "circuito aberto e agencias sem noticias"
    ),
    schedule="30 6-22/2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["scraper", "monitoring"],
    default_args={
        "owner": "scraper",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=5),
    },
)
def monitor_scraping_health_dag():
    @task
    def check_consecutive_failures() -> list[dict]:
        """Verifica agencias com falhas consecutivas ou com erro permanente na ultima execucao.

        Agencias com o disjuntor aberto ficam de fora (o alerta do circuito cobre).
        """
        import psycopg2
        from airflow.models import Variable
        from psycopg2.extras import RealDictCursor

        threshold = int(
            Variable.get(
                "scraper_consecutive_failure_threshold", default_var=_DEFAULT_FAILURE_THRESHOLD
            )
        )
        window_hours = int(
            Variable.get("scraper_failure_window_hours", default_var=_DEFAULT_FAILURE_WINDOW_HOURS)
        )
        database_url = Variable.get("scraper_database_url", default_var="")
        if not database_url:
            raise ValueError("Missing required Airflow Variable: scraper_database_url")

        conn = psycopg2.connect(database_url)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    _CONSECUTIVE_FAILURES_QUERY,
                    {"threshold": threshold, "window_hours": window_hours},
                )
                results = [dict(row) for row in cur.fetchall()]
            open_circuits = _open_circuit_agencies(conn)
        finally:
            conn.close()
        results = _without_open_circuits(results, open_circuits, "Falhas consecutivas")

        if results:
            logger.warning(f"Agencias com {threshold}+ falhas consecutivas: {len(results)}")
            for r in results:
                logger.warning(
                    f"  {_sanitize(r['agency_key'])}: {_sanitize(r['last_error'])} "
                    f"(ultima falha: {r['last_failure_at']})"
                )
        else:
            logger.info("Nenhuma agencia com falhas consecutivas.")

        return results

    @task
    def check_stale_agencies() -> list[dict]:
        """Verifica agencias sem noticias novas em X horas (articles_new; antes, articles_saved)."""
        import psycopg2
        from airflow.models import Variable
        from psycopg2.extras import RealDictCursor

        stale_hours = int(Variable.get("scraper_stale_hours", default_var=24))
        database_url = Variable.get("scraper_database_url", default_var="")
        if not database_url:
            raise ValueError("Missing required Airflow Variable: scraper_database_url")

        conn = psycopg2.connect(database_url)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(_STALE_AGENCIES_QUERY, {"stale_hours": stale_hours})
                results = [dict(row) for row in cur.fetchall()]
        finally:
            conn.close()

        if results:
            logger.warning(f"Agencias sem noticias em {stale_hours}h: {len(results)}")
            for r in results:
                logger.warning(
                    f"  {_sanitize(r['agency_key'])}: ultima noticia em {r['last_success_at']}"
                )
        else:
            logger.info("Todas as agencias com noticias recentes.")

        return results

    @task
    def check_silent_agencies() -> list[dict]:
        """Verifica agencias ativas sem nenhum scrape_run (sucesso ou erro) em X horas.

        Cada chamada da API grava um scrape_run por agencia, com sucesso ou erro.
        Agencia ativa sem nenhum na janela => a task mapeada dela na scrape_agencies
        nao esta chamando a API; todas as agencias de uma vez => a DAG inteira parou.
        Caso real, ainda com uma DAG por agencia (out e nov/2026): DagRuns presos em
        'running' sem task instances, numa DagVersion sem serialized_dag, travaram
        scrape_inma, scrape_inpe, scrape_aids, scrape_patrimonio e scrape_esg
        (max_active_runs=1). As outras checagens partem de scrape_runs e nao enxergam
        esse caso.

        Agencia com o disjuntor aberto nao grava scrape_run (a API a pula ate a sonda) e fica
        de fora: o alerta do circuito cobre.
        """
        import psycopg2
        from airflow.models import Variable
        from psycopg2.extras import RealDictCursor

        silent_hours = _get_silent_hours()
        database_url = Variable.get("scraper_database_url", default_var="")
        if not database_url:
            raise ValueError("Missing required Airflow Variable: scraper_database_url")

        active_agencies = _load_active_agencies()

        conn = psycopg2.connect(database_url)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    _SILENT_AGENCIES_QUERY,
                    {"agencies": active_agencies, "silent_hours": silent_hours},
                )
                results = [dict(row) for row in cur.fetchall()]
            open_circuits = _open_circuit_agencies(conn)
        finally:
            conn.close()
        results = _without_open_circuits(results, open_circuits, "Agencias sem execucao")

        if results:
            logger.warning(
                f"Agencias ativas sem nenhuma execucao em {silent_hours}h: "
                f"{len(results)} de {len(active_agencies)}"
            )
            for r in results:
                logger.warning(
                    f"  {_sanitize(r['agency_key'])}: ultima execucao em {r['last_run_at']}"
                )
        else:
            logger.info(f"Todas as {len(active_agencies)} agencias ativas executaram.")

        return results

    @task
    def check_open_circuits() -> list[dict]:
        """Agencias ativas com o disjuntor aberto (gov.br do site_urls.yaml e fontes EBC).

        Saem de "Agencias Sem Execucao" e das falhas consecutivas e vem para esta secao. O
        alerta de abertura do scraper-api e publicado uma vez so, sem retry: se ele se perde,
        esta secao e o unico aviso de que a agencia esta fora da raspagem regular. Agencia
        desativada no YAML com o circuito aberto fica de fora (nao e mais raspada).
        """
        import psycopg2
        from airflow.models import Variable

        database_url = Variable.get("scraper_database_url", default_var="")
        if not database_url:
            raise ValueError("Missing required Airflow Variable: scraper_database_url")

        active = set(_load_active_agencies()) | set(_EBC_SOURCES)
        conn = psycopg2.connect(database_url)
        try:
            rows = _open_circuits(conn)
        finally:
            conn.close()
        results = [r for r in rows if r["agency_key"] in active]

        if results:
            logger.warning(f"Agencias com o circuito aberto: {len(results)}")
            for r in results:
                logger.warning(
                    f"  {_sanitize(r['agency_key'])}: {_sanitize(r.get('last_error_category'))} "
                    f"(aberto em {r.get('circuit_opened_at')}, proxima sonda em "
                    f"{r.get('next_due_at')})"
                )
        else:
            logger.info("Nenhuma agencia ativa com o circuito aberto.")
        return results

    @task
    def send_alerts(
        failures: list[dict],
        stale: list[dict],
        silent: list[dict],
        open_circuits: list[dict] | None = None,
    ):
        """Envia o alerta agregado (Telegram > Pub/Sub > webhook > log), sem repetir.

        Dedup: o resumo das secoes acionaveis (agencias sem execucao, com falhas
        consecutivas e com o circuito aberto, sem horarios) e comparado com o do ultimo envio,
        guardado na
        Variable scraper_monitor_last_alert. Reenvia quando o resumo muda ou quando o
        ultimo envio tem scraper_alert_repeat_hours (default 24h). Mudanca so nas agencias
        sem noticias nao reenvia. O estado so e gravado quando algum canal entregou: no
        modo log-only, o alerta continua indo para o log em toda execucao.
        """
        from airflow.models import Variable
        from scraper.notify import default_alert_topic, send_alert

        message = _build_alert_message(
            failures, stale, silent, _get_silent_hours(), open_circuits
        )
        state = _read_alert_state()
        if message is None:
            logger.info("Sem alertas para enviar.")
            if state:
                # Problema resolvido: se voltar, o alerta sai na hora.
                _write_alert_state({})
            return

        now = datetime.now(timezone.utc)  # noqa: UP017
        digest = _alert_digest(failures, silent, open_circuits)
        if not _should_send(digest, state, now, _get_repeat_hours()):
            logger.info(
                "Alerta igual ao enviado em %s (sem execucao: %d, falhas: %d, circuito "
                "aberto: %d); nao reenviado.",
                state.get("sent_at"),
                len(digest["silent"]),
                len(digest["failures"]),
                len(digest["open_circuits"]),
            )
            return

        token = Variable.get("scraper_telegram_bot_token", default_var=None)
        chat_id = Variable.get("scraper_telegram_monitor_chat_id", default_var=None)
        webhook = Variable.get("scraper_alert_webhook_url", default_var=None)
        pubsub_topic = Variable.get("scraper_alert_pubsub_topic", default_var=default_alert_topic())

        delivered = send_alert(
            message=message,
            telegram_token=token,
            telegram_chat_id=chat_id,
            webhook_url=webhook,
            pubsub_topic=pubsub_topic,
            title=_ALERT_TITLE,
            severity="warning",
            source=_ALERT_SOURCE,
        )
        if delivered:
            _write_alert_state({"digest": digest, "sent_at": now.isoformat()})

    failures = check_consecutive_failures()
    stale = check_stale_agencies()
    silent = check_silent_agencies()
    open_circuits = check_open_circuits()
    send_alerts(failures, stale, silent, open_circuits)


dag_instance = monitor_scraping_health_dag()
