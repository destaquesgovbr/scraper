"""
Scraper API for DestaquesGovBr.

Lightweight HTTP wrapper around existing CLI scraper commands.
Designed to run on Cloud Run, called by Airflow DAGs, Cloud Tasks and Cloud Scheduler.

/scrape/agencies e /scrape/ebc (DAGs) e /scrape/agency (tarefas do Cloud Tasks) raspam
agência por agência, com o estado por agência (scrape_agency_state, migration 038): sem
datas no corpo, a janela de cada agência vem da marca d'água (desde o último sucesso, até 7
dias ou ~80 notícias da agência); com o circuito aberto e a sonda ainda não vencida, a
agência é pulada (``skipped``), a não ser com ``force``.

/dispatch (Cloud Scheduler a cada 15 min e a DAG scrape_backfill) escolhe as agências
vencidas e cria uma tarefa por agência na fila scrape-govbr (modo em SCRAPE_DISPATCH_MODE).
"""

import json
import logging
import time
from datetime import UTC, date, datetime
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError, field_validator, model_validator

from govbr_scraper.dispatch.config import dispatch_mode_from_env
from govbr_scraper.models.monitoring import UNKNOWN_AGENCY
from govbr_scraper.monitoring.structured_log import (
    OUTCOME_FAILED,
    OUTCOME_SKIPPED,
    OUTCOME_SUCCESS,
    log_dispatch_run,
    log_scrape_attempt,
)
from govbr_scraper.state.model import AgencyState, AttemptContext, Tier
from govbr_scraper.state.report import iso_utc
from govbr_scraper.state.transitions import circuit_gate
from govbr_scraper.state.window import agency_window

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(
    title="DestaquesGovBr Scraper API",
    description="HTTP wrapper for gov.br and EBC news scrapers",
    version="1.0.0",
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Log validation errors with details for debugging."""
    errors = exc.errors()
    truncated = errors[:5]
    suffix = f" (mostrando 5 de {len(errors)})" if len(errors) > 5 else ""
    logger.warning(
        f"Validation error on {request.url.path}: {len(errors)} error(s){suffix} - {truncated}"
    )
    return JSONResponse(
        status_code=422,
        content={"detail": [{k: v for k, v in e.items() if k != "ctx"} for e in errors]},
    )


class _ScrapeRequest(BaseModel):
    # Opcionais. Sem datas, cada agência usa a janela da marca d'água (o estado não indica
    # janela própria: ontem a hoje). Com start_date (end_date padrão = start_date), a janela é
    # explícita: backfill ou recuperação, que não mexe no estado da agência.
    start_date: date | None = None
    end_date: date | None = None
    agencies: list[str] | None = None
    allow_update: bool = False
    # Aceito por compatibilidade e ignorado: a API raspa uma agência por vez, sempre em modo
    # sequencial (o sucesso só é gravado depois das notícias).
    sequential: bool = True
    # Ignora o disjuntor: raspa mesmo com o circuito aberto.
    force: bool = False

    @model_validator(mode="after")
    def _end_date_requires_start_date(self):
        if self.end_date is not None and self.start_date is None:
            raise ValueError(
                "end_date exige start_date (janela explícita); sem as duas datas vale a "
                "marca d'água de cada agência"
            )
        return self


class ScrapeAgenciesRequest(_ScrapeRequest):
    pass


class ScrapeEBCRequest(_ScrapeRequest):
    pass


class AgencyError(BaseModel):
    agency: str
    error: str
    # Mesma categoria gravada em scrape_runs.error_category (ou "unknown_agency": agência
    # fora do YAML ou inativa, sem linha em scrape_runs). None em erro sem classificação.
    category: str | None = None
    # False em erro permanente (url_broken, access_denied, html_changed, anti_bot,
    # unknown_agency): repetir não adianta e a DAG falha sem retry.
    retryable: bool = True


class SkippedAgency(BaseModel):
    agency: str
    reason: str  # "circuit_open": circuito aberto e a sonda ainda não venceu
    next_probe_at: str | None = None  # ISO 8601 (UTC)


class ScrapeResponse(BaseModel):
    status: str  # completed | partial | failed | skipped (todas puladas pelo disjuntor)
    # Janela explícita, ou a menor/maior data das janelas por agência (None se nenhuma raspou).
    start_date: str | None = None
    end_date: str | None = None
    articles_scraped: int = 0
    articles_saved: int = 0  # linhas gravadas: inseridas + atualizadas (re-scrape)
    articles_new: int = 0  # só as inseridas (notícias novas)
    articles_skipped: int = 0  # conteúdo inválido (scraper#65) ou redatado: não gravados
    agencies_processed: list[str] = []
    errors: list[AgencyError] = []
    # Agências não raspadas pelo disjuntor (sem linha em scrape_runs).
    skipped: list[SkippedAgency] = []
    message: str


@app.get("/health")
def health():
    return {"status": "ok"}


# trigger do scrape_attempt: /scrape/agencies e /scrape/ebc são chamados pelas DAGs do
# Airflow; /scrape/agency, pelas tarefas do Cloud Tasks (tick do /dispatch ou backfill).
_TRIGGER_DAG = "dag"
_TRIGGER_TASK = "task"
_TRIGGER_BACKFILL = "backfill"


def _tier_label(state: AgencyState | None, states_available: bool) -> str | None:
    """Faixa da agência para o log: a do estado; sem linha, a padrão (h2) que o
    record_scrape_run vai criar; com o estado indisponível, None."""
    if state is not None:
        return state.tier.value
    return Tier.H2.value if states_available else None


def _attempt_fields(agency: str, metrics: dict) -> dict[str, Any]:
    """outcome/category/retryable/articles_new do scrape_attempt a partir das métricas."""
    if agency in metrics.get("agencies_processed", []):
        return {
            "outcome": OUTCOME_SUCCESS,
            "category": None,
            "retryable": None,
            "articles_new": metrics.get("articles_new", 0),
        }
    errors = metrics.get("errors", [])
    own = [e for e in errors if e.get("agency") == agency] or errors
    error = own[0] if own else {}
    return {
        "outcome": OUTCOME_FAILED,
        "category": error.get("category"),
        "retryable": error.get("retryable", True),
        "articles_new": None,
    }


def _scrape_each_agency(
    req: _ScrapeRequest,
    storage,
    manager,
    yaml_file: str,
    *,
    trigger: str = _TRIGGER_DAG,
    log_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Raspa agência por agência, cada uma com a própria janela, passando pelo disjuntor.

    ``trigger`` e ``log_extra`` (ex.: cabeçalhos do Cloud Tasks) vão no ``scrape_attempt``.
    Devolve as métricas somadas, a janela da resposta e as agências puladas.
    """
    from govbr_scraper.scrapers.yaml_config import active_agency_keys

    extra = log_extra or {}
    explicit = req.start_date is not None
    keys = list(dict.fromkeys(req.agencies)) if req.agencies else active_agency_keys(yaml_file)
    states = storage.load_agency_states()
    totals: dict[str, Any] = {
        "articles_scraped": 0,
        "articles_saved": 0,
        "articles_new": 0,
        "articles_skipped": 0,
        "agencies_processed": [],
        "errors": [],
        "skipped": [],
        "windows": [],
    }
    for key in keys:
        now = datetime.now(UTC)
        state = states.get(key) if states is not None else None
        tier = _tier_label(state, states is not None)
        decision = circuit_gate(state, now, force=req.force)
        if decision.skip:
            next_probe_at = iso_utc(decision.next_probe_at)
            totals["skipped"].append(
                SkippedAgency(agency=key, reason="circuit_open", next_probe_at=next_probe_at)
            )
            log_scrape_attempt(
                agency=key,
                outcome=OUTCOME_SKIPPED,
                tier=tier,
                explicit_window=explicit,
                trigger=trigger,
                reason="circuit_open",
                **extra,
                next_probe_at=next_probe_at,
            )
            continue
        # Sonda de um circuito aberto já vencido: half_open. Com janela explícita a tentativa
        # não mexe no estado, então o circuito também não é marcado.
        if decision.probe and not explicit:
            storage.mark_agency_probe(key, now)
        truncated = False
        if explicit:
            start, end = req.start_date, req.end_date or req.start_date
        else:
            # Estado indisponível ou agência sem linha: ontem a hoje. Com a marca d'água, a
            # janela volta no máximo os dias que cabem em ~80 notícias da agência.
            start, end, truncated = agency_window(now, state)
            if truncated:
                logger.warning(
                    f"{key}: janela automática limitada a {start}..{end} (marca d'água em "
                    f"{iso_utc(state.last_success_at)}); os dias anteriores ficam para a "
                    "recuperação com datas explícitas (runbook)"
                )
        totals["windows"].append((start, end))
        started = time.monotonic()
        try:
            # Sempre sequencial: é uma agência por chamada, então o modo em lote não traz
            # ganho, e nele o scrape_run de sucesso (que avança a marca d'água) é gravado
            # antes do INSERT das notícias. Um erro no INSERT perderia a janela.
            metrics = manager.run_scraper(
                agencies=[key],
                min_date=start.isoformat(),
                max_date=end.isoformat(),
                sequential=True,
                allow_update=req.allow_update,
            )
        except Exception:
            log_scrape_attempt(
                agency=key,
                outcome=OUTCOME_FAILED,
                category="unknown",
                retryable=True,
                duration_s=time.monotonic() - started,
                tier=tier,
                explicit_window=explicit,
                trigger=trigger,
                **extra,
            )
            raise
        log_scrape_attempt(
            agency=key,
            duration_s=time.monotonic() - started,
            tier=tier,
            explicit_window=explicit,
            trigger=trigger,
            probe=decision.probe,
            window_start=start.isoformat(),
            window_end=end.isoformat(),
            window_truncated=truncated,
            **_attempt_fields(key, metrics),
            **extra,
        )
        for field in ("articles_scraped", "articles_saved", "articles_new", "articles_skipped"):
            totals[field] += metrics.get(field, 0) or 0
        totals["agencies_processed"].extend(metrics.get("agencies_processed", []))
        totals["errors"].extend(metrics.get("errors", []))
    return totals


def _scrape_response(req: _ScrapeRequest, totals: dict, failed_message: str) -> JSONResponse:
    """Resposta de /scrape/*: os status HTTP de sempre, mais o 200 de "todas puladas"."""
    errors = [AgencyError(**e) for e in totals["errors"]]
    processed = totals["agencies_processed"]
    skipped = totals["skipped"]
    if errors and not processed:
        status, message, http_status = "failed", failed_message.format(errors[0].error), 500
    elif errors:
        status = "partial"
        message = f"Completed with {len(errors)} error(s)"
        http_status = 207  # Multi-Status
    elif skipped and not processed:
        status = "skipped"
        message = f"{len(skipped)} agência(s) pulada(s): circuito aberto"
        http_status = 200
    else:
        status, message, http_status = "completed", "Scraping completed", 200

    if req.start_date is not None:
        start_date = req.start_date.isoformat()
        end_date = (req.end_date or req.start_date).isoformat()
    elif totals["windows"]:
        start_date = min(w[0] for w in totals["windows"]).isoformat()
        end_date = max(w[1] for w in totals["windows"]).isoformat()
    else:
        start_date = end_date = None

    response = ScrapeResponse(
        status=status,
        start_date=start_date,
        end_date=end_date,
        articles_scraped=totals["articles_scraped"],
        articles_saved=totals["articles_saved"],
        articles_new=totals["articles_new"],
        articles_skipped=totals["articles_skipped"],
        agencies_processed=processed,
        errors=errors,
        skipped=skipped,
        message=message,
    )
    return JSONResponse(content=response.model_dump(), status_code=http_status)


def _describe_request(req: _ScrapeRequest) -> str:
    window = (
        f"{req.start_date} a {req.end_date or req.start_date}" if req.start_date else "marca d'água"
    )
    force = " (force)" if req.force else ""
    return f"{req.agencies or 'ALL'}, janela {window}{force}"


@app.post("/scrape/agencies", response_model=ScrapeResponse)
def scrape_agencies(req: ScrapeAgenciesRequest):
    from govbr_scraper.scrapers.scrape_manager import ScrapeManager
    from govbr_scraper.storage import StorageAdapter

    logger.info(f"Scraping agencies: {_describe_request(req)}")

    storage = None
    try:
        storage = StorageAdapter(
            attempt_context=AttemptContext(
                explicit_window=req.start_date is not None, forced=req.force
            )
        )
        manager = ScrapeManager(storage)
        totals = _scrape_each_agency(req, storage, manager, "site_urls.yaml")
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if storage is not None:
            storage.close()

    return _scrape_response(req, totals, "All agencies failed")


# Allowlist de domínios aceitos pelo /verify/integrity. Mitiga SSRF contra o
# metadata server do GCP (169.254.169.254) e sondagem da rede interna do Cloud
# Run. Manter em sincronia com site_urls.yaml (gov.br) e ebc_webscraper.py (EBC).
_ALLOWED_URL_PREFIXES = (
    "https://www.gov.br/",
    "https://agenciabrasil.ebc.com.br/",
    "https://imagens.ebc.com.br/",
    "https://memoria.ebc.com.br/",
    "https://tvbrasil.ebc.com.br/",
    "https://live.staticflickr.com/",
    "https://storage.googleapis.com/destaquesgovbr-thumbnails/",
)


def _validate_allowed_url(value: str | None) -> str | None:
    if not value:
        return None
    if not any(value.startswith(prefix) for prefix in _ALLOWED_URL_PREFIXES):
        raise ValueError(
            "URL fora da allowlist; domínios aceitos: "
            + ", ".join(p.rstrip("/") for p in _ALLOWED_URL_PREFIXES)
        )
    return value


class VerifyArticle(BaseModel):
    unique_id: str
    url: str | None = None
    image_url: str | None = None
    content_hash: str | None = None
    source_etag: str | None = None
    check_content: bool = False

    @field_validator("url", "image_url")
    @classmethod
    def _check_url_allowlist(cls, value: str | None) -> str | None:
        return _validate_allowed_url(value)


class VerifyRequest(BaseModel):
    articles: list[VerifyArticle]


@app.post("/verify/integrity")
def verify_integrity(req: VerifyRequest):
    from govbr_scraper.integrity.service import verify_batch

    logger.info(f"Verificando integridade de {len(req.articles)} artigos")

    try:
        result = verify_batch([a.model_dump() for a in req.articles])
    except Exception as e:
        logger.error(f"Verificação de integridade falhou: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return result


@app.post("/scrape/ebc", response_model=ScrapeResponse)
def scrape_ebc(req: ScrapeEBCRequest):
    from govbr_scraper.scrapers.ebc_scrape_manager import EBCScrapeManager
    from govbr_scraper.storage import StorageAdapter

    logger.info(f"Scraping EBC agencies: {_describe_request(req)}")

    storage = None
    try:
        storage = StorageAdapter(
            attempt_context=AttemptContext(
                explicit_window=req.start_date is not None, forced=req.force
            )
        )
        manager = EBCScrapeManager(storage)
        # Sem agencies: as duas fontes ativas (agencia_brasil e tvbrasil), cada uma com a
        # própria janela e o próprio disjuntor.
        totals = _scrape_each_agency(req, storage, manager, "ebc_urls.yaml")
    except Exception as e:
        logger.error(f"EBC scraping failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if storage is not None:
            storage.close()

    return _scrape_response(req, totals, "EBC scraping failed: {}")


# =============================================================================
# Estado por agência (painel "Ingestão" do streamlit-infra-health)
# =============================================================================


@app.get("/agencies/state")
def agencies_state():
    """Estado de cada agência em scrape_agency_state, com atraso, SLO e o resumo.

    503 se a tabela não existe (migration 038 não aplicada).
    """
    from govbr_scraper.scrapers.yaml_config import active_agency_keys
    from govbr_scraper.state.report import build_state_report
    from govbr_scraper.state.repository import StateUnavailable
    from govbr_scraper.storage.postgres_manager import PostgresManager

    pg = None
    try:
        pg = PostgresManager(min_connections=1, max_connections=1)
        states = pg.list_agency_states()
    except StateUnavailable as e:
        logger.warning(f"GET /agencies/state: {e}")
        return JSONResponse(status_code=503, content={"detail": "scrape_agency_state indisponível"})
    except Exception as e:
        logger.error(f"GET /agencies/state falhou: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if pg is not None:
            pg.close_all()

    active = active_agency_keys("site_urls.yaml") + active_agency_keys("ebc_urls.yaml")
    dispatch_mode = dispatch_mode_from_env().value
    return build_state_report(states, active, datetime.now(UTC), dispatch_mode)


# =============================================================================
# Despacho por fila (Passos 3 e 4): /scrape/agency (Cloud Tasks) e /dispatch
# =============================================================================


class ScrapeAgencyRequest(BaseModel):
    """Corpo de /scrape/agency, a tarefa do Cloud Tasks (validado à mão: nunca dá 422).

    Sem datas, a janela vem da marca d'água e a tentativa atualiza o estado da agência; com
    ``start_date`` (``end_date`` padrão = ``start_date``), a janela é explícita (backfill) e
    não mexe no estado. ``trigger`` ausente vale ``backfill`` com janela explícita e
    ``task`` sem.
    """

    agency: str
    start_date: date | None = None
    end_date: date | None = None
    force: bool = False
    allow_update: bool = False
    trigger: Literal["task", "backfill"] | None = None

    @field_validator("agency")
    @classmethod
    def _agency_not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("agency vazia")
        return value

    @model_validator(mode="after")
    def _explicit_window(self):
        if self.end_date is not None and self.start_date is None:
            raise ValueError("end_date exige start_date")
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date é posterior a end_date")
        return self


class ScrapeAgencyResponse(ScrapeResponse):
    # status: completed | failed | skipped | unknown_agency | invalid_payload
    agency: str | None = None
    trigger: str | None = None
    # False em erro permanente (200: o Cloud Tasks não repete); True em passageiro (503).
    retryable: bool | None = None


# Cabeçalhos do Cloud Tasks → campos do scrape_attempt.
_TASK_NAME_HEADER = "X-CloudTasks-TaskName"
_TASK_RETRY_HEADER = "X-CloudTasks-TaskRetryCount"


def _task_log_fields(request: Request) -> dict[str, Any]:
    """``task_name`` e ``task_retry`` dos cabeçalhos do Cloud Tasks (ausentes: omitidos)."""
    fields: dict[str, Any] = {}
    name = request.headers.get(_TASK_NAME_HEADER)
    if name is not None:
        fields["task_name"] = name
    retry = request.headers.get(_TASK_RETRY_HEADER)
    if retry is not None:
        try:
            fields["task_retry"] = int(retry)
        except ValueError:
            fields["task_retry"] = retry
    return fields


def _validation_message(error: Exception) -> str:
    if isinstance(error, ValidationError):
        parts = [
            f"{'.'.join(str(p) for p in e['loc']) or 'corpo'}: {e['msg']}"
            for e in error.errors()[:5]
        ]
        return "; ".join(parts)
    return str(error)[:300]


def _agency_source(agency: str) -> str | None:
    """YAML da agência ativa: ``site_urls.yaml`` (gov.br), ``ebc_urls.yaml`` (EBC) ou None."""
    from govbr_scraper.scrapers.yaml_config import active_agency_keys

    for yaml_file in ("site_urls.yaml", "ebc_urls.yaml"):
        if agency in active_agency_keys(yaml_file):
            return yaml_file
    return None


def _agency_json(status_code: int, response: ScrapeAgencyResponse) -> JSONResponse:
    return JSONResponse(content=response.model_dump(), status_code=status_code)


@app.post("/scrape/agency", response_model=ScrapeAgencyResponse)
async def scrape_agency(request: Request):
    """Raspa uma agência: o alvo das tarefas da fila scrape-govbr.

    200 para sucesso, erro permanente, ``skipped`` (circuito aberto), ``unknown_agency`` e
    payload inválido; 503 só para erro passageiro (o Cloud Tasks repete qualquer não-2xx).
    """
    task = _task_log_fields(request)
    raw = await request.body()
    try:
        req = ScrapeAgencyRequest.model_validate(json.loads(raw or b"null"))
    except ValueError as e:  # JSON inválido, UnicodeDecodeError e ValidationError
        message = _validation_message(e)
        logger.warning(f"/scrape/agency: payload inválido {task or ''}: {message}")
        return _agency_json(
            200,
            ScrapeAgencyResponse(
                status="invalid_payload",
                message=f"Payload inválido (não repetir): {message}",
                retryable=False,
            ),
        )
    return await run_in_threadpool(_scrape_single_agency, req, task)


def _scrape_single_agency(req: ScrapeAgencyRequest, task: dict[str, Any]) -> JSONResponse:
    from govbr_scraper.scrapers.ebc_scrape_manager import EBCScrapeManager
    from govbr_scraper.scrapers.scrape_manager import ScrapeManager
    from govbr_scraper.storage import StorageAdapter

    explicit = req.start_date is not None
    trigger = req.trigger or (_TRIGGER_BACKFILL if explicit else _TRIGGER_TASK)
    yaml_file = _agency_source(req.agency)
    if yaml_file is None:
        message = f"Agência '{req.agency}' fora do site_urls.yaml/ebc_urls.yaml ou inativa"
        logger.warning(f"/scrape/agency: {message} {task or ''}")
        log_scrape_attempt(
            agency=req.agency,
            outcome=OUTCOME_FAILED,
            category=UNKNOWN_AGENCY,
            retryable=False,
            explicit_window=explicit,
            trigger=trigger,
            **task,
        )
        error = AgencyError(
            agency=req.agency, error=message, category=UNKNOWN_AGENCY, retryable=False
        )
        return _agency_json(
            200,
            ScrapeAgencyResponse(
                status="unknown_agency",
                agency=req.agency,
                trigger=trigger,
                retryable=False,
                errors=[error],
                message=message,
            ),
        )

    scrape_req = _ScrapeRequest(
        agencies=[req.agency],
        start_date=req.start_date,
        end_date=req.end_date,
        allow_update=req.allow_update,
        sequential=True,
        force=req.force,
    )
    logger.info(f"Scraping agency ({trigger}): {_describe_request(scrape_req)} {task or ''}")
    storage = None
    try:
        storage = StorageAdapter(
            attempt_context=AttemptContext(explicit_window=explicit, forced=req.force)
        )
        manager_cls = ScrapeManager if yaml_file == "site_urls.yaml" else EBCScrapeManager
        totals = _scrape_each_agency(
            scrape_req, storage, manager_cls(storage), yaml_file, trigger=trigger, log_extra=task
        )
    except Exception as e:
        # Banco fora, bug: passageiro. O 503 faz o Cloud Tasks repetir (até 3 tentativas).
        logger.error(f"/scrape/agency {req.agency} falhou: {e}")
        error = AgencyError(agency=req.agency, error=str(e), category=None, retryable=True)
        return _agency_json(
            503,
            ScrapeAgencyResponse(
                status="failed",
                agency=req.agency,
                trigger=trigger,
                retryable=True,
                errors=[error],
                message=f"Erro passageiro: {e}",
            ),
        )
    finally:
        if storage is not None:
            storage.close()
    return _scrape_agency_response(req, trigger, totals)


def _scrape_agency_response(
    req: ScrapeAgencyRequest, trigger: str, totals: dict[str, Any]
) -> JSONResponse:
    """Status HTTP de /scrape/agency: 503 só para erro passageiro."""
    errors = [AgencyError(**e) for e in totals["errors"]]
    processed = totals["agencies_processed"]
    skipped = totals["skipped"]
    retryable: bool | None = None
    if skipped:
        status, http_status = "skipped", 200
        message = f"{req.agency} pulada: circuito aberto (sonda em {skipped[0].next_probe_at})"
    elif processed:
        status, http_status, message = "completed", 200, "Scraping completed"
    elif errors:
        retryable = any(e.retryable for e in errors)
        status, http_status = "failed", 503 if retryable else 200
        kind = "Erro passageiro" if retryable else "Erro permanente"
        message = f"{kind} ({errors[0].category or 'sem categoria'}): {errors[0].error}"
    else:  # não acontece: o manager devolve a agência processada ou um erro
        retryable = True
        status, http_status, message = "failed", 503, "A raspagem não devolveu resultado"

    if req.start_date is not None:
        start_date = req.start_date.isoformat()
        end_date = (req.end_date or req.start_date).isoformat()
    elif totals["windows"]:
        start_date, end_date = (d.isoformat() for d in totals["windows"][0])
    else:
        start_date = end_date = None

    response = ScrapeAgencyResponse(
        status=status,
        start_date=start_date,
        end_date=end_date,
        articles_scraped=totals["articles_scraped"],
        articles_saved=totals["articles_saved"],
        articles_new=totals["articles_new"],
        articles_skipped=totals["articles_skipped"],
        agencies_processed=processed,
        errors=errors,
        skipped=skipped,
        message=message,
        agency=req.agency,
        trigger=trigger,
        retryable=retryable,
    )
    return _agency_json(http_status, response)


class DispatchRequest(BaseModel):
    """Corpo de /dispatch: ``{"reason": "tick"}`` (Scheduler) ou o backfill da DAG.

    No backfill, ``start_date`` e ``end_date`` são obrigatórios e ``agencies`` vazio = todas
    as ativas. ``dry_run`` rebaixa o modo live para dry_run nesta chamada.
    """

    reason: Literal["tick", "backfill"] = "tick"
    agencies: list[str] | None = None
    start_date: date | None = None
    end_date: date | None = None
    force: bool = False
    dry_run: bool = False

    @model_validator(mode="after")
    def _backfill_window(self):
        if self.reason == "backfill":
            if self.start_date is None or self.end_date is None:
                raise ValueError("backfill exige start_date e end_date (YYYY-MM-DD)")
            if self.start_date > self.end_date:
                raise ValueError("start_date é posterior a end_date")
        return self


@app.post("/dispatch")
def dispatch(req: DispatchRequest | None = None):
    """Despacha as agências vencidas (tick) ou um backfill para a fila scrape-govbr.

    Modo em ``SCRAPE_DISPATCH_MODE``: ``off`` só loga ``dispatch_run`` e responde;
    ``dry_run`` calcula, loga e recalcula as faixas, sem criar tarefa; ``live`` cria as
    tarefas e roda o reconciliador. 503 sem a tabela (038), 500 em outro erro (sem
    ``dispatch_run``: o alerta de ausência do heartbeat acusa).
    """
    from govbr_scraper.dispatch.config import (
        DispatchConfigError,
        DispatchMode,
        TasksConfig,
        effective_mode,
    )
    from govbr_scraper.dispatch.service import Dispatcher, DispatchOutcome
    from govbr_scraper.dispatch.tasks_client import CloudTasksClient
    from govbr_scraper.scrapers.yaml_config import active_agency_keys
    from govbr_scraper.state.repository import StateUnavailable
    from govbr_scraper.storage.postgres_manager import PostgresManager

    req = req or DispatchRequest()
    now = datetime.now(UTC)
    mode = effective_mode(dispatch_mode_from_env(), req.dry_run)
    if mode == DispatchMode.OFF:
        outcome = DispatchOutcome(mode=mode.value, reason=req.reason)
        if req.reason == "backfill":
            outcome.start_date = req.start_date.isoformat()
            outcome.end_date = req.end_date.isoformat()
        log_dispatch_run(**outcome.log_fields())
        return outcome.response()

    tasks = None
    if mode == DispatchMode.LIVE:
        try:
            tasks = CloudTasksClient.from_config(TasksConfig.from_env())
        except DispatchConfigError as e:
            logger.error(f"/dispatch: {e}")
            return JSONResponse(status_code=500, content={"detail": str(e)})

    active = active_agency_keys("site_urls.yaml") + active_agency_keys("ebc_urls.yaml")
    pg = None
    try:
        pg = PostgresManager(min_connections=1, max_connections=1)
        conn = pg.get_connection()
        try:
            dispatcher = Dispatcher(conn, mode=mode, now=now, active_keys=active, tasks=tasks)
            if req.reason == "backfill":
                outcome = dispatcher.backfill(
                    agencies=req.agencies,
                    start_date=req.start_date,
                    end_date=req.end_date,
                    force=req.force,
                )
            else:
                outcome = dispatcher.tick()
        finally:
            pg.put_connection(conn)
    except StateUnavailable as e:
        logger.error(f"/dispatch ({req.reason}): {e}")
        return JSONResponse(status_code=503, content={"detail": "scrape_agency_state indisponível"})
    except Exception as e:
        logger.error(f"/dispatch ({req.reason}) falhou: {e}")
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        if pg is not None:
            pg.close_all()
        if tasks is not None:
            tasks.close()

    log_dispatch_run(**outcome.log_fields())
    return outcome.response()
