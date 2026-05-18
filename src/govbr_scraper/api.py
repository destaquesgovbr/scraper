"""
Scraper API for DestaquesGovBr.

Lightweight HTTP wrapper around existing CLI scraper commands.
Designed to run on Cloud Run, called by Airflow DAGs.
"""

import logging
import os

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator

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


class ScrapeAgenciesRequest(BaseModel):
    start_date: str
    end_date: str | None = None
    agencies: list[str] | None = None
    allow_update: bool = False
    sequential: bool = True


class ScrapeEBCRequest(BaseModel):
    start_date: str
    end_date: str | None = None
    agencies: list[str] | None = None
    allow_update: bool = False
    sequential: bool = True


class AgencyError(BaseModel):
    agency: str
    error: str


class ScrapeResponse(BaseModel):
    status: str
    start_date: str
    end_date: str
    articles_scraped: int = 0
    articles_saved: int = 0
    agencies_processed: list[str] = []
    errors: list[AgencyError] = []
    message: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/scrape/agencies", response_model=ScrapeResponse)
def scrape_agencies(req: ScrapeAgenciesRequest):
    from govbr_scraper.storage import StorageAdapter
    from govbr_scraper.scrapers.scrape_manager import ScrapeManager

    end = req.end_date or req.start_date
    logger.info(f"Scraping agencies: {req.agencies or 'ALL'} from {req.start_date} to {end}")

    try:
        storage = StorageAdapter()
        manager = ScrapeManager(storage)
        metrics = manager.run_scraper(
            agencies=req.agencies,
            min_date=req.start_date,
            max_date=end,
            sequential=req.sequential,
            allow_update=req.allow_update,
        )
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    errors = [AgencyError(**e) for e in metrics.get("errors", [])]
    if errors and not metrics["agencies_processed"]:
        status = "failed"
        message = "All agencies failed"
        http_status = 500
    elif errors:
        status = "partial"
        message = f"Completed with {len(errors)} error(s)"
        http_status = 207  # Multi-Status
    else:
        status = "completed"
        message = "Scraping completed"
        http_status = 200

    response = ScrapeResponse(
        status=status,
        start_date=req.start_date,
        end_date=end,
        articles_scraped=metrics["articles_scraped"],
        articles_saved=metrics["articles_saved"],
        agencies_processed=metrics["agencies_processed"],
        errors=errors,
        message=message,
    )
    return JSONResponse(content=response.model_dump(), status_code=http_status)


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
    from govbr_scraper.storage import StorageAdapter
    from govbr_scraper.scrapers.ebc_scrape_manager import EBCScrapeManager

    end = req.end_date or req.start_date
    logger.info(f"Scraping EBC agencies: {req.agencies or 'ALL'} from {req.start_date} to {end}")

    try:
        storage = StorageAdapter()
        manager = EBCScrapeManager(storage)
        metrics = manager.run_scraper(
            min_date=req.start_date,
            max_date=end,
            sequential=req.sequential,
            allow_update=req.allow_update,
            agencies=req.agencies,
        )
    except Exception as e:
        logger.error(f"EBC scraping failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    errors = [AgencyError(**e) for e in metrics.get("errors", [])]
    if errors and not metrics["agencies_processed"]:
        status = "failed"
        message = f"EBC scraping failed: {errors[0].error}"
        http_status = 500
    elif errors:
        status = "partial"
        message = f"Completed with {len(errors)} error(s)"
        http_status = 207  # Multi-Status
    else:
        status = "completed"
        message = "EBC scraping completed"
        http_status = 200

    response = ScrapeResponse(
        status=status,
        start_date=req.start_date,
        end_date=end,
        articles_scraped=metrics["articles_scraped"],
        articles_saved=metrics["articles_saved"],
        agencies_processed=metrics["agencies_processed"],
        errors=errors,
        message=message,
    )
    return JSONResponse(content=response.model_dump(), status_code=http_status)
