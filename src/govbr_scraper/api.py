"""
Scraper API for DestaquesGovBr.

Lightweight HTTP wrapper around existing CLI scraper commands.
Designed to run on Cloud Run, called by Airflow DAGs.
"""

import logging
import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(
    title="DestaquesGovBr Scraper API",
    description="HTTP wrapper for gov.br and EBC news scrapers",
    version="1.0.0",
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
    elif errors:
        status = "partial"
        message = f"Completed with {len(errors)} error(s)"
    else:
        status = "completed"
        message = "Scraping completed"

    return ScrapeResponse(
        status=status,
        start_date=req.start_date,
        end_date=end,
        articles_scraped=metrics["articles_scraped"],
        articles_saved=metrics["articles_saved"],
        agencies_processed=metrics["agencies_processed"],
        errors=errors,
        message=message,
    )


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
    elif errors:
        status = "partial"
        message = f"Completed with {len(errors)} error(s)"
    else:
        status = "completed"
        message = "EBC scraping completed"

    return ScrapeResponse(
        status=status,
        start_date=req.start_date,
        end_date=end,
        articles_scraped=metrics["articles_scraped"],
        articles_saved=metrics["articles_saved"],
        agencies_processed=metrics["agencies_processed"],
        errors=errors,
        message=message,
    )
