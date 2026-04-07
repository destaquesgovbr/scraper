#!/usr/bin/env python3
"""
Validacao local completa da implementacao de monitoramento do scraper.

Requisitos:
- PostgreSQL rodando em localhost:5433 com dados mestres populados
- Tabela scrape_runs criada (scripts/create_scrape_runs.sql)

Uso:
  DATABASE_URL="postgresql://destaquesgovbr_dev:dev_password@localhost:5433/destaquesgovbr_dev" \
    poetry run python scripts/validate_monitoring.py
"""

import os
import sys
from datetime import datetime, timezone, timedelta

import psycopg2
from psycopg2.extras import RealDictCursor

# Adicionar src/ ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

PASS = "PASS"
FAIL = "FAIL"
results = []


def check(name, condition, detail=""):
    status = PASS if condition else FAIL
    results.append((name, status))
    marker = "+" if condition else "X"
    print(f"  [{marker}] {name}" + (f" -- {detail}" if detail else ""))
    return condition


def get_conn():
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("ERRO: DATABASE_URL nao definido.")
        sys.exit(1)
    return psycopg2.connect(url)


# ===========================================================================
# 1. Error Classification
# ===========================================================================
print("\n=== 1. Error Classification ===")

from govbr_scraper.models.monitoring import ErrorCategory, classify_error

check("classify timeout -> NETWORK_ERROR",
      classify_error("Connection timed out") == ErrorCategory.NETWORK_ERROR)

check("classify anti-bot -> ANTI_BOT",
      classify_error("Anti-bot protection detected for mec") == ErrorCategory.ANTI_BOT)

check("classify 403 -> URL_BROKEN",
      classify_error("Forbidden", http_status=403) == ErrorCategory.URL_BROKEN)

check("classify 404 -> URL_BROKEN",
      classify_error("Not Found", http_status=404) == ErrorCategory.URL_BROKEN)

check("classify no articles + bytes -> HTML_CHANGED",
      classify_error("No articles found on first page of mec but response was 15432 bytes") == ErrorCategory.HTML_CHANGED)

check("classify no news -> EMPTY_CONTENT",
      classify_error("No news found for mec.") == ErrorCategory.EMPTY_CONTENT)

check("classify unknown -> UNKNOWN",
      classify_error("Something weird happened") == ErrorCategory.UNKNOWN)

check("classify case insensitive",
      classify_error("ANTI-BOT PROTECTION DETECTED") == ErrorCategory.ANTI_BOT)


# ===========================================================================
# 2. Structured Logging
# ===========================================================================
print("\n=== 2. Structured Logging ===")

from govbr_scraper.monitoring.structured_log import log_scrape_result
from govbr_scraper.models.monitoring import ScrapeRunResult

result = log_scrape_result(
    agency_key="test_agency",
    status="success",
    articles_scraped=5,
    articles_saved=3,
    execution_time_seconds=1.5,
)
check("log_scrape_result returns ScrapeRunResult",
      isinstance(result, ScrapeRunResult))
check("result has correct fields",
      result.agency_key == "test_agency" and result.articles_scraped == 5)
check("result has scraped_at timestamp",
      result.scraped_at is not None)

result_err = log_scrape_result(
    agency_key="test_agency",
    status="error",
    error_category=ErrorCategory.NETWORK_ERROR,
    error_message="Connection timed out",
)
check("error result has category",
      result_err.error_category == ErrorCategory.NETWORK_ERROR)


# ===========================================================================
# 3. Database Persistence (record_scrape_run + get_recent_runs)
# ===========================================================================
print("\n=== 3. Database Persistence ===")

conn = get_conn()

# Limpar dados de teste anteriores
with conn.cursor() as cur:
    cur.execute("DELETE FROM scrape_runs WHERE agency_key LIKE 'test_%%'")
conn.commit()

# Usar PostgresManager diretamente
from govbr_scraper.storage.postgres_manager import PostgresManager

pg = PostgresManager(os.environ["DATABASE_URL"])

now = datetime.now(timezone.utc)

# Inserir runs de teste
test_runs = [
    ScrapeRunResult(agency_key="test_ok", status="success", articles_scraped=10, articles_saved=8,
                    execution_time_seconds=2.5, scraped_at=now - timedelta(minutes=10)),
    ScrapeRunResult(agency_key="test_ok", status="success", articles_scraped=5, articles_saved=5,
                    execution_time_seconds=1.2, scraped_at=now - timedelta(minutes=20)),
    ScrapeRunResult(agency_key="test_fail", status="error", error_category=ErrorCategory.ANTI_BOT,
                    error_message="Anti-bot detected", execution_time_seconds=0.5,
                    scraped_at=now - timedelta(minutes=5)),
    ScrapeRunResult(agency_key="test_fail", status="error", error_category=ErrorCategory.ANTI_BOT,
                    error_message="Anti-bot detected", execution_time_seconds=0.3,
                    scraped_at=now - timedelta(minutes=15)),
    ScrapeRunResult(agency_key="test_fail", status="error", error_category=ErrorCategory.NETWORK_ERROR,
                    error_message="Connection timed out", execution_time_seconds=10.0,
                    scraped_at=now - timedelta(minutes=25)),
    ScrapeRunResult(agency_key="test_stale", status="success", articles_scraped=5, articles_saved=5,
                    execution_time_seconds=3.0, scraped_at=now - timedelta(hours=48)),
]

for run in test_runs:
    pg.record_scrape_run(run)

check("record_scrape_run inserts without error", True)

# Verificar via query direta
with conn.cursor(cursor_factory=RealDictCursor) as cur:
    cur.execute("SELECT COUNT(*) as cnt FROM scrape_runs WHERE agency_key LIKE 'test_%%'")
    cnt = cur.fetchone()["cnt"]
check(f"6 test records inserted in scrape_runs", cnt == 6, f"got {cnt}")

# Testar get_recent_runs
recent = pg.get_recent_runs("test_ok", limit=5)
check("get_recent_runs returns list", isinstance(recent, list))
check("get_recent_runs returns 2 for test_ok", len(recent) == 2, f"got {len(recent)}")
check("get_recent_runs ordered DESC",
      recent[0]["scraped_at"] > recent[1]["scraped_at"] if len(recent) == 2 else False)

recent_fail = pg.get_recent_runs("test_fail", limit=5)
check("get_recent_runs returns 3 for test_fail", len(recent_fail) == 3, f"got {len(recent_fail)}")
check("error_category is preserved",
      recent_fail[0]["error_category"] == "anti_bot" if recent_fail else False)


# ===========================================================================
# 4. Health Check Functions
# ===========================================================================
print("\n=== 4. Health Check Functions ===")

from govbr_scraper.monitoring.health_checks import (
    find_consecutive_failures,
    find_stale_agencies,
    compute_coverage_report,
)

# test_fail tem 3 falhas consecutivas
failures = find_consecutive_failures(conn, threshold=3)
test_fail_found = any(r["agency_key"] == "test_fail" for r in failures)
check("find_consecutive_failures detects test_fail (3 errors)",
      test_fail_found, f"found: {[r['agency_key'] for r in failures]}")

# test_ok NAO deve aparecer (so tem sucesso)
test_ok_in_failures = any(r["agency_key"] == "test_ok" for r in failures)
check("find_consecutive_failures does NOT detect test_ok",
      not test_ok_in_failures)

# test_stale tem ultimo sucesso ha 48h
stale = find_stale_agencies(conn, stale_hours=24)
test_stale_found = any(r["agency_key"] == "test_stale" for r in stale)
check("find_stale_agencies detects test_stale (48h old)",
      test_stale_found, f"found: {[r['agency_key'] for r in stale]}")

# test_ok NAO deve aparecer (sucesso recente)
test_ok_in_stale = any(r["agency_key"] == "test_ok" for r in stale)
check("find_stale_agencies does NOT detect test_ok",
      not test_ok_in_stale)

# Coverage report
coverage = compute_coverage_report(conn, hours=24)
check("compute_coverage_report returns dict", isinstance(coverage, dict))
check("coverage has required keys",
      all(k in coverage for k in ["total_active", "agencies_scraped", "total_articles"]))
check(f"coverage total_active > 0", (coverage.get("total_active") or 0) > 0,
      f"got {coverage}")


# ===========================================================================
# 5. Notification (log-only mode)
# ===========================================================================
print("\n=== 5. Notification ===")

from notify import send_telegram_alert, send_alert

# Sem token -> deve retornar False via log-only
sent = send_alert("Test alert message", telegram_token=None, telegram_chat_id=None)
check("send_alert without telegram returns False (log-only)", sent is False)

# send_telegram_alert com token falso -> deve retornar False sem explodir
sent_tg = send_telegram_alert("FAKE_TOKEN", "-100", "test")
check("send_telegram_alert with bad token returns False", sent_tg is False)


# ===========================================================================
# 6. Verificacao de dados reais do scrape anterior
# ===========================================================================
print("\n=== 6. Dados Reais do Scrape ===")

with conn.cursor(cursor_factory=RealDictCursor) as cur:
    # Verificar que secom e mec foram gravados com sucesso
    cur.execute("""
        SELECT agency_key, status, articles_scraped, articles_saved,
               round(execution_time_seconds::numeric, 2) as exec_time
        FROM scrape_runs
        WHERE agency_key IN ('secom', 'mec')
        ORDER BY agency_key, scraped_at DESC
    """)
    real_runs = cur.fetchall()

check("Real scrape runs exist for secom/mec", len(real_runs) >= 2,
      f"found {len(real_runs)} runs")

for run in real_runs:
    check(f"  {run['agency_key']}: status={run['status']}, scraped={run['articles_scraped']}, "
          f"saved={run['articles_saved']}, time={run['exec_time']}s",
          run["status"] == "success")


# ===========================================================================
# Cleanup
# ===========================================================================
print("\n=== Cleanup ===")

with conn.cursor() as cur:
    cur.execute("DELETE FROM scrape_runs WHERE agency_key LIKE 'test_%%'")
conn.commit()
check("Test data cleaned up", True)

conn.close()
pg.close_all()


# ===========================================================================
# Resumo
# ===========================================================================
print("\n" + "=" * 60)
passed = sum(1 for _, s in results if s == PASS)
failed = sum(1 for _, s in results if s == FAIL)
total = len(results)
print(f"RESULTADO: {passed}/{total} passed, {failed} failed")

if failed > 0:
    print("\nFalhas:")
    for name, status in results:
        if status == FAIL:
            print(f"  [X] {name}")
    sys.exit(1)
else:
    print("\nTodos os testes passaram!")
    sys.exit(0)
