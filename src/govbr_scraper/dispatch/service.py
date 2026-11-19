"""Despachante da ingestão (``POST /dispatch``): o tick de 15 em 15 min e o backfill manual.

Tick (Cloud Scheduler, ``{"reason": "tick"}``):

1. ``ensure_rows`` das chaves ativas (gov.br e EBC);
2. recálculo diário das faixas (``dry_run`` e ``live``), no primeiro tick depois das 03:00
   BRT, reservado em ``scrape_dispatch_meta['tiers_recomputed_on']``;
3. seleção (``select_due``: modo noturno, vencidas, em voo, disjuntor, teto de 200);
4. criação das tarefas no Cloud Tasks (só ``live``) e ``mark_dispatched`` das criadas e
   duplicadas. O corpo leva ``dispatched_at`` (o ``now`` do tick): o ``/scrape/agency``
   ignora a tarefa repetida de uma agência já tentada depois do despacho;
5. frescor por faixa (evento ``freshness``, só nos ticks diurnos);
6. reconciliador (só ``live``, 1× por dia a partir das 03:00, reservado em
   ``scrape_dispatch_meta['reconciled_on']``): republica em ``dgb.news.scraped`` até 20
   notícias sem tema.

Um tick ``live`` em que o Cloud Tasks recusou tarefas e nenhuma foi criada (nem duplicada)
termina com ``DispatchFailed``: a API responde 500 sem o ``dispatch_run``, e o alerta de
ausência do heartbeat acusa (permissão revogada, fila apagada ou desabilitada).

Backfill (DAG ``scrape_backfill``, ``{"reason": "backfill", ...}``): uma tarefa por agência
com a janela explícita, sem olhar o vencimento; o disjuntor vale, a não ser com ``force``.

O modo ``off`` não chega aqui: a API só loga ``dispatch_run`` e responde.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time
from typing import Any

from loguru import logger

from govbr_scraper.dispatch import queries
from govbr_scraper.dispatch.config import DispatchMode
from govbr_scraper.dispatch.freshness import compute_freshness
from govbr_scraper.dispatch.selection import is_night, select_due
from govbr_scraper.dispatch.tasks_client import (
    CREATED,
    DUPLICATE,
    FAILED,
    CreateResult,
    backfill_task_id,
    tick_task_id,
)
from govbr_scraper.dispatch.tiers import ewma_rate, recompute_tier
from govbr_scraper.monitoring.structured_log import log_freshness
from govbr_scraper.state import repository
from govbr_scraper.state.model import AgencyState
from govbr_scraper.state.policy import BRT
from govbr_scraper.state.report import iso_utc, slo_interval_floor
from govbr_scraper.state.repository import StateUnavailable
from govbr_scraper.state.transitions import circuit_gate

REASON_TICK = "tick"
REASON_BACKFILL = "backfill"
TRIGGER_TASK = "task"
TRIGGER_BACKFILL = "backfill"

# Chaves de scrape_dispatch_meta (o valor é {"date": "<dia BRT>", "claimed_at": ...}).
TIERS_RECOMPUTED_KEY = "tiers_recomputed_on"
RECONCILED_KEY = "reconciled_on"

# O recálculo e o reconciliador rodam no primeiro tick do dia BRT a partir das 03:00.
DAILY_JOBS_AFTER = time(3, 0)

# Depois de tantas falhas seguidas no Cloud Tasks (permissão, fila inexistente, API fora), as
# tarefas restantes do tick nem são tentadas: contam como falha, e o tick termina dentro do
# attempt_deadline de 300 s do Scheduler (cada chamada tem timeout de 10 s).
MAX_CONSECUTIVE_CREATE_FAILURES = 5

PUBSUB_TOPIC_NEWS_SCRAPED_ENV = "PUBSUB_TOPIC_NEWS_SCRAPED"

RESULT_DRY_RUN = "dry_run"


@dataclass
class DispatchOutcome:
    """Resultado de uma chamada do ``/dispatch``: o corpo da resposta e o ``dispatch_run``."""

    mode: str
    reason: str
    night_skip: bool = False
    due: int = 0
    dispatched: int = 0
    duplicates: int = 0
    failed: int = 0
    skipped_inflight: int = 0
    overflow: int = 0
    tiers_recomputed: bool = False
    tiers_changed: int = 0
    reconciled: int = 0
    slo_ok_ratio: float | None = None
    agencies: list[dict[str, Any]] = field(default_factory=list)
    # Só no backfill.
    start_date: str | None = None
    end_date: str | None = None
    skipped_circuit: list[str] = field(default_factory=list)
    unknown_agencies: list[str] = field(default_factory=list)

    def response(self) -> dict[str, Any]:
        body: dict[str, Any] = {
            "mode": self.mode,
            "reason": self.reason,
            "night_skip": self.night_skip,
            "due": self.due,
            "dispatched": self.dispatched,
            "duplicates": self.duplicates,
            "failed": self.failed,
            "skipped_inflight": self.skipped_inflight,
            "overflow": self.overflow,
            "tiers_recomputed": self.tiers_recomputed,
            "tiers_changed": self.tiers_changed,
            "reconciled": self.reconciled,
            "slo_ok_ratio": self.slo_ok_ratio,
            "agencies": self.agencies,
        }
        if self.reason == REASON_BACKFILL:
            body.update(
                start_date=self.start_date,
                end_date=self.end_date,
                skipped_circuit=self.skipped_circuit,
                unknown_agencies=self.unknown_agencies,
            )
        return body

    def log_fields(self) -> dict[str, Any]:
        """Campos do evento ``dispatch_run`` (os do contrato e os extras)."""
        fields = {
            "mode": self.mode,
            "reason": self.reason,
            "night_skip": self.night_skip,
            "due": self.due,
            "dispatched": self.dispatched,
            "duplicates": self.duplicates,
            "skipped_inflight": self.skipped_inflight,
            "tiers_recomputed": self.tiers_recomputed,
            "reconciled": self.reconciled,
            "failed": self.failed,
            "overflow": self.overflow,
            "tiers_changed": self.tiers_changed,
            "agencies": [a["agency"] for a in self.agencies],
        }
        if self.reason == REASON_BACKFILL:
            fields.update(
                start_date=self.start_date,
                end_date=self.end_date,
                skipped_circuit=self.skipped_circuit,
                unknown_agencies=self.unknown_agencies,
            )
        return fields


class DispatchFailed(RuntimeError):
    """Tick ``live`` em que o Cloud Tasks recusou tarefas e nenhuma foi criada nem duplicada.

    Não é um tick normal: sem tarefa nenhuma agência é raspada, e o heartbeat
    (``dispatch_run``) esconderia isso dos alertas. A API responde 500 e loga
    ``dispatch_failed`` no lugar. ``outcome`` traz as contagens e o resultado por agência.
    """

    def __init__(self, outcome: DispatchOutcome) -> None:
        self.outcome = outcome
        errors = sorted({a["error"] for a in outcome.agencies if a.get("error")})
        detail = f": {errors[0]}" if errors else ""
        super().__init__(
            f"o Cloud Tasks recusou {outcome.failed} de {outcome.due} tarefa(s) e nenhuma foi "
            f"criada{detail}"
        )


def _create_error(result: CreateResult) -> str | None:
    """Erro de uma criação que falhou, para a resposta (``HTTP 403: ...``)."""
    if result.http_status is not None:
        return f"HTTP {result.http_status}: {result.error}"
    return result.error


def _default_publisher():
    from govbr_scraper.storage.event_publisher import EventPublisher

    return EventPublisher()


class Dispatcher:
    """Executa um tick ou um backfill numa conexão psycopg2 (commit por etapa).

    ``tasks`` é o ``CloudTasksClient`` (obrigatório em ``live``). ``publisher_factory`` cria o
    ``EventPublisher`` do reconciliador.
    """

    def __init__(
        self,
        conn,
        *,
        mode: DispatchMode,
        now: datetime,
        active_keys: Iterable[str],
        tasks=None,
        publisher_factory: Callable[[], Any] = _default_publisher,
    ) -> None:
        if mode == DispatchMode.OFF:
            raise ValueError("o modo off não usa o Dispatcher")
        if mode == DispatchMode.LIVE and tasks is None:
            raise ValueError("o modo live exige o cliente do Cloud Tasks")
        self.conn = conn
        self.mode = mode
        self.now = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
        self.active_keys = sorted(set(active_keys))
        self.tasks = tasks
        self.publisher_factory = publisher_factory

    @property
    def live(self) -> bool:
        return self.mode == DispatchMode.LIVE

    @property
    def _local(self) -> datetime:
        return self.now.astimezone(BRT)

    @property
    def _daily_jobs_due(self) -> bool:
        return self._local.time() >= DAILY_JOBS_AFTER

    @contextmanager
    def _transaction(self):
        try:
            yield
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    # ------------------------------------------------------------------ tick

    def tick(self) -> DispatchOutcome:
        """Um tick do Scheduler. Erro ao ler ou criar o estado (inclusive sem a 038) sobe.

        Em ``live``, se o Cloud Tasks recusou tarefas e nenhuma foi criada nem duplicada,
        levanta ``DispatchFailed`` no fim (depois do frescor e do reconciliador, que não
        dependem da fila).
        """
        out = DispatchOutcome(mode=self.mode.value, reason=REASON_TICK)
        with self._transaction():
            repository.ensure_rows(self.conn, self.active_keys, self.now)
        out.tiers_recomputed, out.tiers_changed = self._recompute_tiers()
        with self._transaction():
            states = repository.list_states(self.conn)

        selection = select_due(states, self.active_keys, self.now)
        out.night_skip = selection.night_skip
        out.due = len(selection.due)
        out.skipped_inflight = len(selection.skipped_inflight)
        out.overflow = len(selection.overflow)

        dispatched_at = iso_utc(self.now)
        items = [
            (
                d.agency_key,
                tick_task_id(d.agency_key, self.now),
                {"agency": d.agency_key, "trigger": TRIGGER_TASK, "dispatched_at": dispatched_at},
            )
            for d in selection.due
        ]
        results = self._create_tasks(items)
        for due, result in zip(selection.due, results, strict=True):
            entry = {
                "agency": due.agency_key,
                "tier": due.state.tier.value,
                "next_due_at": iso_utc(due.state.next_due_at),
                "probe": due.probe,
                "task": result.task_id,
                "result": result.status,
            }
            if result.status == FAILED:
                entry["error"] = _create_error(result)
            out.agencies.append(entry)
        self._count(out, results)
        if self.live:
            self._mark_dispatched(
                [a["agency"] for a in out.agencies if a["result"] in (CREATED, DUPLICATE)]
            )

        if not is_night(self.now):
            out.slo_ok_ratio = self._log_freshness(states)
        if self.live:
            out.reconciled = self._reconcile()
        if self.live and out.failed and not (out.dispatched + out.duplicates):
            raise DispatchFailed(out)
        return out

    def _recompute_tiers(self) -> tuple[bool, int]:
        """Recalcula as faixas de todas as linhas, 1× por dia BRT a partir das 03:00.

        Devolve (recalculou, quantas mudaram de faixa). Erro só vira warning: o tick segue
        com as faixas de ontem, e o próximo tick tenta de novo (a reserva foi desfeita).
        """
        if not self._daily_jobs_due:
            return False, 0
        today = self._local.date()
        updates: list = []
        try:
            with self._transaction():
                if not repository.claim_daily(self.conn, TIERS_RECOMPUTED_KEY, today, self.now):
                    return False, 0
                counts = queries.daily_news_counts(self.conn, today)
                states = repository.list_states(self.conn)
                updates = [
                    recompute_tier(s, ewma_rate(counts.get(s.agency_key, {}), today), self.now)
                    for s in states
                ]
                repository.update_tiers(self.conn, updates, self.now)
        except Exception as e:
            logger.warning(f"Recálculo das faixas falhou (fica para o próximo tick): {e}")
            return False, 0

        changed = [u for u in updates if u.changed]
        by_tier: dict[str, int] = {}
        for u in updates:
            by_tier[u.tier.value] = by_tier.get(u.tier.value, 0) + 1
        logger.bind(
            event="tiers_recomputed",
            day=today.isoformat(),
            agencies=len(updates),
            changed=len(changed),
            by_tier=by_tier,
            changes=[f"{u.agency_key}:{u.previous_tier.value}->{u.tier.value}" for u in changed],
        ).info(
            f"Faixas recalculadas ({today.isoformat()}): {len(updates)} agências, "
            f"{len(changed)} mudaram de faixa; {by_tier}"
        )
        return True, len(changed)

    def _create_tasks(self, items: list[tuple[str, str, dict]]) -> list[CreateResult]:
        """Cria as tarefas (``live``) ou só as nomeia (``dry_run``), na ordem de ``items``.

        Uma falha não derruba o tick: é logada e conta em ``failed``. Depois de
        ``MAX_CONSECUTIVE_CREATE_FAILURES`` seguidas, as restantes nem são tentadas.
        """
        if not self.live:
            return [CreateResult(RESULT_DRY_RUN, task_id) for _, task_id, _ in items]
        results: list[CreateResult] = []
        consecutive = 0
        not_tried = 0
        for agency, task_id, body in items:
            if consecutive >= MAX_CONSECUTIVE_CREATE_FAILURES:
                not_tried += 1
                results.append(
                    CreateResult(
                        FAILED,
                        task_id,
                        error=f"não tentada: {consecutive} falhas seguidas no Cloud Tasks",
                    )
                )
                continue
            result = self.tasks.create_task(task_id, body)
            if result.status == FAILED:
                consecutive += 1
                logger.warning(
                    f"Falha ao criar a tarefa {task_id} ({agency}): "
                    f"HTTP {result.http_status} {result.error}"
                )
            else:
                consecutive = 0
            results.append(result)
        if not_tried:
            logger.error(
                f"Cloud Tasks: {MAX_CONSECUTIVE_CREATE_FAILURES} falhas seguidas; "
                f"{not_tried} tarefa(s) não tentada(s) neste tick"
            )
        return results

    @staticmethod
    def _count(out: DispatchOutcome, results: list[CreateResult]) -> None:
        out.dispatched = sum(1 for r in results if r.status == CREATED)
        out.duplicates = sum(1 for r in results if r.status == DUPLICATE)
        out.failed = sum(1 for r in results if r.status == FAILED)

    def _mark_dispatched(self, keys: list[str]) -> None:
        """``last_dispatched_at = now`` das criadas e duplicadas. Erro só vira warning."""
        if not keys:
            return
        try:
            with self._transaction():
                repository.mark_dispatched(self.conn, keys, self.now)
        except Exception as e:
            logger.warning(f"Falha ao registrar o despacho de {len(keys)} agência(s): {e}")

    def _log_freshness(self, states: list[AgencyState]) -> float | None:
        """Loga um evento ``freshness`` por faixa e devolve a fração geral dentro do SLO.

        Em ``dry_run`` o SLO usa o piso da cadência das DAGs (``slo_interval_floor``), como o
        ``GET /agencies/state``: elas ainda são o relógio. Em ``live``, sem piso.
        """
        fresh = compute_freshness(
            states,
            self.active_keys,
            self.now,
            min_interval_minutes=slo_interval_floor(self.mode.value),
        )
        for t in fresh.tiers:
            if t.lag_p90_min is None:  # nenhuma agência da faixa com sucesso ainda
                continue
            log_freshness(
                tier=t.tier,
                lag_p50_min=t.lag_p50_min,
                lag_p90_min=t.lag_p90_min,
                lag_max_min=t.lag_max_min,
                agencies=t.agencies,
                slo_ok_ratio=fresh.slo_ok_ratio,
                tier_slo_ok_ratio=t.tier_slo_ok_ratio,
            )
        return fresh.slo_ok_ratio

    def _reconcile(self) -> int:
        """Republica até 20 notícias sem tema, 1× por dia a partir das 03:00 BRT.

        Sem ``PUBSUB_TOPIC_NEWS_SCRAPED`` não reserva o dia (não haveria como publicar).
        Erro só vira warning. Devolve quantas mensagens foram publicadas.
        """
        if not self._daily_jobs_due:
            return 0
        if not os.getenv(PUBSUB_TOPIC_NEWS_SCRAPED_ENV, "").strip():
            logger.warning(
                f"Reconciliador sem {PUBSUB_TOPIC_NEWS_SCRAPED_ENV}: nada republicado hoje"
            )
            return 0
        today = self._local.date()
        try:
            with self._transaction():
                if not repository.claim_daily(self.conn, RECONCILED_KEY, today, self.now):
                    return 0
                articles = queries.unthemed_news(self.conn, self.now)
        except Exception as e:
            logger.warning(f"Reconciliador falhou (fica para o próximo tick): {e}")
            return 0
        if not articles:
            logger.info("Reconciliador: nenhuma notícia sem tema entre 7 dias e 2 h atrás")
            return 0
        publisher = self.publisher_factory()
        try:
            published = publisher.publish_scraped(articles)
        finally:
            close = getattr(publisher, "close", None)
            if callable(close):
                close()
        logger.bind(
            event="reconciled",
            reconciled=published,
            unique_ids=[a["unique_id"] for a in articles],
        ).info(f"Reconciliador: {published}/{len(articles)} notícia(s) sem tema republicada(s)")
        return published

    # -------------------------------------------------------------- backfill

    def backfill(
        self,
        *,
        agencies: list[str] | None,
        start_date: date,
        end_date: date,
        force: bool = False,
        dedup_key: str | None = None,
    ) -> DispatchOutcome:
        """Uma tarefa por agência com a janela explícita (não mexe no estado da agência).

        ``agencies`` vazio = todas as ativas; chave fora das ativas vai para
        ``unknown_agencies``. O vencimento não importa; circuito aberto ainda não vencido
        vai para ``skipped_circuit``, a não ser com ``force``. Não marca ``last_dispatched_at``:
        a janela explícita não move ``last_attempt_at``, e a agência pareceria em voo por
        30 min, fora do tick regular.

        ``dedup_key`` (da DAG, estável por execução) entra no nome no lugar do ``HHmm``: a
        repetição da task depois de um timeout ou 5xx recebe 409 nas tarefas já criadas.
        """
        out = DispatchOutcome(
            mode=self.mode.value,
            reason=REASON_BACKFILL,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )
        active = set(self.active_keys)
        requested = list(dict.fromkeys(agencies)) if agencies else list(self.active_keys)
        out.unknown_agencies = [k for k in requested if k not in active]
        states = self._states_by_key()
        selected: list[str] = []
        for key in requested:
            if key not in active:
                continue
            if circuit_gate(states.get(key), self.now, force=force).skip:
                out.skipped_circuit.append(key)
                continue
            selected.append(key)
        out.due = len(selected)

        items = []
        for key in selected:
            body: dict[str, Any] = {
                "agency": key,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "trigger": TRIGGER_BACKFILL,
            }
            if force:
                body["force"] = True
            task_id = backfill_task_id(key, start_date, end_date, self.now, suffix=dedup_key)
            items.append((key, task_id, body))
        results = self._create_tasks(items)
        for key, result in zip(selected, results, strict=True):
            entry = {"agency": key, "task": result.task_id, "result": result.status}
            if result.status == FAILED:
                entry["error"] = _create_error(result)
            out.agencies.append(entry)
        self._count(out, results)
        return out

    def _states_by_key(self) -> dict[str, AgencyState]:
        """Estados para o disjuntor do backfill; indisponível = sem disjuntor (warning)."""
        try:
            with self._transaction():
                states = repository.list_states(self.conn)
        except StateUnavailable as e:
            logger.warning(f"Backfill sem o estado por agência ({e}): disjuntor ignorado")
            return {}
        return {s.agency_key: s for s in states}
