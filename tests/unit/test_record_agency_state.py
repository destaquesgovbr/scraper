"""record_scrape_run atualiza scrape_agency_state num SAVEPOINT (conexão mockada).

O comportamento contra um Postgres real (com e sem a tabela) está em
tests/integration/test_postgres_agency_state.py.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from govbr_scraper.models.monitoring import ErrorCategory, ScrapeRunResult
from govbr_scraper.state.model import AgencyState, CircuitState
from govbr_scraper.state.repository import StateUnavailable
from govbr_scraper.storage.postgres_manager import PostgresManager
from govbr_scraper.storage.storage_adapter import StorageAdapter

T0 = datetime(2026, 11, 19, 13, 0, tzinfo=UTC)
MODULE = "govbr_scraper.storage.postgres_manager"


def _pg():
    pg = PostgresManager.__new__(PostgresManager)
    pg.pool = MagicMock()
    conn = MagicMock()
    pg.pool.getconn.return_value = conn
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return pg, conn, cursor


def _run(**overrides) -> ScrapeRunResult:
    data = {
        "agency_key": "mec",
        "status": "error",
        "error_category": ErrorCategory.HTML_CHANGED,
        "error_message": "No articles found on first page of mec",
        "execution_time_seconds": 2.0,
        "scraped_at": T0,
    }
    data.update(overrides)
    return ScrapeRunResult(**data)


def _sql_calls(cursor) -> list[str]:
    return [c[0][0] for c in cursor.execute.call_args_list]


@pytest.fixture
def repo():
    with patch(f"{MODULE}.state_repository") as repository:
        repository.get_state.return_value = None
        yield repository


@pytest.fixture
def announce():
    with patch("govbr_scraper.state.alerts.announce_transition") as announce:
        yield announce


@pytest.fixture(autouse=True)
def _reset_warning(monkeypatch):
    monkeypatch.setattr(PostgresManager, "_warned_missing_agency_state", False)


class TestRecordAgencyState:
    def test_state_is_updated_inside_a_savepoint_before_commit(self, repo, announce):
        pg, conn, cursor = _pg()
        order = []
        conn.commit.side_effect = lambda: order.append("commit")
        repo.upsert_state.side_effect = lambda c, s: order.append("upsert")

        assert pg.record_scrape_run(_run()) is None

        sqls = _sql_calls(cursor)
        assert sqls[0].startswith("INSERT INTO scrape_runs")
        assert sqls[1] == "SAVEPOINT agency_state"
        assert sqls[-1] == "RELEASE SAVEPOINT agency_state"
        repo.ensure_rows.assert_called_once()
        assert repo.ensure_rows.call_args[0][1] == ["mec"]
        assert repo.get_state.call_args == ((conn, "mec"), {"for_update": True})
        assert order == ["upsert", "commit"]
        announce.assert_not_called()

    def test_new_row_starts_from_default_and_counts_failure(self, repo, announce):
        pg, _, _ = _pg()
        pg.record_scrape_run(_run())
        (_, new_state), _ = repo.upsert_state.call_args
        assert new_state.agency_key == "mec"
        assert new_state.consecutive_permanent_failures == 1
        assert new_state.last_attempt_at == T0 - timedelta(seconds=2)
        assert new_state.last_error_category == "html_changed"

    def test_transition_is_announced_after_commit(self, repo, announce):
        pg, conn, _ = _pg()
        prev = replace(
            AgencyState.new("mec", T0 - timedelta(days=1)), consecutive_permanent_failures=1
        )
        repo.get_state.return_value = prev
        order = []
        conn.commit.side_effect = lambda: order.append("commit")
        announce.side_effect = lambda t: order.append("announce")

        transition = pg.record_scrape_run(_run())

        assert transition.to_state == CircuitState.OPEN
        assert transition.reason == "permanent_errors"
        announce.assert_called_once_with(transition)
        assert order == ["commit", "announce"]

    def test_explicit_window_keeps_the_state(self, repo, announce):
        pg, _, _ = _pg()
        prev = replace(
            AgencyState.new("mec", T0 - timedelta(days=1)), consecutive_permanent_failures=1
        )
        repo.get_state.return_value = prev
        assert pg.record_scrape_run(_run(), explicit_window=True) is None
        (_, new_state), _ = repo.upsert_state.call_args
        assert new_state.consecutive_permanent_failures == 1
        announce.assert_not_called()

    def test_forced_success_closes_with_forced_reason(self, repo, announce):
        pg, _, _ = _pg()
        repo.get_state.return_value = replace(
            AgencyState.new("mec", T0), circuit_state=CircuitState.OPEN
        )
        transition = pg.record_scrape_run(
            _run(status="success", error_category=None, error_message=None), forced=True
        )
        assert transition.reason == "forced_success"

    def test_explicit_started_at(self, repo, announce):
        pg, _, _ = _pg()
        pg.record_scrape_run(_run(), started_at=T0 - timedelta(minutes=10))
        (_, new_state), _ = repo.upsert_state.call_args
        assert new_state.last_attempt_at == T0 - timedelta(minutes=10)

    def test_missing_table_rolls_back_only_the_savepoint_and_warns_once(self, repo, announce):
        pg, conn, cursor = _pg()
        repo.ensure_rows.side_effect = StateUnavailable("scrape_agency_state indisponível")

        with patch(f"{MODULE}.logger") as log:
            pg.record_scrape_run(_run())
            pg.record_scrape_run(_run())

        sqls = _sql_calls(cursor)
        assert sqls.count("ROLLBACK TO SAVEPOINT agency_state") == 2
        assert sum(s.startswith("INSERT INTO scrape_runs") for s in sqls) == 2
        assert conn.commit.call_count == 2
        conn.rollback.assert_not_called()
        assert log.warning.call_count == 1
        assert "038" in log.warning.call_args[0][0]
        announce.assert_not_called()

    def test_other_state_errors_are_warned_and_run_is_kept(self, repo, announce):
        pg, conn, cursor = _pg()
        repo.upsert_state.side_effect = RuntimeError("deadlock detected")

        with patch(f"{MODULE}.logger") as log:
            assert pg.record_scrape_run(_run()) is None

        assert "ROLLBACK TO SAVEPOINT agency_state" in _sql_calls(cursor)
        conn.commit.assert_called_once()
        assert "deadlock" in log.warning.call_args[0][0]

    def test_insert_failure_skips_state_and_raises(self, repo, announce):
        pg, conn, cursor = _pg()
        cursor.execute.side_effect = RuntimeError("conexão caiu")
        with pytest.raises(RuntimeError):
            pg.record_scrape_run(_run())
        repo.ensure_rows.assert_not_called()
        conn.rollback.assert_called_once()
        announce.assert_not_called()


class TestPostgresManagerStateHelpers:
    def test_list_agency_states_returns_connection(self, repo):
        pg, conn, _ = _pg()
        repo.list_states.return_value = ["x"]
        assert pg.list_agency_states() == ["x"]
        pg.pool.putconn.assert_called_once_with(conn)

    def test_list_agency_states_propagates_unavailable(self, repo):
        pg, conn, _ = _pg()
        repo.list_states.side_effect = StateUnavailable("x")
        with pytest.raises(StateUnavailable):
            pg.list_agency_states()
        pg.pool.putconn.assert_called_once_with(conn)

    def test_mark_agency_probe_commits(self, repo):
        pg, conn, _ = _pg()
        repo.mark_probe.return_value = True
        assert pg.mark_agency_probe("mec", T0) is True
        repo.mark_probe.assert_called_once_with(conn, "mec", T0)
        conn.commit.assert_called_once()

    def test_mark_agency_probe_rolls_back_on_error(self, repo):
        pg, conn, _ = _pg()
        repo.mark_probe.side_effect = StateUnavailable("x")
        with pytest.raises(StateUnavailable):
            pg.mark_agency_probe("mec", T0)
        conn.rollback.assert_called_once()


class TestStorageAdapterState:
    def test_load_agency_states_by_key(self):
        pg = MagicMock()
        pg.list_agency_states.return_value = [
            AgencyState.new("mec", T0),
            AgencyState.new("mds", T0),
        ]
        states = StorageAdapter(postgres_manager=pg).load_agency_states()
        assert sorted(states) == ["mds", "mec"]

    @pytest.mark.parametrize("error", [StateUnavailable("x"), RuntimeError("banco fora")])
    def test_load_agency_states_unavailable_is_none(self, error):
        pg = MagicMock()
        pg.list_agency_states.side_effect = error
        assert StorageAdapter(postgres_manager=pg).load_agency_states() is None

    def test_mark_agency_probe_never_raises(self):
        pg = MagicMock()
        pg.mark_agency_probe.side_effect = RuntimeError("x")
        assert StorageAdapter(postgres_manager=pg).mark_agency_probe("mec", T0) is False

    def test_close_closes_pool_once(self):
        pg = MagicMock()
        adapter = StorageAdapter(postgres_manager=pg)
        adapter.close()
        adapter.close()
        pg.close_all.assert_called_once()

    def test_close_also_closes_the_event_publisher(self):
        pg = MagicMock()
        adapter = StorageAdapter(postgres_manager=pg)
        adapter._event_publisher = MagicMock()
        adapter._event_publisher.close.side_effect = RuntimeError("x")  # não impede o pool
        adapter.close()
        adapter._event_publisher.close.assert_called()
        pg.close_all.assert_called_once()

    def test_close_without_pool_is_noop(self):
        StorageAdapter().close()
