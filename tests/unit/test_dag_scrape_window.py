"""
Janela de datas das DAGs scrape_* (scrape_agencies e scrape_ebc).

As listagens gov.br/EBC datam as notícias no horário de Brasília. Com a janela calculada
em UTC ((logical_date - 1h).date em UTC), das 22:00 às 23:59 BRT a data UTC já é o dia
seguinte e a DAG pula as notícias do dia em Brasília: nos 30 dias até 02/11/2026 não há
nenhum artigo gov.br com publicação entre 22h e 23h59 BRT. A janela passa a ser calculada
em America/Sao_Paulo, mantendo a sobreposição de 1 hora na virada do dia.
"""

import sys
from datetime import UTC, datetime
from unittest.mock import MagicMock, mock_open, patch

import pytest
import yaml

SAMPLE_YAML = {"agencies": {"mec": {"url": "https://www.gov.br/mec/pt-br/noticias"}}}


def _airflow_mocks():
    def fake_dag(**kwargs):
        def wrapper(fn):
            return fn

        return wrapper

    def fake_task(fn):
        return MagicMock(name=f"task_{fn.__name__}")

    decorators = MagicMock()
    decorators.dag = fake_dag
    decorators.task = fake_task
    return {
        "airflow": MagicMock(),
        "airflow.decorators": decorators,
        "airflow.models": MagicMock(),
        "airflow.exceptions": MagicMock(),
    }


def _load(module_name):
    for key in list(sys.modules):
        if key.startswith(module_name):
            del sys.modules[key]
    with (
        patch.dict(sys.modules, _airflow_mocks()),
        patch("builtins.open", mock_open(read_data=yaml.dump(SAMPLE_YAML))),
    ):
        module = __import__(module_name, fromlist=["_scrape_window"])
    return module


@pytest.fixture(params=["dags.scrape_agencies", "dags.scrape_ebc"])
def dag_module(request):
    module = _load(request.param)
    yield module
    for key in list(sys.modules):
        if key.startswith(request.param):
            del sys.modules[key]


@pytest.mark.parametrize(
    ("logical_date", "esperado"),
    [
        # 22:30 BRT de 02/11: a janela ainda é 02/11 (em UTC já seria 03/11)
        (datetime(2026, 11, 3, 1, 30, tzinfo=UTC), ("2026-11-02", "2026-11-02")),
        # 23:55 BRT de 02/11
        (datetime(2026, 11, 3, 2, 55, tzinfo=UTC), ("2026-11-02", "2026-11-02")),
        # 00:30 BRT de 03/11: sobreposição de 1 hora pega o fim de 02/11
        (datetime(2026, 11, 3, 3, 30, tzinfo=UTC), ("2026-11-02", "2026-11-03")),
        # 01:30 BRT de 03/11: só o dia corrente
        (datetime(2026, 11, 3, 4, 30, tzinfo=UTC), ("2026-11-03", "2026-11-03")),
        # meio da tarde
        (datetime(2026, 11, 3, 18, 0, tzinfo=UTC), ("2026-11-03", "2026-11-03")),
    ],
)
def test_scrape_window_usa_horario_de_brasilia(dag_module, logical_date, esperado):
    assert dag_module._scrape_window(logical_date) == esperado


def test_scrape_window_trata_datetime_ingenuo_como_utc(dag_module):
    assert dag_module._scrape_window(datetime(2026, 11, 3, 1, 30)) == ("2026-11-02", "2026-11-02")


def test_scrape_window_aceita_logical_date_em_outro_fuso(dag_module):
    """O Airflow entrega logical_date em UTC (pendulum), mas o resultado não depende do fuso."""
    from zoneinfo import ZoneInfo

    brt = datetime(2026, 11, 2, 22, 30, tzinfo=ZoneInfo("America/Sao_Paulo"))
    assert dag_module._scrape_window(brt) == ("2026-11-02", "2026-11-02")
