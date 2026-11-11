"""
Guarda pós-defeso eleitoral 2026 (scraper#61 e #62).

Durante o defeso (04/07 a 25/10/2026) várias agências publicaram notícias numa seção
temporária, e o scraper#61 apontou o scraper para ela. Depois do defeso essas seções
deixam de receber notícias e a agência passa a rodar com sucesso e 0 artigos: uma falha
silenciosa, que o monitoramento não pega. Este teste impede que uma URL de defeso
continue (ou volte) na configuração sem uma decisão explícita na allowlist.

A igualdade byte a byte entre as duas cópias do site_urls.yaml é garantida por
test_config_sync.py; aqui as duas cópias são verificadas, para o erro apontar o arquivo.
"""

from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILES = [
    REPO_ROOT / "src/govbr_scraper/scrapers/config/site_urls.yaml",
    REPO_ROOT / "dags/config/site_urls.yaml",
]
CONFIG_IDS = ["src", "dags"]

# Trechos de caminho que denunciam uma seção temporária de período eleitoral.
DEFESO_MARKERS = ("defeso", "eleitoral")

# URLs temporárias exatas do defeso 2026 (scraper#61; casaruibarbosa no #62, cnpq no
# #61 como "permanente"). Cobre também a de mcti, que não tem marcador no caminho.
DEFESO_2026_URLS = {
    "ana": "https://www.gov.br/ana/pt-br/assuntos/noticias-e-eventos/noticias-periodo-eleitoral-2026/",
    "ans": "https://www.gov.br/ans/pt-br/assuntos/noticias-1/periodo-eleitoral",
    "antt": "https://www.gov.br/antt/pt-br/assuntos/noticias-defeso-eleitoral",
    "capes": "https://www.gov.br/capes/pt-br/assuntos/noticias-defeso-eleitoral",
    "casaruibarbosa": "https://www.gov.br/casaruibarbosa/pt-br/centrais-de-conteudo/noticias/2026-defeso-eleitoral",
    "cgu": "https://www.gov.br/cgu/pt-br/assuntos/noticias/2026/07-defeso-eleitoral",
    "cnpq": "https://www.gov.br/cnpq/pt-br/assuntos/defeso-eleitoral",
    "cultura": "https://www.gov.br/cultura/pt-br/assuntos/noticias-defeso-eleitoral",
    "dnit": "https://www.gov.br/dnit/pt-br/assuntos/noticias-periodo-eleitoral",
    "iti": "https://www.gov.br/iti/pt-br/assuntos/noticias/periodo-de-defeso-eleitoral",
    "mcti": "https://www.gov.br/mcti/pt-br/acompanhe-o-mcti/noticias/noticias-julho-outubro-2026",
    "mma": "https://www.gov.br/mma/pt-br/noticias-defeso-eleitoral",
    "museus": "https://www.gov.br/museus/pt-br/comunicados-defeso-eleitoral",
    "palmares": "https://www.gov.br/palmares/pt-br/assuntos/noticias-defeso-eleitoral",
    "planejamento": "https://www.gov.br/planejamento/pt-br/assuntos/noticias-defeso-eleitoral-2026",
    "servidor": "https://www.gov.br/servidor/pt-br/assuntos/noticias-defeso-eleitoral",
}

# Agências que ainda podem ficar numa URL de defeso, com o motivo.
# Ao trocar a URL de uma delas, remova a entrada: test_defeso_allowlist_has_no_stale_entries
# falha se a agência não usar mais URL de defeso. ana e museus saíram em 11/11/2026, quando as
# URLs originais voltaram a listar as notícias recentes.
DEFESO_ALLOWLIST: dict[str, str] = {}


def _load_agencies(config_file: Path) -> dict[str, dict]:
    with open(config_file, encoding="utf-8") as f:
        return yaml.safe_load(f)["agencies"]


def _is_defeso_url(agency_key: str, url: str) -> bool:
    lowered = url.lower()
    return any(marker in lowered for marker in DEFESO_MARKERS) or (
        DEFESO_2026_URLS.get(agency_key) == url
    )


@pytest.mark.parametrize("config_file", CONFIG_FILES, ids=CONFIG_IDS)
def test_no_defeso_url_outside_allowlist(config_file: Path) -> None:
    agencies = _load_agencies(config_file)

    offenders = {
        key: data["url"]
        for key, data in agencies.items()
        if _is_defeso_url(key, str(data["url"])) and key not in DEFESO_ALLOWLIST
    }

    assert not offenders, (
        f"URLs de defeso eleitoral fora da allowlist em {config_file.relative_to(REPO_ROOT)}:\n"
        + "\n".join(f"  {key}: {url}" for key, url in sorted(offenders.items()))
        + "\nReverta para a URL de notícias vigente ou justifique em DEFESO_ALLOWLIST."
    )


@pytest.mark.parametrize("config_file", CONFIG_FILES, ids=CONFIG_IDS)
def test_defeso_allowlist_has_no_stale_entries(config_file: Path) -> None:
    agencies = _load_agencies(config_file)

    stale = [
        key
        for key in DEFESO_ALLOWLIST
        if key not in agencies or not _is_defeso_url(key, str(agencies[key]["url"]))
    ]

    assert not stale, (
        f"Entradas da DEFESO_ALLOWLIST que não usam mais URL de defeso: {stale}. "
        "Remova-as da allowlist."
    )


def test_defeso_detection_covers_known_temporary_urls() -> None:
    """Toda URL temporária conhecida do defeso 2026 é reconhecida como de defeso."""
    for key, url in DEFESO_2026_URLS.items():
        assert _is_defeso_url(key, url), f"{key}: {url}"

    assert not _is_defeso_url("mec", "https://www.gov.br/mec/pt-br/assuntos/noticias")
