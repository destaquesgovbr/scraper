"""
Testes para validar sincronização de arquivos de configuração.
"""
import filecmp
from pathlib import Path


def test_site_urls_yaml_sync():
    """
    Valida que dags/config/site_urls.yaml está sincronizado com src/.

    Os arquivos site_urls.yaml existem em dois locais:
    - src/govbr_scraper/scrapers/config/site_urls.yaml (fonte editável)
    - dags/config/site_urls.yaml (cópia usada pelas DAGs)

    Este teste garante que estão sincronizados. Se divergirem, o desenvolvedor
    deve copiar manualmente:
        cp src/govbr_scraper/scrapers/config/site_urls.yaml dags/config/site_urls.yaml
    """
    repo_root = Path(__file__).parent.parent.parent

    source_file = repo_root / "src/govbr_scraper/scrapers/config/site_urls.yaml"
    dags_file = repo_root / "dags/config/site_urls.yaml"

    # Verificar que ambos existem
    assert source_file.exists(), f"Arquivo fonte não encontrado: {source_file}"
    assert dags_file.exists(), f"Arquivo DAG não encontrado: {dags_file}"

    # Verificar que são idênticos (byte-by-byte)
    assert filecmp.cmp(source_file, dags_file, shallow=False), (
        f"\n\n"
        f"ERROR: Arquivos site_urls.yaml estão dessincronizados!\n"
        f"\n"
        f"  Fonte:  {source_file}\n"
        f"  Cópia:  {dags_file}\n"
        f"\n"
        f"Para sincronizar:\n"
        f"  cp {source_file} {dags_file}\n"
        f"\n"
        f"IMPORTANTE: Sempre edite o arquivo em src/, não em dags/\n"
    )
