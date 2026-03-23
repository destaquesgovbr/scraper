# Configuração das DAGs

## site_urls.yaml

**⚠️ IMPORTANTE: Este arquivo é mantido sincronizado manualmente**

Este arquivo deve ser **idêntico** a `src/govbr_scraper/scrapers/config/site_urls.yaml`.

### Por que dois arquivos?

- **DAGs do Airflow**: Autocontidas, não importam código Python da API
- **API Cloud Run**: Usa o arquivo em `src/` empacotado no Docker
- **Separação de responsabilidades**: DAGs e API são componentes independentes

### Como modificar a configuração de agências

1. **Sempre edite** `src/govbr_scraper/scrapers/config/site_urls.yaml` (arquivo fonte)
2. **Copie manualmente** para este arquivo:
   ```bash
   cp src/govbr_scraper/scrapers/config/site_urls.yaml dags/config/site_urls.yaml
   ```
3. Faça commit de ambos os arquivos
4. Abra PR - o CI validará que estão sincronizados via `test_config_sync.py`

### Estrutura do YAML

```yaml
agencies:
  agency_key:
    url: string (obrigatório)
    active: bool (opcional, default: true)
    disabled_reason: string (opcional)
    disabled_date: string (opcional)
```

Apenas agências com `active: true` geram DAGs no Airflow.

### Migração Futura

Esta configuração pode migrar para banco de dados PostgreSQL no futuro, eliminando a necessidade de sincronização manual. Quando isso acontecer, ambos os arquivos YAML serão removidos.
