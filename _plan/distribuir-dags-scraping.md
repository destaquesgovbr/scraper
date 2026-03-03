# Plano: Distribuir DAGs de scraping uniformemente + reduzir intervalo para 10 min

## Contexto

Atualmente, as ~156 DAGs de scraping (155 agências gov.br + 1 EBC) disparam **todas simultaneamente** a cada 15 minutos (`*/15 * * * *`). Isso gera um pico de ~156 requests HTTP concorrentes ao Cloud Run Scraper API a cada 15 minutos, seguido de inatividade.

**Objetivos:**
1. Distribuir as DAGs uniformemente ao longo da janela de 10 minutos
2. Reduzir o intervalo de 15 para 10 minutos (maior freshness das notícias)

## Abordagem: Cron com offsets por minuto

Com intervalo de 10 minutos, temos 10 slots de minuto disponíveis (0-9). Distribuindo 156 DAGs:

| Offset (minuto) | Cron expression | DAGs nesse slot |
|------------------|-----------------|-----------------|
| 0 | `0,10,20,30,40,50 * * * *` | ~16 |
| 1 | `1,11,21,31,41,51 * * * *` | ~16 |
| 2 | `2,12,22,32,42,52 * * * *` | ~16 |
| ... | ... | ~16 |
| 9 | `9,19,29,39,49,59 * * * *` | ~16 |

**Resultado:** em vez de 156 DAGs simultâneas, máximo de ~16 por minuto.

A sintaxe cron `{offset}/10` (e.g., `3/10`) é equivalente e mais limpa:
- `0/10` → minutos 0, 10, 20, 30, 40, 50
- `3/10` → minutos 3, 13, 23, 33, 43, 53

## Arquivos a modificar

| Arquivo | Mudança |
|---------|---------|
| `scraper/dags/scrape_agencies.py` | Adicionar `minute_offset` ao factory, calcular offset por índice |
| `scraper/dags/scrape_ebc.py` | Alterar schedule de `*/15` para offset fixo (e.g., `0/10`) |
| `scraper/CLAUDE.md` | Atualizar documentação do schedule |

## Mudanças detalhadas

### `scraper/dags/scrape_agencies.py`

**1. Alterar `create_scraper_dag` para aceitar `minute_offset`:**

```python
def create_scraper_dag(agency_key: str, agency_url: str, minute_offset: int = 0):
    @dag(
        dag_id=f"scrape_{agency_key}",
        schedule=f"{minute_offset}/10 * * * *",  # Era: */15 * * * *
        # ... resto igual
    )
```

**2. Alterar o loop de geração para calcular offsets:**

```python
agencies = sorted(_load_agencies_config().items())
for idx, (key, url) in enumerate(agencies):
    minute_offset = idx % 10
    globals()[f"scrape_{key}"] = create_scraper_dag(key, url, minute_offset)
```

O `sorted()` garante ordenação determinística (mesmo offset sempre para mesma agência).

**3. Atualizar `execution_timeout`** de 15 para 10 minutos (compatível com a nova janela):

```python
"execution_timeout": timedelta(minutes=10),
```

**4. Atualizar docstring** no topo do arquivo.

### `scraper/dags/scrape_ebc.py`

- Alterar schedule de `*/15 * * * *` para `0/10 * * * *` (roda no minuto 0 junto com o primeiro grupo de agências)

### `scraper/CLAUDE.md`

- Alterar referências de "15min" para "10min"
- Mencionar a distribuição por offset

## Verificação

1. Após deploy das DAGs, verificar no Airflow UI:
   - As DAGs têm schedules diferentes (não todas `*/15`)
   - Confirmar que os schedules são `0/10`, `1/10`, ..., `9/10`
2. Observar por 10 minutos que os DAG runs iniciam escalonados (não todos ao mesmo tempo)
3. Verificar que nenhuma DAG ficou sem schedule
