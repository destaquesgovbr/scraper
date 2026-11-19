"""Constantes da política por agência: disjuntor, intervalo, encurtamento e marca d'água.

Ref: scraper/_plan/rearquitetura-ingestao.md (Passo 2) e o desenho de 19/11/2026.
"""

from datetime import timedelta
from zoneinfo import ZoneInfo

from govbr_scraper.state.model import Tier

# Erros permanentes seguidos (desde o último sucesso) que abrem o circuito.
PERMANENT_FAILURES_TO_OPEN = 2

# Sequência de falhas (de qualquer tipo) que abre o circuito mesmo sem erro permanente.
TRANSIENT_STREAK_TO_OPEN = timedelta(hours=24)

# Espera até a sonda do circuito aberto: a sonda n usa PROBE_DELAYS[min(n, 2)], ou seja,
# 2 h, 6 h, 24 h e depois a cada 24 h. Falsos positivos de html_changed/access_denied
# duram minutos ou horas; com teste só diário, cada um deixaria a agência ~24 h sem raspar.
PROBE_DELAYS = (timedelta(hours=2), timedelta(hours=6), timedelta(hours=24))

# next_due_at = início da tentativa + intervalo − DUE_SLACK: sem a folga, o tick seguinte
# chegaria segundos antes do vencimento e um intervalo de 30 min viraria 45.
DUE_SLACK = timedelta(minutes=5)

# Tolerância do vencimento (meio tick de 15 min do /dispatch): o despachante escolhe o que
# vence até now + DUE_LOOKAHEAD, e o disjuntor da API aceita a sonda na mesma tolerância,
# senão pularia uma sonda despachada alguns minutos antes do vencimento.
DUE_LOOKAHEAD = timedelta(minutes=7)

# Notícia nova encurta o intervalo pela metade durante BOOST_DURATION, só nestas faixas
# (a m30 fica no piso de 30 min).
BOOST_DURATION = timedelta(hours=6)
BOOST_TIERS = frozenset({Tier.H2, Tier.H6, Tier.D1})

# Cadência das DAGs scrape_agencies/scrape_ebc (0 6-22/2): enquanto elas são o relógio
# (Passo 2 e /dispatch fora de "live"), toda agência é raspada a cada 2 h, então o SLO não
# pode exigir um intervalo menor que esse (m30 e boost só valem com o /dispatch em live).
DAG_CADENCE_MINUTES = 120

# A janela pela marca d'água volta no máximo 7 dias (backfill maior: scrape_backfill).
WATERMARK_MAX_DAYS = 7

# As listagens datam as notícias em horário de Brasília.
BRT = ZoneInfo("America/Sao_Paulo")

# last_error_message é truncado como scrape_runs.error_message.
ERROR_MESSAGE_MAX_CHARS = 500
