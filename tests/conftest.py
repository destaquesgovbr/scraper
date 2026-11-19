"""Configuração comum dos testes.

Os logs do pacote saem em JSON por padrão (Cloud Logging). Nos testes vale o formato de
texto, definido antes de qualquer import de ``govbr_scraper`` (o ``__init__`` configura os
logs no import).
"""

import os

os.environ.setdefault("LOG_FORMAT", "text")
