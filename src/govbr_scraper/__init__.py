from govbr_scraper.logging_setup import configure_logging

# Logs em JSON por linha no stderr (Cloud Logging), ou no formato de texto do loguru com
# LOG_FORMAT=text (uso local e testes). LOG_LEVEL define o nível (padrão INFO).
configure_logging()
