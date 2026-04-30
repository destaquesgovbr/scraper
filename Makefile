.PHONY: help test test-unit test-integration clean

# Default target
help:
	@echo "DestaquesGovBr Scraper - Makefile Commands"
	@echo ""
	@echo "Testing Commands:"
	@echo "  make test             - Run all tests"
	@echo "  make test-unit        - Run unit tests only"
	@echo "  make test-integration - Run integration tests only"
	@echo ""
	@echo "Cleanup Commands:"
	@echo "  make clean            - Clean Python cache files"

# testing commands
test:
	PYTHONPATH=src poetry run pytest tests/ -v

test-unit:
	PYTHONPATH=src poetry run pytest tests/unit/ -v

test-integration:
	DATABASE_URL="postgresql://destaquesgovbr_dev:dev_password@localhost:5433/destaquesgovbr_dev" \
		PYTHONPATH=src poetry run pytest tests/integration/ -v --no-cov

# Cleanup commands
clean:
	@echo "Cleaning Python cache files..."
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true