FROM python:3.12-slim

WORKDIR /app

# System dependencies (minimal: only what scrapers + postgres need)
RUN apt-get update && apt-get install -y \
    build-essential gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install --no-cache-dir poetry

# Copy dependency files first (leverage Docker layer caching)
COPY pyproject.toml poetry.lock ./

# Configure Poetry to not create virtual environments (install globally)
RUN poetry config virtualenvs.create false

# Install dependencies (without the package itself)
RUN poetry install --no-root --no-interaction --no-ansi

# Copy readme (referenced in pyproject.toml) and source code
COPY CLAUDE.md ./
COPY src/ src/

# Install the package itself
RUN poetry install --no-interaction --no-ansi

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

EXPOSE 8080

CMD ["uvicorn", "govbr_scraper.api:app", "--host", "0.0.0.0", "--port", "8080"]
