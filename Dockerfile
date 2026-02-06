FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements/ requirements/

RUN pip install --no-cache-dir pip-tools \
    && pip-compile requirements/base.in \
        --output-file requirements/base.txt \
        --resolver=backtracking \
    && pip install --no-cache-dir -r requirements/base.txt

RUN dbt --version

COPY . .

ENTRYPOINT ["python", "main.py"]