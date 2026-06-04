FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml /app/pyproject.toml
COPY src /app/src
COPY apps/api /app/apps/api

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e .[api]

WORKDIR /app/apps/api/src
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
