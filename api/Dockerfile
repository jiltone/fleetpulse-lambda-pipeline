# FastAPI serving layer. Build context = project root (so common/ is included).
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

COPY api/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY common/ /app/common/
COPY api/ /app/api/

EXPOSE 8000
# --no-access-log: our middleware already writes one JSON log line per request.
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
