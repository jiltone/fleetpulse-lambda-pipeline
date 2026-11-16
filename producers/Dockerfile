# Image shared by both producers. Build context = project root (so common/ is included).
FROM python:3.11-slim

WORKDIR /app
# Print logs immediately and let `python -m producers.x` find our packages.
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

COPY producers/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY common/ /app/common/
COPY producers/ /app/producers/
