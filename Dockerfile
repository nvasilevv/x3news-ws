FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-websocket.txt ./
RUN pip install --no-cache-dir -r requirements-websocket.txt

COPY x3news_feed.py ./
COPY x3news_store.py ./
COPY x3news_websocket.py ./

EXPOSE 8765

CMD ["python", "x3news_websocket.py"]
