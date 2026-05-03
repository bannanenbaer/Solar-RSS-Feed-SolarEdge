FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY rss_server.py .
COPY start.sh .
RUN chmod +x /app/start.sh

VOLUME ["/data"]
EXPOSE 5000

CMD ["/app/start.sh"]
