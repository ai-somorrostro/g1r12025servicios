FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt ./
RUN pip install --no-cache-dir --default-timeout=600 -r requirements.txt

# Copy entire api directory so helper modules (api_log_manager) are available
COPY . /app/

# Directory where scrapper will write data (mounted from host)
RUN mkdir -p /data/bulk-data

# Expose port
EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
