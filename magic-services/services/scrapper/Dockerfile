FROM python:3.11-slim

# Set a working directory
WORKDIR /app

# Prevent Python from writing .pyc files and enable stdout/stderr flushing
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . /app

# Create directory for data (so volumes have predictable paths)
RUN mkdir -p /app/bulk-data

# Default command: run the scrapper in loop mode (the script will sleep between runs)
CMD ["python", "scryfall_scrapper.py"]
