# Use Python 3.11 slim image as base
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY youtube_hydrator.py .
#COPY *.py .
#COPY *.md .

# Create cache directory
#RUN mkdir -p /app/cache

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Default command
CMD ["python", "youtube_hydrator.py"]