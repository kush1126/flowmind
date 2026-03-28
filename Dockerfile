# Use stable Python 3.11
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install basic build tools (just in case)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Expose port 8000 (standard for FastAPI)
EXPOSE 8000

# Set environment variable defaults
ENV PORT=8000
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "main.py"]
