# Use Python 3.11 base image
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source files
COPY . .

# Start Gunicorn
CMD ["gunicorn", "app:app"]
