# Use an official Python runtime as a base image
FROM python:3.12.5

# Install system dependencies including curl for health checks
RUN apt-get update && apt-get install -y \
    gnupg2 \
    curl \
    unixodbc \
    unixodbc-dev \
    libodbc2 \
    libodbc1 \
    odbcinst1debian2 \
    odbcinst \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Verify ODBC installation and drivers
RUN odbcinst -j && cat /etc/odbcinst.ini

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# No longer need startup script - orchestration integrated into server.py

# Create logs directory
RUN mkdir -p /app/logs

# Environment variables for configuration
ENV FLASK_ENV=production
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Expose the port that Flask will run on
EXPOSE 6000

# Add health check for the complete system
# The health check waits longer during startup to allow for warmup (35 min + buffer)
HEALTHCHECK --interval=60s --timeout=15s --start-period=2400s --retries=3 \
    CMD curl -f http://localhost:6000/trade_signal -X POST \
    -H "Content-Type: application/json" \
    -d '{"data":{"Time":"2024-01-01 12:00:00","Price":1.0},"currency":"EUR.USD"}' \
    || exit 1

# Run the Flask server which now orchestrates both services internally
CMD ["python", "server.py"]