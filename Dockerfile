FROM python:3.10-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir fastapi uvicorn psycopg2-binary mysql-connector-python

# Copy application files
COPY alert_resolution_api.py .
COPY testing_queries/ ./testing_queries/

# Expose port
EXPOSE 5050

# Run the application
CMD ["python3", "alert_resolution_api.py"]
