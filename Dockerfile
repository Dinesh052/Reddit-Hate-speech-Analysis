FROM python:3.10-slim

LABEL description="Reddit Hate Speech Pipeline"

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY hate_speech_pipeline /app/hate_speech_pipeline
COPY .env /app/hate_speech_pipeline/.env

# Expose FastAPI port
EXPOSE 8000

# Default command: run the dashboard
CMD ["uvicorn", "hate_speech_pipeline.reddit_dashboard:app", "--host", "0.0.0.0", "--port", "8000"]
