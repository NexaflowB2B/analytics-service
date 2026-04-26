# Stage 1: Build
FROM python:3.11-slim AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Stage 2: Runtime
FROM python:3.11-slim
WORKDIR /app

# Patch OS vulnerabilities
RUN apt-get update && apt-get upgrade -y && pip install --upgrade pip wheel==0.46.2 setuptools && rm -rf /var/lib/apt/lists/*

COPY --from=builder /root/.local /root/.local
COPY . .
ENV PATH=/root/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1

EXPOSE 8005
CMD ["uvicorn", "analytics_service.main:app", "--host", "0.0.0.0", "--port", "8005"]
