# Runs both simulated data sources (telemetry producer, expense generator).
# Application code is bind-mounted from ./common and ./sim; the image only
# carries dependencies so code edits don't require a rebuild.
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
