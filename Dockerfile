FROM python:3.11-slim

WORKDIR /app

# Install build dependencies for numpy/pandas and git for pip installs from GitHub
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN python -m pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt \
    && python -c "from iqoptionapi.stable_api import IQ_Option; print('iqoptionapi.stable_api import OK')"

COPY iq_signal_worker.py .

# Railway healthcheck: worker must be connected to IQ Option
CMD ["python", "iq_signal_worker.py"]
