FROM python:3.12-slim

WORKDIR /app

# ==============================
# System dependencies (fix OpenCV error)
# ==============================
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxrender1 \
    libxext6 \
    libxcb1 \
    && rm -rf /var/lib/apt/lists/*

# ==============================
# Install Python dependencies (cached layer)
# ==============================
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# ==============================
# Copy source code (LAST for fast rebuilds)
# ==============================
COPY . .

# ==============================
# Streamlit config (important for AWS)
# ==============================
ENV PYTHONUNBUFFERED=1
ENV STREAMLIT_SERVER_HEADLESS=true

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]