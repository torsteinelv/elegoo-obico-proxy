FROM python:3.11-slim

WORKDIR /app

# Install git and tools needed for building
RUN apt-get update && apt-get install -y --no-install-recommends git janus && rm -rf /var/lib/apt/lists/*

# Clone the official Obico client
RUN git clone https://github.com/TheSpaghettiDetective/moonraker-obico.git /app/moonraker-obico

# Install dependencies for both the proxy and the Obico client
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r /app/moonraker-obico/requirements.txt

COPY . .

# Make entrypoint script executable and create logs directory
RUN chmod +x entrypoint.sh && mkdir -p /app/logs

EXPOSE 7125

ENTRYPOINT ["/app/entrypoint.sh"]
