# Elegoo CC2 to Moonraker/Obico Proxy (v1.0.0)

This project acts as a proxy translating Elegoo CC2 MQTT packets to Moonraker-compatible Klipper JSON format, allowing you to connect your Elegoo printer to Obico for remote monitoring and AI failure detection.

## Core Features

- MQTT to Klipper Translation: Translates Elegoo MQTT packets to Klipper JSON format.
- Print Control: Supports Pause, Resume, and Cancel commands.
- Interactive G-code Endpoint: Features an interactive G-code endpoint (GET/POST on /printer/gcode/script).
- Webcam Routing: Routes the native MJPEG webcam stream directly.
- Database Normalization: Normalizes database queries (POST /server/database/item) to prevent Obico from crashing.

## Limitations

- Direct G-code Uploads: Uploading G-code files directly via Obico to start a print is not supported in v1.0.0. Prints must be started locally on the printer.

## Deployment Guide

### Docker Compose

Create a docker-compose.yml file:

    version: "3.8"
    
    services:
      elegoo-proxy:
        image: elegoo-obico-proxy:latest
        build: .
        container_name: elegoo-obico-proxy
        restart: unless-stopped
        ports:
          - "7125:7125"
        environment:
          - PRINTER_IP=10.10.11.41
          - SERIAL_NUMBER=CC2XXXXXXXXXXXX
          - ACCESS_CODE=YOUR_ACCESS_CODE
          - OBICO_AUTH_TOKEN=YOUR_OBICO_AUTH_TOKEN
          - OBICO_URL=https://app.obico.io
        volumes:
          - ./logs:/app/logs

### Kubernetes Deployment

Create the following manifests:

    apiVersion: v1
    kind: Secret
    metadata:
      name: elegoo-proxy-secret
    type: Opaque
    stringData:
      access-code: "YOUR_ACCESS_CODE"
      obico-auth-token: "YOUR_OBICO_AUTH_TOKEN"
    ---
    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: elegoo-proxy
      labels:
        app: elegoo-proxy
    spec:
      replicas: 1
      strategy:
        type: Recreate
      selector:
        matchLabels:
          app: elegoo-proxy
      template:
        metadata:
          labels:
            app: elegoo-proxy
        spec:
          containers:
            - name: proxy
              image: elegoo-obico-proxy:latest
              ports:
                - containerPort: 7125
              env:
                - name: PRINTER_IP
                  value: "10.10.11.41"
                - name: SERIAL_NUMBER
                  value: "CC2XXXXXXXXXXXX"
                - name: ACCESS_CODE
                  valueFrom:
                    secretKeyRef:
                      name: elegoo-proxy-secret
                      key: access-code
                - name: OBICO_AUTH_TOKEN
                  valueFrom:
                    secretKeyRef:
                      name: elegoo-proxy-secret
                      key: obico-auth-token
                - name: OBICO_URL
                  value: "https://app.obico.io"
              volumeMounts:
                - name: logs-volume
                  mountPath: /app/logs
          volumes:
            - name: logs-volume
              emptyDir: {}

## Printer Linking Protocol (The Hack)

To bypass Obico's local network scan, you must manually link the printer by executing into the running container:

1. Exec into the container:
   - For Docker:
     docker exec -it elegoo-obico-proxy /bin/sh
   - For Kubernetes:
     kubectl exec -it deployment/elegoo-proxy -- /bin/sh

2. Generate the link configuration:
   Inside the container, run the linking module:
   python3 -m moonraker_obico.link -c /app/moonraker-obico.cfg

3. Retrieve and Save the Token:
   Follow the on-screen instructions to link your printer with Obico. Once linked, extract the generated auth_token from /app/moonraker-obico.cfg and update your deployment configuration (OBICO_AUTH_TOKEN environment variable) to make it permanent.

## Roadmap (v2.0.0)

We invite contributions to reverse-engineer the Elegoo file transfer protocol to support direct G-code uploads in future releases.

## Acknowledgments & Credits

This project stands on the shoulders of giants. Special thanks to the open-source creators who made this proxy possible:

- **[danielcherubini/elegoo-homeassistant](https://github.com/danielcherubini/elegoo-homeassistant):** For the stellar reverse-engineering work on the Elegoo CC2 (Centauri Carbon 2) MQTT architecture, client registration protocol, and packet structure mappings.
- **[Moonraker-Obico](https://github.com/TheSpaghettiDetective/moonraker-obico):** For the official client daemon linking Klipper API standards to the Obico smart monitoring framework.
- **[Klipper](https://github.com/Klipper3d/klipper) & [Moonraker](https://github.com/Arksine/moonraker):** For compiling the robust open-source 3D printing ecosystem APIs emulated by this proxy.
