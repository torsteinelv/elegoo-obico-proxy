​Please generate a professional, Docker and Kubernetes-ready README.md file for the repository. The project is an "Elegoo CC2 to Moonraker/Obico Proxy" (v1.0.0).
​Requirements for the README.md:
​Language: English.
​Formatting: Clean, standard Markdown. Do not include any private variables (use placeholders like YOUR_ACCESS_CODE, CC2XXXXXXXXXXXX, and 10.10.11.41).
​Core Features Section: Explain that it translates Elegoo MQTT packets to Klipper JSON, supports Pause/Resume/Cancel, features an interactive G-code endpoint (GET/POST on /printer/gcode/script), routes the native MJPEG webcam stream, and normalizes database queries (POST /server/database/item) to prevent Obico from crashing.
​Limitations Section: Mention that uploading G-code files directly via Obico to start a print is not supported in v1; prints must be started locally.
​Deployment Guide: Provide clear, copy-pasteable YAML examples for both Docker Compose (docker-compose.yml) and Kubernetes (Deployment and Secret manifests). Ensure the Kubernetes strategy is explicitly set to type: Recreate.
​Printer Linking Protocol (The Hack): Provide step-by-step instructions on how to bypass Obico's local network scan by manually exec'ing into the container (docker exec or kubectl exec), generating a local moonraker-obico.cfg file pointing to the server, and running python3 -m moonraker_obico.link -c /app/moonraker-obico.cfg. Explain how to extract the generated auth_token and make it permanent in the deployment config.
​Roadmap (v2.0.0): Add a section inviting contributions to reverse-engineer the Elegoo file transfer protocol for future direct G-code uploads.
​Please output only the raw Markdown content so it can be saved directly as README.md.
