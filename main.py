"""Elegoo CC2 -> Moonraker-proxy.

Startopp:
  1. Last inn felles tilstand fra state
  2. Opprett FastAPI-app
  3. Start bakgrunnstråd for kamera-cache
  4. Koble opp MQTT mot printeren
  5. Start uvicorn på port 7125
"""
import sys
import threading

import uvicorn
from fastapi import FastAPI

from state import (
    PRINTER_IP,
    logger,
)
from mqtt_client import (
    create_mqtt_client,
    mqtt_heartbeat_loop,
    on_connect,
    on_message,
)
from camera import webcam_stream_cache_worker, register_camera_routes
from moonraker_api import register_routes as register_moonraker_routes

# --- App ---
app = FastAPI(title="Elegoo CC2 Obico Proxy")


def main() -> None:
    """Oppsett og oppstart av alle komponenter."""

    # 1. Kamera-bakgrunnstråd
    cam_thread = threading.Thread(target=webcam_stream_cache_worker, daemon=True)
    cam_thread.start()

    # 2. MQTT-kommunikasjon mot printeren
    logger.info("Connecting to printer at IP: %s ...", PRINTER_IP)
    try:
        mqtt_client = create_mqtt_client()
        mqtt_client.on_connect = on_connect
        mqtt_client.on_message = on_message
        mqtt_client.connect(PRINTER_IP, 1883, 60)
        threading.Thread(target=mqtt_client.loop_forever, daemon=True).start()
        threading.Thread(target=mqtt_heartbeat_loop, args=(mqtt_client,), daemon=True).start()
    except Exception as e:
        logger.error("Critical error: Could not start MQTT connection: %s", e)
        sys.exit(1)

    # 3. Registrer HTTP- og WebSocket-ruter
    register_camera_routes(app)
    register_moonraker_routes(app)

    # 4. Start FastAPI
    logger.info("Starting Moonraker emulation on port 7125...")
    uvicorn.run(app, host="0.0.0.0", port=7125, loop="asyncio")


if __name__ == "__main__":
    main()
