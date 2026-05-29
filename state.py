"""Felles tilstand, miljøvariabler og trådlåser for proxyen."""
import os
import time
import secrets
import logging
from threading import RLock

# Konfigurasjon fra miljøvariabler
PRINTER_IP = os.getenv("PRINTER_IP", "192.168.1.100")
SERIAL_NUMBER = os.getenv("SERIAL_NUMBER", "CC2ABCD123456789")
ACCESS_CODE = os.getenv("ACCESS_CODE", "123456")

# Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("ElegooObicoProxy")

# --- Genererte identifikatorer (match Elegoo's format) ---
timestamp_hex = format(int(time.time() * 1000), "x")[-5:]
random_hex = secrets.token_hex(3)
CLIENT_ID = f"0cli{timestamp_hex}{random_hex}"[:10]

uuid_part = "".join(
    format(secrets.randbelow(16) if c == "x" else (secrets.randbelow(4) + 8), "x")
    for c in "xxxxxxxxxxxxxxxx"
)
timestamp_hex_long = format(int(time.time() * 1000), "x")
REQUEST_ID = f"{uuid_part}{timestamp_hex_long}"

logger.info(f"Generated client identifiers: CLIENT_ID={CLIENT_ID}, REQUEST_ID={REQUEST_ID}")

# --- Printer tilstand (cache) ---
elegoo_status_cache = {}
elegoo_status_lock = RLock()

# --- WebSocket klienter ---
active_websocket_clients = set()
active_ws_lock = RLock()

# --- MQTT tilstand ---
mqtt_client_connected = False
fastapi_loop = None

# --- MQTT-klient (deltes på tvers av moduler) ---
mqtt_client = None

# --- Kamera-cache (RAM) for å beskytte printeren mot overbelastning ---
latest_live_frame = b""
latest_frame_timestamp = 0.0
latest_frame_lock = RLock()
