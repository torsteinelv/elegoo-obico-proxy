import os
import sys
import time
import json
import random
import secrets
import logging
import asyncio
import threading
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import paho.mqtt.client as mqtt
import uvicorn

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("ElegooObicoProxy")

# Fetch Environment Variables
PRINTER_IP = os.getenv("PRINTER_IP", "192.168.1.100")
SERIAL_NUMBER = os.getenv("SERIAL_NUMBER", "CC2ABCD123456789")
ACCESS_CODE = os.getenv("ACCESS_CODE", "123456")

app = FastAPI(title="Elegoo CC2 to Moonraker Proxy")

# Global printer state (cache)
elegoo_status_cache = {}
active_websocket_clients = set()
mqtt_client_connected = False

# Generate valid IDs matching Elegoo's official web interface format
timestamp_hex = format(int(time.time() * 1000), "x")[-5:]
random_hex = format(secrets.randbelow(4096), "x")
CLIENT_ID = f"0cli{timestamp_hex}{random_hex}"[:10]

uuid_part = "".join(format(secrets.randbelow(16) if c == "x" else (secrets.randbelow(4) + 8), "x") for c in "xxxxxxxxxxxxxxxx")
timestamp_hex_long = format(int(time.time() * 1000), "x")
REQUEST_ID = f"{uuid_part}{timestamp_hex_long}"

logger.info(f"Generated client identifiers: CLIENT_ID={CLIENT_ID}, REQUEST_ID={REQUEST_ID}")

def deep_merge(base: dict, update: dict):
    """Recursive merge of delta status updates."""
    for key, value in update.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            deep_merge(base[key], value)
        else:
            base[key] = value

def map_to_moonraker_format():
    """Translates Elegoo's internal status cache to Moonraker's Klipper format."""
    ext_temp = elegoo_status_cache.get("extruder", {}).get("temperature", 0.0)
    ext_target = elegoo_status_cache.get("extruder", {}).get("target", 0.0)
    bed_temp = elegoo_status_cache.get("heater_bed", {}).get("temperature", 0.0)
    bed_target = elegoo_status_cache.get("heater_bed", {}).get("target", 0.0)
    
    progress_raw = elegoo_status_cache.get("machine_status", {}).get("progress", 0)
    progress = float(progress_raw) / 100.0

    el_status = elegoo_status_cache.get("machine_status", {}).get("status", 1)
    el_sub_status = elegoo_status_cache.get("machine_status", {}).get("sub_status", 0)
    
    klipper_state = "ready"
    if el_status == 2:
        if el_sub_status in [2502, 2505]:
            klipper_state = "paused"
        elif el_sub_status == 2077:
            klipper_state = "complete"
        elif el_sub_status == 2504:
            klipper_state = "cancelled"
        else:
            klipper_state = "printing"
    elif el_status == 1:
        klipper_state = "standby"

    filename = elegoo_status_cache.get("print_status", {}).get("filename", "")
    print_duration = elegoo_status_cache.get("print_status", {}).get("print_duration", 0)
    remaining = elegoo_status_cache.get("print_status", {}).get("remaining_time_sec", 0)

    return {
        "toolhead": {
            "homed_axes": elegoo_status_cache.get("toolhead", {}).get("homed_axes", "xyz"),
            "position": [
                elegoo_status_cache.get("gcode_move_inf", {}).get("x", 0.0),
                elegoo_status_cache.get("gcode_move_inf", {}).get("y", 0.0),
                elegoo_status_cache.get("gcode_move_inf", {}).get("z", 0.0),
                elegoo_status_cache.get("gcode_move_inf", {}).get("e", 0.0)
            ]
        },
        "extruder": {
            "temperature": ext_temp,
            "target": ext_target
        },
        "heater_bed": {
            "temperature": bed_temp,
            "target": bed_target
        },
        "print_stats": {
            "filename": filename,
            "total_duration": print_duration + remaining,
            "print_duration": print_duration,
            "progress": progress,
            "state": klipper_state,
            "message": ""
        },
        "display_status": {
            "progress": progress
        },
        # --- NYE TING OBICO FORVENTER ---
        "heaters": {
            "available_heaters": ["extruder", "heater_bed"],
            "available_sensors": ["extruder", "heater_bed"]
        },
        "virtual_sdcard": {
            "progress": progress,
            "is_active": klipper_state == "printing",
            "file_position": 0
        },
        "webhooks": {
            "state": "ready",
            "state_message": "Printer is ready"
        }
    }

async def broadcast_status_to_websockets():
    """Broadcasting updated Moonraker status to all open WebSockets for Obico."""
    if not active_websocket_clients:
        return
    
    status_data = map_to_moonraker_format()
    notification = {
        "jsonrpc": "2.0",
        "method": "notify_status_update",
        "params": [status_data]
    }
    
    payload = json.dumps(notification)
    for ws in list(active_websocket_clients):
        try:
            await ws.send_text(payload)
        except Exception:
            active_websocket_clients.remove(ws)

# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    global mqtt_client_connected
    if rc == 0:
        logger.info("Connected to printer's MQTT broker via TCP.")
        mqtt_client_connected = True
        
        client.subscribe(f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_response")
        client.subscribe(f"elegoo/{SERIAL_NUMBER}/api_status")
        client.subscribe(f"elegoo/{SERIAL_NUMBER}/{REQUEST_ID}/register_response")
        
        reg_payload = {"client_id": CLIENT_ID, "request_id": REQUEST_ID}
        client.publish(f"elegoo/{SERIAL_NUMBER}/api_register", json.dumps(reg_payload))
        logger.info("Registration request sent to the printer.")
    else:
        logger.error(f"Could not connect to printer's MQTT. Status code: {rc}")

def on_message(client, userdata, msg):
    global elegoo_status_cache
    topic = msg.topic
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
    except Exception:
        return

    if "register_response" in topic:
        if payload.get("error") == "ok":
            logger.info("Registration approved by printer! Requesting full status report...")
            status_req = {"id": 100, "method": 102, "params": {}}
            client.publish(f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request", json.dumps(status_req))
        else:
            logger.error(f"Printer rejected registration: {payload.get('error')}")

    elif "api_status" in topic or "api_response" in topic:
        result_data = payload.get("result", {})
        if not result_data:
            return
            
        if payload.get("method") == 1002 or not elegoo_status_cache:
            elegoo_status_cache = result_data
            logger.info("Full status report received and cached.")
        else:
            deep_merge(elegoo_status_cache, result_data)
        
        asyncio.run_coroutine_threadsafe(broadcast_status_to_websockets(), fastapi_loop)

def mqtt_heartbeat_loop(mqtt_client):
    while True:
        if mqtt_client_connected:
            try:
                ping_payload = {"type": "PING"}
                mqtt_client.publish(f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request", json.dumps(ping_payload))
            except Exception as e:
                logger.warning(f"Error sending heartbeat: {e}")
        time.sleep(10)

mqtt_client = mqtt.Client(client_id=CLIENT_ID)
mqtt_client.username_pw_set("elegoo", ACCESS_CODE)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# HTTP Endpoints (Moonraker emulation)

@app.get("/access/api_key")
async def get_api_key():
    """Dummy endpoint to satisfy Obico's API key check."""
    return {"result": "elegoo-obico-proxy-dummy-key"}

@app.get("/server/info")
async def get_server_info():
    """Tells Obico that Moonraker and Klipper are fully connected and ready."""
    return {
        "result": {
            "state": "ready",
            "klippy_state": "ready",
            "klippy_connected": True,
            "components": ["machine", "file_manager", "metadata"],
            "failed_components": [],
            "moonraker_version": "v0.12.0-proxy"
        }
    }

@app.get("/printer/info")
async def get_printer_info():
    """Returns Klipper status info."""
    return {
        "result": {
            "state": map_to_moonraker_format()["print_stats"]["state"],
            "state_message": "Printer is ready",
            "hostname": "elegoo-cc2",
            "software_version": "v0.12.0-proxy",
            "cpu_info": "Allwinner R528 Proxy"
        }
    }

@app.get("/printer/objects/list")
async def list_printer_objects():
    """Tells Obico which Klipper modules are available."""
    return {
        "result": {
            "objects": [
                "toolhead",
                "extruder",
                "heater_bed",
                "print_stats",
                "display_status",
                "heaters",
                "virtual_sdcard",
                "webhooks"
            ]
        }
    }

@app.get("/printer/objects/query")
async def query_printer_objects():
    """Returns the full status object requested by Obico."""
    return {
        "result": {
            "status": map_to_moonraker_format()
        }
    }

# Control Endpoints for Obico
@app.post("/printer/print/pause")
async def print_pause():
    logger.info("Received PAUSE command from Obico. Forwarding to printer...")
    cmd = {"id": random.randint(1000, 9999), "method": 1021, "params": {}}
    mqtt_client.publish(f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request", json.dumps(cmd))
    return {"result": "ok"}

@app.post("/printer/print/resume")
async def print_resume():
    logger.info("Received RESUME command from Obico. Forwarding to printer...")
    cmd = {"id": random.randint(1000, 9999), "method": 1023, "params": {}}
    mqtt_client.publish(f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request", json.dumps(cmd))
    return {"result": "ok"}

@app.post("/printer/print/cancel")
async def print_cancel():
    logger.info("Received CANCEL/STOP command from Obico. Canceling print...")
    cmd = {"id": random.randint(1000, 9999), "method": 1022, "params": {}}
    mqtt_client.publish(f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request", json.dumps(cmd))
    return {"result": "ok"}

# WebSocket Endpoint for Obico RPC communication
@app.websocket("/websocket")
@app.websocket("/server/websocket")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_websocket_clients.add(websocket)
    logger.info("Obico client connected to proxy WebSocket.")
    
    initial_status = map_to_moonraker_format()
    await websocket.send_text(json.dumps({
        "jsonrpc": "2.0",
        "result": {"status": initial_status},
        "id": 1
    }))

    try:
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                msg_id = msg.get("id")
                method = msg.get("method")
                
                if method in ["printer.objects.subscribe", "printer.objects.query"]:
                    await websocket.send_text(json.dumps({
                        "jsonrpc": "2.0",
                        "result": {"status": map_to_moonraker_format()},
                        "id": msg_id
                    }))
                elif method == "server.info":
                    await websocket.send_text(json.dumps({
                        "jsonrpc": "2.0",
                        "result": {
                            "state": "ready",
                            "klippy_state": "ready",
                            "klippy_connected": True
                        },
                        "id": msg_id
                    }))
                elif method == "printer.info":
                    await websocket.send_text(json.dumps({
                        "jsonrpc": "2.0",
                        "result": {
                            "state": map_to_moonraker_format()["print_stats"]["state"]
                        },
                        "id": msg_id
                    }))
            except Exception:
                pass
    except WebSocketDisconnect:
        logger.info("Obico client disconnected from proxy WebSocket.")
    finally:
        active_websocket_clients.remove(websocket)

if __name__ == "__main__":
    fastapi_loop = asyncio.get_event_loop()
    
    logger.info(f"Connecting to printer at IP: {PRINTER_IP}...")
    try:
        mqtt_client.connect(PRINTER_IP, 1883, 60)
        mqtt_thread = threading.Thread(target=mqtt_client.loop_forever, daemon=True)
        mqtt_thread.start()
        
        heartbeat_thread = threading.Thread(target=mqtt_heartbeat_loop, args=(mqtt_client,), daemon=True)
        heartbeat_thread.start()
    except Exception as e:
        logger.error(f"Critical error: Could not start MQTT connection: {e}")
        sys.exit(1)

    logger.info("Starting Moonraker emulation on port 7125...")
    uvicorn.run(app, host="0.0.0.0", port=7125)
