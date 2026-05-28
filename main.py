import os
import sys
import time
import json
import random
import secrets
import logging
import asyncio
import threading
from contextlib import asynccontextmanager
from threading import RLock
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.responses import Response, StreamingResponse
import urllib.request
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
elegoo_status_lock = RLock()
active_websocket_clients = set()
active_ws_lock = RLock()
mqtt_client_connected = False
fastapi_loop = None

# Global kamera-cache (RAM) for å beskytte printeren mot overbelastning
latest_live_frame = b""
latest_frame_lock = RLock()

# Generate valid IDs matching Elegoo's official web interface format
timestamp_hex = format(int(time.time() * 1000), "x")[-5:]
random_hex = secrets.token_hex(3)
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
    
    # Robust progress parsing
    progress_raw = elegoo_status_cache.get("machine_status", {}).get("progress", 0)
    if not progress_raw:
        progress_raw = elegoo_status_cache.get("print_status", {}).get("progress", 0)
    
    try:
        progress_val = float(progress_raw)
        progress = progress_val / 100.0 if progress_val > 1.0 else progress_val
    except Exception:
        progress = 0.0

    el_status = elegoo_status_cache.get("machine_status", {}).get("status", 1)
    el_sub_status = elegoo_status_cache.get("machine_status", {}).get("sub_status", 0)

    klipper_state = "standby"
    if el_status == 2:
        if el_sub_status in [2501, 2502, 2503, 2505]:
            klipper_state = "paused"
        elif el_sub_status == 2077:
            klipper_state = "complete"
        elif el_sub_status == 2504:
            klipper_state = "cancelled"
        elif el_sub_status == 2401:
            klipper_state = "printing"
        else:
            klipper_state = "printing"
    elif el_status == 3:
        klipper_state = "printing"
    elif el_status == 4:
        klipper_state = "printing"

    # Robust filename parsing
    filename = elegoo_status_cache.get("print_status", {}).get("filename", "")
    if not filename:
        filename = elegoo_status_cache.get("machine_status", {}).get("filename", "")

    print_duration = elegoo_status_cache.get("print_status", {}).get("print_duration", 0)
    remaining = elegoo_status_cache.get("print_status", {}).get("remaining_time_sec", 0)

    gm = elegoo_status_cache.get("gcode_move_inf", {})
    gcode_position = gm.get("gcode_position")
    if not gcode_position:
        gcode_position = [gm.get("x", 0.0), gm.get("y", 0.0), gm.get("z", 0.0), gm.get("e", 0.0)]

    return {
        "toolhead": {
            "homed_axes": elegoo_status_cache.get("toolhead", {}).get("homed_axes", "xyz"),
            "position": gcode_position
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
        },
        "gcode_move": {
            "speed_factor": 1.0,
            "extrude_factor": 1.0,
            "gcode_position": gcode_position
        },
        "fan": {
            "speed": 0.0
        }
    }

async def broadcast_status_to_websockets():
    """Broadcasting updated Moonraker status with valid eventtime to all open WebSockets."""
    with active_ws_lock:
        clients = list(active_websocket_clients)
    if not clients:
        return

    with elegoo_status_lock:
        status_data = map_to_moonraker_format()
    notification = {
        "jsonrpc": "2.0",
        "method": "notify_status_update",
        "params": [status_data]
    }

    payload = json.dumps(notification)
    for ws in list(clients):
        try:
            await ws.send_text(payload)
        except Exception:
            with active_ws_lock:
                active_websocket_clients.discard(ws)

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
            with elegoo_status_lock:
                elegoo_status_cache = result_data
            logger.info("Full status report received and cached.")
        else:
            with elegoo_status_lock:
                deep_merge(elegoo_status_cache, result_data)

        if fastapi_loop:
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

# --- KAMERA BAKGRUNNSTRÅD (STREAM CACHE WORKER) ---
def webcam_stream_cache_worker():
    """Holder én stabil tilkobling til printeren, leser streamen og lagrer siste bilde i RAM."""
    global latest_live_frame
    url = f"http://{PRINTER_IP}:8080/?action=stream"
    
    while True:
        try:
            logger.info("Kobler til Elegoo MJPEG-strømmen i bakgrunnen...")
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Connection': 'keep-alive'})
            with urllib.request.urlopen(req, timeout=15) as response:
                buffer = b""
                while True:
                    chunk = response.read(4096)
                    if not chunk:
                        break
                    buffer += chunk
                    
                    # Let etter JPEG start (0xffd8) og slutt (0xffd9) i bufferen
                    while True:
                        start = buffer.find(b"\xff\xd8")
                        if start == -1:
                            break
                        end = buffer.find(b"\xff\xd9", start)
                        if end == -1:
                            break
                        
                        # Vi fant et komplett bilde! Spara det i RAM.
                        frame = buffer[start:end+2]
                        buffer = buffer[end+2:]
                        
                        with latest_frame_lock:
                            latest_live_frame = frame
        except Exception as e:
            logger.error(f"Kameratilkobling feilet i bakgrunnen: {e}. Prøver på nytt om 3 sekunder...")
            time.sleep(3)

# HTTP Endpoints (Moonraker emulation)

@app.get("/access/api_key")
async def get_api_key():
    return {"result": "elegoo-obico-proxy-dummy-key"}

@app.get("/server/info")
async def get_server_info():
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
    with elegoo_status_lock:
        state = map_to_moonraker_format()["print_stats"]["state"]
    return {
        "result": {
            "state": state,
            "state_message": "Printer is ready",
            "hostname": "elegoo-cc2",
            "software_version": "v0.12.0-proxy",
            "cpu_info": "Allwinner R528 Proxy",
            "klipper_path": "/home/pi/klipper",
            "python_path": "/home/pi/klippy-env/bin/python",
            "log_file": "/tmp/klippy.log",
            "config_file": "/home/pi/klipper_config/printer.cfg"
        }
    }

@app.get("/printer/objects/list")
async def list_printer_objects():
    return {
        "result": {
            "objects": [
                "toolhead", "extruder", "heater_bed", "print_stats", 
                "display_status", "heaters", "virtual_sdcard", "webhooks", 
                "gcode_move", "fan", 
                "gcode_macro _OBICO_LAYER_CHANGE", 
                "gcode_macro TIMELAPSE_TAKE_FRAME"
            ]
        }
    }

@app.get("/printer/objects/query")
async def query_printer_objects():
    with elegoo_status_lock:
        status = map_to_moonraker_format()
    return {"result": {"status": status}}

# --- WEBCAM ENDPOINTS (MJPEG Adaptive tvinger direktemodus) ---
@app.get("/server/webcams/list")
async def list_webcams():
    return {
        "result": {
            "webcams": [{
                "name": "Elegoo Camera",
                "service": "mjpeg_adaptive",
                "target_fps": 15,
                "stream_url": "http://127.0.0.1:7125/camera/stream",
                "snapshot_url": "http://127.0.0.1:7125/camera/snapshot",
                "flip_horizontal": False,
                "flip_vertical": False,
                "rotation": 0
            }]
        }
    }

@app.get("/server/webcams/get")
async def get_webcam(name: str = None):
    return {
        "result": {
            "name": name or "Elegoo Camera",
            "service": "mjpeg_adaptive",
            "target_fps": 15,
            "stream_url": "http://127.0.0.1:7125/camera/stream",
            "snapshot_url": "http://127.0.0.1:7125/camera/snapshot",
            "flip_horizontal": False,
            "flip_vertical": False,
            "rotation": 0
        }
    }

@app.get("/camera/snapshot")
async def camera_snapshot():
    """Serverer det ferskeste bildet superraskt direkte fra RAM-cache uten å røre printeren."""
    with latest_frame_lock:
        frame = latest_live_frame
        
    if frame:
        return Response(
            content=frame, 
            media_type="image/jpeg",
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )
    raise HTTPException(status_code=502, detail="Kamera-cache er ikke klar ennå")

@app.get("/camera/stream")
async def camera_stream():
    """Tunnelerer live MJPEG-videostrømmen direkte ved behov."""
    url = f"http://{PRINTER_IP}:8080/?action=stream"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        response = urllib.request.urlopen(req, timeout=10)
        content_type = response.headers.get("Content-Type", "multipart/x-mixed-replace; boundary=boundarydonotcross")
        
        def iter_stream():
            try:
                while True:
                    chunk = response.read(4096)
                    if not chunk:
                        break
                    yield chunk
            except Exception as e:
                logger.error(f"Strømavbrudd under overføring: {e}")
            finally:
                response.close()
                
        return StreamingResponse(iter_stream(), media_type=content_type)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Klarte ikke å koble til live-strøm: {e}")

@app.get("/machine/device_power/devices")
async def get_device_power_devices():
    return {"result": []}

@app.get("/server/files/metadata")
async def get_metadata(filename: str = ""):
    return {
        "result": {
            "filename": filename,
            "size": 1000000,
            "modified": time.time(),
            "thumbnails": []
        }
    }

@app.get("/server/files/list")
async def get_files_list():
    return {"result": []}

# --- DATABASE ROUTE ---
@app.get("/server/database/item")
async def get_database_item(namespace: str = None, key: str = None):
    return {"result": {"namespace": namespace or "", "key": key or "", "value": {}}}

@app.post("/server/database/item")
async def post_database_item(request: Request):
    return {"result": {"namespace": "obico", "key": "printer_id", "value": 1}}

# --- G-KODE MOTOR ---
@app.get("/printer/gcode/script")
@app.post("/printer/gcode/script")
async def execute_gcode(request: Request, script: str = None):
    if not script:
        try:
            body = await request.json()
            script = body.get("script", "")
        except: pass
    if script:
        cmd = {"id": random.randint(1000, 9999), "method": 1008, "params": {"command": script}}
        mqtt_client.publish(f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request", json.dumps(cmd))
    return {"result": "ok"}

@app.post("/printer/print/pause")
async def print_pause():
    cmd = {"id": random.randint(1000, 9999), "method": 1021, "params": {}}
    mqtt_client.publish(f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request", json.dumps(cmd))
    return {"result": "ok"}

@app.post("/printer/print/resume")
async def print_resume():
    cmd = {"id": random.randint(1000, 9999), "method": 1023, "params": {}}
    mqtt_client.publish(f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request", json.dumps(cmd))
    return {"result": "ok"}

@app.post("/printer/print/cancel")
async def print_cancel():
    cmd = {"id": random.randint(1000, 9999), "method": 1022, "params": {}}
    mqtt_client.publish(f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request", json.dumps(cmd))
    return {"result": "ok"}

@app.get("/server/history/list")
async def get_history(limit: int = 1, order: str = "desc"):
    return {"result": {"jobs": []}}

@app.get("/server/history/stats")
async def get_history_stats():
    return {"result": {"total_jobs": 0, "longest_job": 0.0, "total_print_time": 0.0, "total_filament": 0.0}}

@app.get("/machine/update/status")
async def get_update_status(refresh: str = "false"):
    return {"result": {"version_info": {}}}

@app.get("/machine/config/info")
async def get_machine_config_info():
    return {"result": {}}

@app.get("/printer/state")
async def get_printer_state():
    return {"result": {"state": "ready", "message": "Printer is ready"}}

@app.get("/server/authorization/check")
async def check_auth():
    return {"result": {"authenticated": True}}

# --- PROXY DEBUG ENDPOINT ---
@app.get("/proxy/debug")
async def proxy_debug():
    with elegoo_status_lock:
        status_cache_copy = dict(elegoo_status_cache)
    with latest_frame_lock:
        frame_size = len(latest_live_frame)
    return {
        "status": "running",
        "mqtt_connected": mqtt_client_connected,
        "active_websocket_clients_count": len(active_websocket_clients),
        "latest_frame_size_bytes": frame_size,
        "elegoo_status_cache": status_cache_copy,
        "mapped_to_moonraker": map_to_moonraker_format()
    }

@app.websocket("/websocket")
@app.websocket("/server/websocket")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    with active_ws_lock:
        active_websocket_clients.add(websocket)
    logger.info("Obico client connected to proxy WebSocket.")
    
    with elegoo_status_lock:
        initial_status = map_to_moonraker_format()
    await websocket.send_text(json.dumps({"jsonrpc": "2.0", "result": {"status": initial_status}, "id": 1}))

    try:
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                msg_id = msg.get("id")
                method = msg.get("method")
                
                if method in ["printer.objects.subscribe", "printer.objects.query"]:
                    with elegoo_status_lock:
                        status_fmt = map_to_moonraker_format()
                    await websocket.send_text(json.dumps({"jsonrpc": "2.0", "result": {"status": status_fmt}, "id": msg_id}))
                elif method == "server.info":
                    await websocket.send_text(json.dumps({"jsonrpc": "2.0", "result": {"state": "ready", "klippy_state": "ready", "klippy_connected": True}, "id": msg_id}))
                elif method == "printer.info":
                    with elegoo_status_lock:
                        state = map_to_moonraker_format()["print_stats"]["state"]
                    await websocket.send_text(json.dumps({
                        "jsonrpc": "2.0",
                        "result": {
                            "state": state,
                            "state_message": "Printer is ready",
                            "hostname": "elegoo-cc2",
                            "software_version": "v0.12.0-proxy",
                            "cpu_info": "Allwinner R528 Proxy",
                            "klipper_path": "/home/pi/klipper",
                            "python_path": "/home/pi/klippy-env/bin/python",
                            "log_file": "/tmp/klippy.log",
                            "config_file": "/home/pi/klipper_config/printer.cfg"
                        },
                        "id": msg_id
                    }))
                elif method == "printer.gcode.script":
                    script = msg.get("params", {}).get("script", "")
                    if script:
                        cmd = {"id": random.randint(1000, 9999), "method": 1008, "params": {"command": script}}
                        mqtt_client.publish(f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request", json.dumps(cmd))
                    await websocket.send_text(json.dumps({"jsonrpc": "2.0", "result": "ok", "id": msg_id}))
                elif method in ["connection.register_remote_method", "server.connection.identify"]:
                    await websocket.send_text(json.dumps({"jsonrpc": "2.0", "result": None, "id": msg_id}))
            except Exception:
                logger.exception("Error handling WebSocket message")
    except WebSocketDisconnect:
        logger.info("Obico client disconnected from proxy WebSocket.")
    finally:
        with active_ws_lock:
            active_websocket_clients.discard(websocket)

if __name__ == "__main__":
    fastapi_loop = asyncio.get_event_loop()
    
    # 1. Start bakgrunnstråden som kontinuerlig cacher kameraet fra printeren
    cam_thread = threading.Thread(target=webcam_stream_cache_worker, daemon=True)
    cam_thread.start()
    
    # 2. Start MQTT-kommunikasjon mot printeren
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
