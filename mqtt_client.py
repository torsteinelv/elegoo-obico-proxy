"""MQTT-kommunikasjon mot Elegoo-printer."""
import json
import time
import threading
import asyncio
import paho.mqtt.client as mqtt
from state import (
    PRINTER_IP, SERIAL_NUMBER, ACCESS_CODE,
    CLIENT_ID, REQUEST_ID,
    mqtt_client_connected, fastapi_loop,
    elegoo_status_cache, elegoo_status_lock,
    logger
)


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
        "extruder": {"temperature": ext_temp, "target": ext_target},
        "heater_bed": {"temperature": bed_temp, "target": bed_target},
        "print_stats": {
            "filename": filename,
            "total_duration": print_duration + remaining,
            "print_duration": print_duration,
            "progress": progress,
            "state": klipper_state,
            "message": ""
        },
        "display_status": {"progress": progress},
        "heaters": {
            "available_heaters": ["extruder", "heater_bed"],
            "available_sensors": ["extruder", "heater_bed"]
        },
        "virtual_sdcard": {
            "progress": progress,
            "is_active": klipper_state == "printing",
            "file_position": 0
        },
        "webhooks": {"state": "ready", "state_message": "Printer is ready"},
        "gcode_move": {
            "speed_factor": 1.0,
            "extrude_factor": 1.0,
            "gcode_position": gcode_position
        },
        "fan": {"speed": 0.0}
    }


async def broadcast_status_to_websockets():
    """Broadcasting updated Moonraker status to all open WebSockets."""
    from state import active_websocket_clients, active_ws_lock
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


def create_mqtt_client():
    """Create and configure the MQTT client."""
    client = mqtt.Client(client_id=CLIENT_ID)
    client.username_pw_set("elegoo", ACCESS_CODE)
    client.on_connect = on_connect
    client.on_message = on_message
    return client
