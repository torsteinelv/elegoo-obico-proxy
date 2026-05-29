"""Moonraker HTTP-ruter og WebSocket-håndtering."""
import json
import random
import time

import state
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from mqtt_client import map_to_moonraker_format, broadcast_status_to_websockets
from state import (
    CLIENT_ID,
    SERIAL_NUMBER,
    active_websocket_clients,
    active_ws_lock,
    elegoo_status_cache,
    elegoo_status_lock,
    latest_frame_lock,
    latest_live_frame,
    logger,
    mqtt_client_connected,
)


def register_routes(app: FastAPI):
    """Registrer Moonraker-kompatible HTTP-endepunkter og WebSocket på FastAPI-appen."""

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
                "moonraker_version": "v0.12.0-proxy",
            }
        }

    @app.get("/printer/info")
    async def get_printer_info():
        with elegoo_status_lock:
            printer_state = map_to_moonraker_format()["print_stats"]["state"]
        return {
            "result": {
                "state": printer_state,
                "state_message": "Printer is ready",
                "hostname": "elegoo-cc2",
                "software_version": "v0.12.0-proxy",
                "cpu_info": "Allwinner R528 Proxy",
                "klipper_path": "/home/pi/klipper",
                "python_path": "/home/pi/klippy-env/bin/python",
                "log_file": "/tmp/klippy.log",
                "config_file": "/home/pi/klipper_config/printer.cfg",
            }
        }

    @app.get("/printer/objects/list")
    async def list_printer_objects():
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
                    "webhooks",
                    "gcode_move",
                    "fan",
                    "gcode_macro _OBICO_LAYER_CHANGE",
                    "gcode_macro TIMELAPSE_TAKE_FRAME",
                ]
            }
        }

    @app.get("/printer/objects/query")
    async def query_printer_objects():
        with elegoo_status_lock:
            status = map_to_moonraker_format()
        return {"result": {"status": status}}

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
                "thumbnails": [],
            }
        }

    @app.get("/server/files/list")
    async def get_files_list():
        return {"result": []}

    @app.get("/server/database/item")
    async def get_database_item(namespace: str = None, key: str = None):
        return {
            "result": {"namespace": namespace or "", "key": key or "", "value": {}}
        }

    @app.post("/server/database/item")
    async def post_database_item(request: Request):
        return {
            "result": {"namespace": "obico", "key": "printer_id", "value": 1}
        }

    @app.get("/printer/gcode/script")
    @app.post("/printer/gcode/script")
    async def execute_gcode(request: Request, script: str = None):
        if not script:
            try:
                body = await request.json()
                script = body.get("script", "")
            except Exception:
                pass
        if script:
            cmd = {
                "id": random.randint(1000, 9999),
                "method": 1008,
                "params": {"command": script},
            }
            state.mqtt_client.publish(
                f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request",
                json.dumps(cmd),
            )
        return {"result": "ok"}

    @app.post("/printer/print/pause")
    async def print_pause():
        cmd = {"id": random.randint(1000, 9999), "method": 1021, "params": {}}
        state.mqtt_client.publish(
            f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request", json.dumps(cmd)
        )
        return {"result": "ok"}

    @app.post("/printer/print/resume")
    async def print_resume():
        cmd = {"id": random.randint(1000, 9999), "method": 1023, "params": {}}
        state.mqtt_client.publish(
            f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request", json.dumps(cmd)
        )
        return {"result": "ok"}

    @app.post("/printer/print/cancel")
    async def print_cancel():
        cmd = {"id": random.randint(1000, 9999), "method": 1022, "params": {}}
        state.mqtt_client.publish(
            f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request", json.dumps(cmd)
        )
        return {"result": "ok"}

    @app.get("/server/history/list")
    async def get_history(limit: int = 1, order: str = "desc"):
        return {"result": {"jobs": []}}

    @app.get("/server/history/stats")
    async def get_history_stats():
        return {
            "result": {
                "total_jobs": 0,
                "longest_job": 0.0,
                "total_print_time": 0.0,
                "total_filament": 0.0,
            }
        }

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
            "mapped_to_moonraker": map_to_moonraker_format(),
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
        await websocket.send_text(
            json.dumps({"jsonrpc": "2.0", "result": {"status": initial_status}, "id": 1})
        )

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
                        await websocket.send_text(
                            json.dumps(
                                {"jsonrpc": "2.0", "result": {"status": status_fmt}, "id": msg_id}
                            )
                        )
                    elif method == "server.info":
                        await websocket.send_text(
                            json.dumps(
                                {
                                    "jsonrpc": "2.0",
                                    "result": {
                                        "state": "ready",
                                        "klippy_state": "ready",
                                        "klippy_connected": True,
                                    },
                                    "id": msg_id,
                                }
                            )
                        )
                    elif method == "printer.info":
                        with elegoo_status_lock:
                            printer_state = map_to_moonraker_format()["print_stats"]["state"]
                        await websocket.send_text(
                            json.dumps(
                                {
                                    "jsonrpc": "2.0",
                                    "result": {
                                        "state": printer_state,
                                        "state_message": "Printer is ready",
                                        "hostname": "elegoo-cc2",
                                        "software_version": "v0.12.0-proxy",
                                        "cpu_info": "Allwinner R528 Proxy",
                                        "klipper_path": "/home/pi/klipper",
                                        "python_path": "/home/pi/klippy-env/bin/python",
                                        "log_file": "/tmp/klippy.log",
                                        "config_file": "/home/pi/klipper_config/printer.cfg",
                                    },
                                    "id": msg_id,
                                }
                            )
                        )
                    elif method == "printer.gcode.script":
                        script = msg.get("params", {}).get("script", "")
                        if script:
                            cmd = {
                                "id": random.randint(1000, 9999),
                                "method": 1008,
                                "params": {"command": script},
                            }
                            state.mqtt_client.publish(
                                f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request",
                                json.dumps(cmd),
                            )
                        await websocket.send_text(
                            json.dumps({"jsonrpc": "2.0", "result": "ok", "id": msg_id})
                        )
                    elif method in [
                        "connection.register_remote_method",
                        "server.connection.identify",
                    ]:
                        await websocket.send_text(
                            json.dumps({"jsonrpc": "2.0", "result": None, "id": msg_id})
                        )
                except Exception:
                    logger.exception("Error handling WebSocket message")
        except WebSocketDisconnect:
            logger.info("Obico client disconnected from proxy WebSocket.")
        finally:
            with active_ws_lock:
                active_websocket_clients.discard(websocket)
