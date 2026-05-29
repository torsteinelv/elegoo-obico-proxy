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
            if state.mqtt_client is None:
                logger.warning("Cannot send gcode script: MQTT client not initialized yet")
                return {"result": "error", "message": "MQTT client not ready"}
            state.mqtt_client.publish(
                f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request",
                json.dumps(cmd),
            )
        return {"result": "ok"}

    @app.post("/printer/print/pause")
    async def print_pause():
        if state.mqtt_client is None:
            logger.warning("Cannot pause: MQTT client not initialized yet")
            return {"result": "error", "message": "MQTT client not ready"}
        cmd = {"id": random.randint(1000, 9999), "method": 1021, "params": {}}
        state.mqtt_client.publish(
            f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request", json.dumps(cmd)
        )
        return {"result": "ok"}

    @app.post("/printer/print/resume")
    async def print_resume():
        if state.mqtt_client is None:
            logger.warning("Cannot resume: MQTT client not initialized yet")
            return {"result": "error", "message": "MQTT client not ready"}
        cmd = {"id": random.randint(1000, 9999), "method": 1023, "params": {}}
        state.mqtt_client.publish(
            f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request", json.dumps(cmd)
        )
        return {"result": "ok"}

    @app.post("/printer/print/cancel")
    async def print_cancel():
        if state.mqtt_client is None:
            logger.warning("Cannot cancel: MQTT client not initialized yet")
            return {"result": "error", "message": "MQTT client not ready"}
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

    # --- OctoPrint-kompatible endepunkter (kalt av moonraker-obico client) ---

    @app.get("/api/v1/octo/")
    async def octo_root():
        return {"version": "proxy", "online": True}

    @app.get("/api/v1/octo/job")
    async def octo_job():
        with elegoo_status_lock:
            status = map_to_moonraker_format()
        print_stats = status.get("print_stats", {})
        state_val = print_stats.get("state", "offline")
        return {
            "job": {
                "state": state_val,
                "printTime": print_stats.get("print_duration", 0) or 0,
                "printTimeLeft": None,
                "printTimeRemaining": None,
                "file": {"name": print_stats.get("filename", "") or ""},
                "filamentTotal": print_stats.get("filament_used", 0) or 0,
                "filamentUsed": print_stats.get("filament_used", 0) or 0,
            },
            "printProgress": print_stats.get("progress", 0) or 0,
            "state": state_val,
            "error": "",
            "warnings": [],
            "closedOrError": state_val in ("shutdown", "error"),
        }

    @app.get("/api/v1/octo/print")
    async def octo_print():
        with elegoo_status_lock:
            status = map_to_moonraker_format()
        print_stats = status.get("print_stats", {})
        state_val = print_stats.get("state", "offline")
        return {"state": state_val, "error": "", "closedOrError": state_val in ("shutdown", "error")}

    @app.post("/api/v1/octo/print")
    async def octo_print_post(request: Request):
        if state.mqtt_client is None:
            logger.warning("Cannot handle print command: MQTT client not initialized yet")
            return {"state": "error", "message": "MQTT client not ready"}
        body = {}
        try:
            body = await request.json()
        except Exception:
            pass
        action = body.get("command", "")
        if action == "start":
            cmd = {"id": random.randint(1000, 9999), "method": 1023, "params": {}}
            state.mqtt_client.publish(f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request", json.dumps(cmd))
        elif action == "pause":
            pause_action = body.get("action", "")
            method = 1021 if pause_action == "pause" else (1023 if pause_action == "resume" else 1021)
            cmd = {"id": random.randint(1000, 9999), "method": method, "params": {}}
            state.mqtt_client.publish(f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request", json.dumps(cmd))
        elif action == "cancel":
            cmd = {"id": random.randint(1000, 9999), "method": 1022, "params": {}}
            state.mqtt_client.publish(f"elegoo/{SERIAL_NUMBER}/{CLIENT_ID}/api_request", json.dumps(cmd))
        return {"state": "ok"}

    @app.get("/api/v1/octo/g_code_files/")
    async def octo_g_code_files_get():
        return {"files": []}

    @app.post("/api/v1/octo/g_code_files/")
    async def octo_g_code_files_post(request: Request):
        try:
            body = await request.json()
            logger.debug(f"Received g_code_files update: {body}")
        except Exception:
            logger.warning("Received g_code_files/ post with invalid body")
        return {"saved": True}

    @app.get("/api/v1/octo/g_code_files/{filename:path}")
    async def octo_g_code_files_file(filename: str):
        return {"file": {"name": filename, "path": filename, "size": 0, "date": int(time.time())}}

    @app.post("/api/v1/octo/g_code_files/{filename:path}")
    async def octo_g_code_files_file_post(request: Request, filename: str):
        try:
            body = await request.json()
            logger.debug(f"Received g_code_files/{filename} update: {body}")
        except Exception:
            pass
        return {"saved": True}

    @app.delete("/api/v1/octo/g_code_files/{filename:path}")
    async def octo_g_code_files_file_delete(filename: str):
        logger.debug(f"Received g_code_files/{filename} delete request")
        return {"removed": True}

    @app.get("/api/v1/octo/pic/")
    async def octo_pic():
        with latest_frame_lock:
            frame_data = latest_live_frame
        if not frame_data:
            from fastapi.responses import JSONResponse
            return JSONResponse(status_code=404, content={"error": "No frame available"})
        import base64
        return {"pic": base64.b64encode(frame_data).decode("utf-8")}

    @app.get("/api/v1/octo/temperatures")
    async def octo_temperatures():
        with elegoo_status_lock:
            status = map_to_moonraker_format()
        extruder = status.get("extruder", {})
        heater_bed = status.get("heater_bed", {})
        return {
            "print": {"hotend_temp": extruder.get("temperature", 0) or 0},
            "bed": {"bed_temp": heater_bed.get("temperature", 0) or 0},
            "temps": extruder.get("target", 0) or 0,
            "bed_temps": heater_bed.get("target", 0) or 0,
            "state": "Ready",
        }

    @app.get("/api/v1/octo/files")
    async def octo_files():
        return {"files": []}

    @app.post("/api/v1/octo/files")
    async def octo_files_post(request: Request):
        return {"saved": True}

    @app.get("/api/v1/octo/files/{filename:path}")
    async def octo_file(filename: str):
        return {"file": {"name": filename, "path": filename, "size": 0, "date": int(time.time())}}

    @app.post("/api/v1/octo/files/{filename:path}")
    async def octo_file_post(request: Request, filename: str):
        return {"saved": True}

    @app.delete("/api/v1/octo/files/{filename:path}")
    async def octo_file_delete(filename: str):
        return {"removed": True}

    @app.get("/api/v1/octo/printer")
    async def octo_printer():
        return {"name": "Elegoo CC2", "type": "3dprinter"}

    @app.post("/api/v1/octo/printer")
    async def octo_printer_post(request: Request):
        return {"name": "Elegoo CC2"}

    @app.get("/api/v1/octo/printer/command")
    async def octo_printer_command():
        return {"command": "ok"}

    @app.post("/api/v1/octo/printer/command")
    async def octo_printer_command_post(request: Request):
        return {"command": "ok"}

    @app.get("/api/v1/octo/authorization/check")
    async def octo_auth_check():
        return {"authenticated": True}

    @app.get("/api/v1/octo/timelapse/")
    async def octo_timelapse():
        return {"result": "ok"}

    @app.post("/api/v1/octo/timelapse/")
    async def octo_timelapse_post():
        return {"result": "ok"}

    @app.get("/api/v1/octo/completion")
    async def octo_completion():
        return {"completion": {}}

    @app.post("/api/v1/octo/completion")
    async def octo_completion_post(request: Request):
        return {"result": "ok"}

    @app.get("/api/v1/octo/event")
    async def octo_event():
        return {"events": []}

    @app.post("/api/v1/octo/event")
    async def octo_event_post(request: Request):
        return {"result": "ok"}

    @app.get("/api/v1/octo/profiles/printer")
    async def octo_printer_profiles():
        return {"profiles": [{"name": "default", "vendor": "elegoo", "model": "cc2", "customProfile": False}]}

    @app.post("/api/v1/octo/profiles/printer")
    async def octo_printer_profiles_post(request: Request):
        return {"name": "default"}

    @app.get("/api/v1/octo/profiles/temperatures")
    async def octo_temp_profiles():
        return {"profiles": []}

    @app.get("/api/v1/octo/profiles/feeds")
    async def octo_feed_profiles():
        return {"profiles": []}

    @app.get("/api/v1/octo/profiles/timezone")
    async def octo_tz_profiles():
        return {"profiles": []}

    @app.get("/api/v1/octo/profiles/management")
    async def octo_mgmt_profiles():
        return {"profiles": []}

    @app.get("/api/v1/octo/logs")
    async def octo_logs():
        return {"logs": []}

    @app.get("/api/v1/octo/virtual_printer")
    async def octo_virtual_printer():
        return {"virtual_printer": False}

    @app.post("/api/v1/octo/virtual_printer")
    async def octo_virtual_printer_post(request: Request):
        return {"virtual_printer": False}

    # --- Slutt på OctoPrint-kompatible endepunkter ---

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
                            if state.mqtt_client is None:
                                await websocket.send_text(
                                    json.dumps({
                                        "jsonrpc": "2.0",
                                        "error": {"code": -32603, "message": "MQTT client not ready"},
                                        "id": msg_id,
                                    })
                                )
                                continue
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
