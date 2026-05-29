"""Kamera-bakgrunnstråd og HTTP-endepunkter for video."""
import time
import threading
import urllib.request
import aiohttp
import asyncio
from fastapi import HTTPException
from fastapi.responses import Response, StreamingResponse
from state import (
    PRINTER_IP, latest_live_frame, latest_frame_timestamp,
    latest_frame_lock, logger
)


# --- Bakgrunnstråd som cacher MJPEG-strøm ---
def webcam_stream_cache_worker():
    """Holder én stabil tilkobling til printeren, leser streamen og lagrer siste bilde i RAM."""
    global latest_live_frame
    url = f"http://{PRINTER_IP}:8080/?action=stream"

    while True:
        try:
            logger.info("Kobler til Elegoo MJPEG-strømmen i bakgrunnen...")
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0',
                'Connection': 'keep-alive'
            })
            with urllib.request.urlopen(req, timeout=15) as response:
                buffer = b""
                while True:
                    chunk = response.read(4096)
                    if not chunk:
                        break
                    buffer += chunk

                    while True:
                        start = buffer.find(b"\xff\xd8")
                        if start == -1:
                            break
                        end = buffer.find(b"\xff\xd9", start)
                        if end == -1:
                            break

                        frame = buffer[start:end+2]
                        buffer = buffer[end+2:]

                        with latest_frame_lock:
                            latest_live_frame = frame
        except Exception as e:
            logger.error(f"Kameratilkobling feilet i bakgrunnen: {e}. Prøver på nytt om 3 sekunder...")
            time.sleep(3)


# --- HTTP-endepunkter ---
def register_camera_routes(app):
    """Registrer kamera-relaterte HTTP-endepunkter på FastAPI-appen."""

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
        """Serverer det ferskeste bildet fra cache."""
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
        """Proxyer live MJPEG-strømmen async via aiohttp med riktig boundary-format.

        moonraker-obico leser streamen line-by-line (readline) og forventer
        multipart/x-mixed-replace med --boundary-linjer mellom hver JPEG-frame.
        """
        url = f"http://{PRINTER_IP}:8080/?action=stream"
        headers = {'User-Agent': 'Mozilla/5.0'}
        boundary = b"--boundarydonotcross\r\n"

        async def stream_generator():
            session = aiohttp.ClientSession()
            try:
                async with session.get(url, headers=headers,
                                       timeout=aiohttp.ClientTimeout(total=60)) as resp:
                    buffer = b""
                    async for chunk in resp.content.iter_chunked(4096):
                        buffer += chunk
                        while True:
                            start = buffer.find(b"\xff\xd8")
                            if start == -1:
                                break
                            end = buffer.find(b"\xff\xd9", start)
                            if end == -1:
                                break
                            frame = buffer[start:end+2]
                            buffer = buffer[end+2:]
                            yield boundary
                            yield frame
                            if not frame.endswith(b"\r\n"):
                                yield b"\r\n"
            except asyncio.CancelledError:
                logger.info("Klient disconnectet fra video-strøm.")
            except Exception as e:
                logger.error(f"Strømavbrudd under overføring: {e}")
            finally:
                await session.close()

        return StreamingResponse(
            stream_generator(),
            media_type="multipart/x-mixed-replace; boundary=boundarydonotcross"
        )
