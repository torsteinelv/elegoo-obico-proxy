"""Kamera-bakgrunnstråd og HTTP-endepunkter for video."""
import time
import threading
import aiohttp
import asyncio
from fastapi import HTTPException
from fastapi.responses import Response, StreamingResponse
import state


# --- Bakgrunnstråd som cacher MJPEG-strøm ---
def webcam_stream_cache_worker():
    """Holder stabil tilkobling til printeren, leser streamen og lagrer siste bilde i RAM.

    Bruker aiohttp i en bakgrunnstråd via asyncio loop for å unngå blocking I/O.
    """
    url = f"http://{state.PRINTER_IP}:8080/?action=stream"

    def _run_async():
        logger = state.logger
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_cache_worker_loop(loop, url))
        except Exception:
            logger.exception("Bakgrunnstråd feilet")

    async def _cache_worker_loop(loop, url):
        """Async loop som holder én tilkobling og parser MJPEG frames."""
        logger = state.logger
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    logger.info("Kobler til Elegoo MJPEG-strømmen i bakgrunnen...")
                    timeout = aiohttp.ClientTimeout(total=60, connect=10)
                    async with session.get(url, headers={'User-Agent': 'Mozilla/5.0'},
                                           timeout=timeout) as resp:
                        logger.info("MJPEG-strøm tilkoblet, leser frames...")
                        buffer = b""
                        frame_count = 0
                        async for chunk in resp.content.iter_chunked(8192):
                            buffer += chunk
                            while True:
                                start = buffer.find(b"\xff\xd8")
                                if start == -1:
                                    break
                                end = buffer.find(b"\xff\xd9", start)
                                if end == -1:
                                    break
                                frame = buffer[start:end + 2]
                                buffer = buffer[end + 2:]

                                with state.latest_frame_lock:
                                    state.latest_live_frame = frame
                                    state.latest_frame_timestamp = time.time()
                                frame_count += 1

                        logger.info(f"Strøm brøt av etter {frame_count} frames. Prøver å koble på nytt...")
                        await asyncio.sleep(3)
                except asyncio.CancelledError:
                    logger.info("Cache worker avbrutt")
                    break
                except aiohttp.ClientError as e:
                    logger.error(f"MJPEG tilkobling feilet: {e}. Prøver på nytt om 3 sekunder...")
                    await asyncio.sleep(3)
                except Exception:
                    logger.exception("Uventet feil i cache worker")
                    await asyncio.sleep(3)

    # Start async worker i egen tråd med egen event loop
    worker_thread = threading.Thread(target=_run_async, daemon=True, name="mjpeg-cache")
    worker_thread.start()


# --- HTTP-endepunkter ---
def register_camera_routes(app):
    """Registrer kamera-relaterte HTTP-endepunkter på FastAPI-appen."""

    @app.get("/webcam")
    async def get_webcam():
        """OctoPrint-standard webcam config endpoint.

        moonraker-obico kaller GET /webcam for å oppdage webkameraer.
        """
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
    async def get_webcam_by_name(name: str = None):
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
        with state.latest_frame_lock:
            frame = state.latest_live_frame

        if frame:
            return Response(
                content=frame,
                media_type="image/jpeg",
                headers={
                    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                    "Pragma": "no-cache",
                    "Expires": 0
                }
            )
        raise HTTPException(status_code=502, detail="Kamera-cache er ikke klar ennå")

    @app.get("/camera/stream")
    async def camera_stream():
        """Proxyer live MJPEG-strømmen async via aiohttp med riktig boundary-format.

        moonraker-obico leser streamen line-by-line (readline) og forventer
        multipart/x-mixed-replace med --boundary-linjer mellom hver JPEG-frame.
        """
        url = f"http://{state.PRINTER_IP}:8080/?action=stream"
        headers = {'User-Agent': 'Mozilla/5.0'}
        boundary = b"--boundarydonotcross\r\n"

        async def stream_generator():
            session = aiohttp.ClientSession()
            try:
                timeout = aiohttp.ClientTimeout(total=60, connect=10)
                async with session.get(url, headers=headers, timeout=timeout) as resp:
                    buffer = b""
                    async for chunk in resp.content.iter_chunked(8192):
                        buffer += chunk
                        while True:
                            start = buffer.find(b"\xff\xd8")
                            if start == -1:
                                break
                            end = buffer.find(b"\xff\xd9", start)
                            if end == -1:
                                break
                            frame = buffer[start:end + 2]
                            buffer = buffer[end + 2:]
                            yield boundary
                            yield frame
                            if not frame.endswith(b"\r\n"):
                                yield b"\r\n"
            except asyncio.CancelledError:
                state.logger.info("Klient disconnectet fra video-strøm.")
            except Exception as e:
                state.logger.error(f"Strømavbrudd under overføring: {e}")
            finally:
                await session.close()

        return StreamingResponse(
            stream_generator(),
            media_type="multipart/x-mixed-replace; boundary=boundarydonotcross"
        )
