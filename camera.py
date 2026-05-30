"""Kamera-bakgrunnstråd og HTTP-endepunkter for video.

Proxyen cacher MJPEG-strøm frå printeren i RAM og serverer bileta
via HTTP-endepunkt som moonraker-obico kan polla. Dette betyr at
moonraker-obico aldri stressar printeren direkte.
"""
import time
import threading
import aiohttp
import asyncio
from fastapi import HTTPException
from fastapi.responses import Response
import state


# --- Bakgrunnstråd som cacher MJPEG-strøm ---

def webcam_stream_cache_worker():
    """Holder stabil tilkopling til printeren, les streamen og lagra siste bilete i RAM.

    Brukar aiohttp i ei bakgrunnstråd via asyncio-loop for å unngå blocking I/O.
    Reconnectar automatisk dersom straumen fell saman.
    """
    url = f"http://{state.PRINTER_IP}:8080/?action=stream"

    def _run_async():
        logger = state.logger
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_cache_worker_loop(loop, url))
        except Exception:
            logger.exception("Bakgrunnstråd feila")

    async def _cache_worker_loop(loop, url):
        """Async-loop som held éi tilkopling og parser MJPEG-frames."""
        logger = state.logger
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    logger.info("Koblar til Elegoo MJPEG-strømmen i bakgrunnen...")
                    timeout = aiohttp.ClientTimeout(total=60, connect=10)
                    async with session.get(url, headers={'User-Agent': 'Mozilla/5.0'},
                                           timeout=timeout) as resp:
                        logger.info("MJPEG-strøm tilkopla, les frames...")
                        buffer = b""
                        frame_count = 0
                        last_frame_time = 0.0
                        chunk_count = 0
                        while True:
                            try:
                                chunk = await resp.content.readany()
                            except Exception:
                                break
                            if not chunk:
                                logger.info(
                                    f"MJPEG-strøm avslutta (EOF) etter {frame_count} frames,"
                                    f" {chunk_count} chunks, buffer_size={len(buffer)}"
                                )
                                break
                            chunk_count += 1
                            buffer += chunk

                            # Trim buffer: fjern alt før første SOI-markør for å unngå
                            # ubegrensa buffervekst dersom straumen sender søppel først.
                            first_soi = buffer.find(b"\xff\xd8")
                            if first_soi == -1:
                                buffer = buffer[-1:] if buffer.endswith(b"\xff") else b""
                            elif first_soi > 0:
                                buffer = buffer[first_soi:]

                            while True:
                                start = buffer.find(b"\xff\xd8")
                                if start == -1:
                                    break
                                end = buffer.find(b"\xff\xd9", start)
                                if end == -1:
                                    break
                                frame = buffer[start:end + 2]
                                buffer = buffer[end + 2:]
                                frame_size = len(frame)

                                with state.latest_frame_lock:
                                    state.latest_live_frame = frame
                                    state.latest_frame_timestamp = time.time()
                                    now = time.time()
                                frame_count += 1

                                # Logg første frame og kvart 5. frame
                                if frame_count == 1 or frame_count % 5 == 0:
                                    logger.info(
                                        f"Frame #{frame_count}: size={frame_size},"
                                        f" buffer={len(buffer)}, chunks={chunk_count}"
                                    )
                                    last_frame_time = now

                        logger.info(f"Strøm braut av etter {frame_count} frames. Prøver på nytt...")
                        await asyncio.sleep(3)
                except asyncio.CancelledError:
                    logger.info("Cache worker avbroten")
                    break
                except aiohttp.ClientError as e:
                    logger.error(f"MJPEG tilkopling feila: {e}. Prøver på nytt om 3 sekund...")
                    await asyncio.sleep(3)
                except Exception:
                    logger.exception("Uventa feil i cache worker")
                    await asyncio.sleep(3)

    # Start async worker i eigen tråd med eigen event loop
    worker_thread = threading.Thread(target=_run_async, daemon=True, name="mjpeg-cache")
    worker_thread.start()


# --- HTTP-endepunkt ---
def register_camera_routes(app):
    """Registrer kamera-relaterte HTTP-endepunkt på FastAPI-appen."""

    @app.get("/webcam")
    async def get_webcam():
        """OctoPrint-standard webcam config endpoint.

        moonraker-obico kallar GET /webcam for å oppdage webkamera.
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
        """Serverer det ferskeste bildet frå cache."""
        with state.latest_frame_lock:
            frame = state.latest_live_frame

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
        raise HTTPException(
            status_code=503,
            detail="Kamera-cache er ikkje klar ennå. Vent litt og prøv på nytt."
        )

    @app.get("/camera/stream")
    async def camera_stream():
        """Serverer det ferskeste bildet frå cache.

        moonraker-obico pollar /camera/snapshot kontinuerleg for WebRTC-streaming.
        /camera/stream blir òg vist i frontend der ein treng bilde.
        Begge endepunkta les frå same RAM-cache — ingen nye tilkopningar til
        printeren frå klientar.
        """
        with state.latest_frame_lock:
            frame = state.latest_live_frame
            ts = state.latest_frame_timestamp

        if frame:
            age = time.time() - ts
            return Response(
                content=frame,
                media_type="image/jpeg",
                headers={
                    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                    "Pragma": "no-cache",
                    "Expires": "0",
                    "X-Frame-Age": f"{age:.1f}"
                }
            )
        raise HTTPException(
            status_code=503,
            detail="Kamera-cache er ikkje klar ennå. Vent litt og prøv på nytt."
        )
