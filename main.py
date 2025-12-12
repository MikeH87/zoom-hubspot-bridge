from typing import Any, Dict, Optional, List
import hmac
import hashlib
import time
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse

from config import ZOOM_WEBHOOK_SECRET_TOKEN, MEDIA_PROXY_SECRET, PUBLIC_BASE_URL
from hubspot_client import (
    search_meeting_by_zoom_id,
    create_call_for_meeting,
    get_meeting_contact_ids,
    get_meeting_deal_ids,
    get_contact_name,
    associate_call_to_contacts,
    associate_call_to_deals,
)
from zoom_client import get_participant_count, get_meeting_recordings, stream_recording_bytes


app = FastAPI()


DISPOSITION_CONNECTED = "f240bbac-87c9-4f6e-bf70-924b57d47db7"
DISPOSITION_NO_ANSWER = "73a0d17f-1163-4015-bdd5-ec830791da20"

ALLOWED_MEDIA_FILE_TYPES = {"M4A", "MP4", "MP3", "WAV"}


def _extract_duration_ms(zoom_object: Dict[str, Any]) -> int:
    duration_minutes = zoom_object.get("duration")
    try:
        if duration_minutes is None:
            return 0
        minutes = float(duration_minutes)
        if minutes < 0:
            return 0
        return int(minutes * 60 * 1000)
    except Exception:
        return 0


def _choose_media_recording_file(recording_files: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Selection rules:
    - Only accept file_type in: M4A (preferred), MP4, MP3, WAV
    - Ignore transcript/JSON/TXT/VTT/CHAT etc by file_type filter
    - Prefer audio_only recordings first (and M4A if present)
    - Otherwise choose the smallest MP4 (by file_size if present)
    """
    if not recording_files:
        return None

    media = []
    for rf in recording_files:
        ft = (rf.get("file_type") or "").upper().strip()
        if ft in ALLOWED_MEDIA_FILE_TYPES:
            media.append(rf)

    if not media:
        return None

    audio_only = [rf for rf in media if (rf.get("recording_type") == "audio_only")]
    if audio_only:
        m4a = [rf for rf in audio_only if (str(rf.get("file_type") or "").upper() == "M4A")]
        return m4a[0] if m4a else audio_only[0]

    mp4s = [rf for rf in media if (str(rf.get("file_type") or "").upper() == "MP4")]
    if mp4s:
        def size_or_big(x: Dict[str, Any]) -> int:
            try:
                return int(x.get("file_size") or 10**18)
            except Exception:
                return 10**18
        mp4s.sort(key=size_or_big)
        return mp4s[0]

    return media[0]


def _zoom_encrypted_token(plain_token: str) -> str:
    if not ZOOM_WEBHOOK_SECRET_TOKEN:
        raise HTTPException(status_code=500, detail="Zoom webhook secret token not configured")

    digest = hmac.new(
        ZOOM_WEBHOOK_SECRET_TOKEN.encode("utf-8"),
        plain_token.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return digest


def _proxy_sig(meeting_id: str, file_id: str, exp: int) -> str:
    if not MEDIA_PROXY_SECRET:
        raise HTTPException(status_code=500, detail="MEDIA_PROXY_SECRET not configured")
    msg = f"{meeting_id}:{file_id}:{exp}".encode("utf-8")
    return hmac.new(MEDIA_PROXY_SECRET.encode("utf-8"), msg, hashlib.sha256).hexdigest()


def _build_proxy_url(meeting_id: str, file_id: str, exp: int) -> str:
    sig = _proxy_sig(meeting_id, file_id, exp)
    # Encode file_id for safety (it can contain special chars)
    return (
        f"{PUBLIC_BASE_URL}/recordings/proxy"
        f"?meeting_id={quote(str(meeting_id))}"
        f"&file_id={quote(str(file_id))}"
        f"&exp={exp}"
        f"&sig={sig}"
    )


@app.get("/recordings/proxy")
async def recordings_proxy(meeting_id: str, file_id: str, exp: int, sig: str):
    """
    Secure proxy for AI worker:
    - validates HMAC signature + expiry
    - fetches the real Zoom download_url via Zoom API
    - streams the bytes back to the caller
    """
    now = int(time.time())
    if exp < now:
        raise HTTPException(status_code=403, detail="Link expired")

    expected = _proxy_sig(meeting_id, file_id, exp)
    if not hmac.compare_digest(expected, sig):
        raise HTTPException(status_code=403, detail="Bad signature")

    data = await get_meeting_recordings(meeting_id)
    recording_files = data.get("recording_files") or []

    chosen = None
    for rf in recording_files:
        if str(rf.get("id") or "") == str(file_id):
            chosen = rf
            break

    if not chosen:
        raise HTTPException(status_code=404, detail="Recording file not found")

    download_url = chosen.get("download_url")
    if not download_url:
        raise HTTPException(status_code=404, detail="No download_url for recording file")

    client, resp = await stream_recording_bytes(download_url)

    content_type = resp.headers.get("content-type") or "application/octet-stream"

    async def _iter():
        try:
            async for chunk in resp.aiter_bytes():
                yield chunk
        finally:
            await client.aclose()

    return StreamingResponse(_iter(), media_type=content_type)


@app.post("/zoom/recording-webhook")
async def zoom_recording_webhook(request: Request) -> Dict[str, Any]:
    payload = await request.json()
    zoom_event = payload.get("event")

    # URL validation
    if zoom_event == "endpoint.url_validation":
        validation_payload = (payload.get("payload") or {})
        plain_token = validation_payload.get("plainToken")
        if not plain_token:
            raise HTTPException(status_code=400, detail="plainToken missing in validation payload")
        return {"plainToken": plain_token, "encryptedToken": _zoom_encrypted_token(plain_token)}

    # Recording events only
    if not zoom_event or "recording" not in zoom_event:
        raise HTTPException(status_code=400, detail="Not a recording event")

    zoom_object = (payload.get("payload") or {}).get("object") or {}

    zoom_meeting_id = str(zoom_object.get("id") or "")
    zoom_meeting_uuid = zoom_object.get("uuid")
    zoom_start_time = zoom_object.get("start_time")

    if not zoom_meeting_id:
        raise HTTPException(status_code=400, detail="Zoom meeting ID is missing")

    # STRICT media selection (from payload)
    recording_files = zoom_object.get("recording_files") or []
    chosen = _choose_media_recording_file(recording_files)
    if not chosen:
        print(f"No media recording file found for meeting {zoom_meeting_id}")
        return {"status": "ignored_no_media_recording_file", "zoom_meeting_id": zoom_meeting_id}

    recording_file_id = chosen.get("id")
    if not recording_file_id:
        print(f"No recording file id found for meeting {zoom_meeting_id}")
        return {"status": "ignored_no_recording_file_id", "zoom_meeting_id": zoom_meeting_id}

    # Store PROXY url in HubSpot (AI worker downloads from us, we fetch from Zoom)
    exp = int(time.time()) + (7 * 24 * 60 * 60)  # 7 days
    proxy_url = _build_proxy_url(zoom_meeting_id, str(recording_file_id), exp)

    # Match HubSpot meeting
    meeting = await search_meeting_by_zoom_id(zoom_meeting_id)
    if not meeting:
        print(f"No HubSpot meeting found for Zoom meeting ID {zoom_meeting_id}. Ignoring webhook.")
        return {"status": "ignored_no_matching_meeting", "zoom_meeting_id": zoom_meeting_id}

    meeting_id = meeting.get("id")
    meeting_props = meeting.get("properties") or {}
    meeting_type = meeting_props.get("hs_activity_type")

    # SAFEGUARD: require activity type
    if not meeting_type:
        print(f"HubSpot meeting {meeting_id} has no hs_activity_type. Ignoring webhook.")
        return {
            "status": "ignored_missing_activity_type",
            "hubspot_meeting_id": meeting_id,
            "zoom_meeting_id": zoom_meeting_id,
        }

    # Get associations FIRST (so we can name the call using primary contact)
    contact_ids = await get_meeting_contact_ids(meeting_id)
    deal_ids = await get_meeting_deal_ids(meeting_id)

    primary_contact_name = None
    if contact_ids:
        try:
            primary_contact_name = await get_contact_name(contact_ids[0])
        except Exception as e:
            print("Failed to fetch primary contact name:", repr(e))

    # Determine disposition via participants (if available)
    disposition = DISPOSITION_CONNECTED
    participant_count = None
    if zoom_meeting_uuid:
        try:
            participant_count = await get_participant_count(zoom_meeting_uuid)
            if participant_count <= 1:
                disposition = DISPOSITION_NO_ANSWER
        except Exception as e:
            print("Zoom participant lookup failed; defaulting disposition to CONNECTED.")
            print("Error:", repr(e))

    duration_ms = _extract_duration_ms(zoom_object)

    call = await create_call_for_meeting(
        meeting=meeting,
        meeting_type=meeting_type,
        recording_url=proxy_url,
        zoom_meeting_id=zoom_meeting_id,
        zoom_meeting_uuid=zoom_meeting_uuid,
        zoom_start_time=zoom_start_time,
        duration_ms=duration_ms,
        disposition=disposition,
        primary_contact_name=primary_contact_name,
    )

    call_id = call.get("id")

    await associate_call_to_contacts(call_id, contact_ids)
    await associate_call_to_deals(call_id, deal_ids)

    return {
        "status": "ok",
        "hubspot_call_id": call_id,
        "participant_count": participant_count,
        "disposition": disposition,
        "associated_contact_ids": contact_ids,
        "associated_deal_ids": deal_ids,
    }


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}



