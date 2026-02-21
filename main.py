from hubspot_client import find_existing_call_by_zoom_meeting_id
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
    associate_meeting_to_deals,
    get_latest_deal,
    get_latest_deal_from_contacts,
    update_deal_stage,
    mark_meeting_completed,
)
from zoom_client import get_participant_count, get_meeting_recordings, stream_recording_bytes


app = FastAPI()


DISPOSITION_CONNECTED = "f240bbac-87c9-4f6e-bf70-924b57d47db7"
DISPOSITION_NO_ANSWER = "73a0d17f-1163-4015-bdd5-ec830791da20"

ALLOWED_MEDIA_FILE_TYPES = {"M4A", "MP4", "MP3", "WAV"}

# Deal stages
DEAL_STAGE_QUALIFIED = "1054943518"  # Qualified (Zoom call arranged)
DEAL_STAGE_FOLLOWUP = "1054943519"   # Followup (Zoom call completed)
DEAL_STAGE_SENT_CLIENT_AGREEMENT = "1054943520"
DEAL_STAGE_AGREEMENT_SIGNED = "1054943521"
DEAL_STAGE_CLOSED_LOST = "1054943524"
DEAL_STAGE_CANCELLED = "1080046258"

PROTECTED_DEAL_STAGES = {
    DEAL_STAGE_SENT_CLIENT_AGREEMENT,
    DEAL_STAGE_AGREEMENT_SIGNED,
    DEAL_STAGE_CANCELLED,
}


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


def _choose_media_recording_file(recording_files):
    """
    Choose the *actual media* recording file from Zoom payloads.

    Prefer real audio/video (MP4/M4A/MP3/WAV) and avoid transcript/summary/timeline.
    This prevents streaming WEBVTT/JSON into the AI worker.
    """
    if not recording_files:
        return None

    files = [f for f in recording_files if isinstance(f, dict)]

    # Prefer explicit media file types first
    media = []
    for f in files:
        ft = (f.get('file_type') or f.get('file_extension') or '').upper()
        rt = (f.get('recording_type') or '').lower()

        # Exclude non-media
        if ft in {'TRANSCRIPT','SUMMARY','TIMELINE','CHAT','CC','TXT','VTT','JSON'}:
            continue
        if rt in {'audio_transcript','transcript','summary','timeline','chat_file','closed_caption'}:
            continue

        if ft in {'MP4','M4A','MP3','WAV'}:
            media.append(f)

    # Prefer audio_only if present
    if media:
        for f in media:
            if (f.get('recording_type') or '').lower() == 'audio_only':
                return f
        return media[0]

    # Fallback: first thing with a URL that isn't transcript-ish
    for f in files:
        rt = (f.get('recording_type') or '').lower()
        if rt in {'audio_transcript','transcript','summary','timeline','chat_file','closed_caption'}:
            continue
        if f.get('download_url') or f.get('play_url'):
            return f

    return None

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

    client, cm, resp = await stream_recording_bytes(download_url)

    content_type = resp.headers.get("content-type") or "application/octet-stream"

    async def _iter():
        try:
            async for chunk in resp.aiter_bytes():
                yield chunk
        finally:
            await cm.__aexit__(None, None, None)
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
    # Only create calls when the recording is actually completed
    if zoom_event != "recording.completed":
        return {"status": "ignored_event", "event": zoom_event}
    zoom_object = (payload.get("payload") or {}).get("object") or {}

    zoom_meeting_id = str(zoom_object.get("id") or "")
    zoom_meeting_uuid = zoom_object.get("uuid")
    zoom_start_time = zoom_object.get("start_time")

    if not zoom_meeting_id:
        raise HTTPException(status_code=400, detail="Zoom meeting ID is missing")

    # STRICT media selection (from payload)
    # Always pull the authoritative recording file list from Zoom API (webhook payload can be incomplete)
    data = await get_meeting_recordings(str(zoom_meeting_id))
    recording_files = data.get("recording_files") or []
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

    existing_call = await find_existing_call_by_zoom_meeting_id(zoom_meeting_id)

    if existing_call:

        return {"status": "duplicate_ignored", "hubspot_call_id": existing_call.get("id")}


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

    # Deal stage update logic
    try:
        latest_meeting_deal = await get_latest_deal(deal_ids)
    except Exception as e:
        print("Failed to fetch meeting-associated deals:", repr(e))
        latest_meeting_deal = None

    def _stage_from_deal(deal: Optional[dict]) -> Optional[str]:
        if not deal:
            return None
        props = deal.get("properties") or {}
        return props.get("dealstage")

    updated_deal_id = None
    updated_deal_stage = None

    if latest_meeting_deal:
        stage = _stage_from_deal(latest_meeting_deal)
        deal_id = str(latest_meeting_deal.get("id") or "")

        if stage in PROTECTED_DEAL_STAGES:
            print(f"Skipping protected deal stage for deal {deal_id}: {stage}")
        elif stage == DEAL_STAGE_QUALIFIED:
            await update_deal_stage(deal_id, DEAL_STAGE_FOLLOWUP)
            updated_deal_id = deal_id
            updated_deal_stage = DEAL_STAGE_FOLLOWUP
        else:
            print(f"Deal stage not eligible for update (meeting-associated): {stage}")
    else:
        # Fallback: find deal via contact associations
        try:
            contact_deal = await get_latest_deal_from_contacts(contact_ids)
        except Exception as e:
            print("Failed to fetch contact-associated deals:", repr(e))
            contact_deal = None

        if contact_deal:
            deal_id = str(contact_deal.get("id") or "")
            stage = _stage_from_deal(contact_deal)

            # Associate the deal to both call and meeting
            await associate_call_to_deals(call_id, [deal_id])
            await associate_meeting_to_deals(meeting_id, [deal_id])

            if stage in PROTECTED_DEAL_STAGES:
                print(f"Skipping protected deal stage for deal {deal_id}: {stage}")
            elif stage in {DEAL_STAGE_QUALIFIED, DEAL_STAGE_CLOSED_LOST}:
                await update_deal_stage(deal_id, DEAL_STAGE_FOLLOWUP)
                updated_deal_id = deal_id
                updated_deal_stage = DEAL_STAGE_FOLLOWUP
            else:
                print(f"Deal stage not eligible for update (contact fallback): {stage}")
        else:
            print("No deal found via meeting or contact associations; skipping deal update.")

    try:
        await mark_meeting_completed(meeting_id)
    except Exception as e:
        print(f"Failed to mark meeting {meeting_id} as completed:", repr(e))

    return {
        "status": "ok",
        "hubspot_call_id": call_id,
        "participant_count": participant_count,
        "disposition": disposition,
        "associated_contact_ids": contact_ids,
        "associated_deal_ids": deal_ids,
        "updated_deal_id": updated_deal_id,
        "updated_deal_stage": updated_deal_stage,
    }


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}



