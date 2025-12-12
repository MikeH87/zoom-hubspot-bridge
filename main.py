from typing import Any, Dict, Optional, List
import hmac
import hashlib

from fastapi import FastAPI, HTTPException, Request

from config import ZOOM_WEBHOOK_SECRET_TOKEN
from hubspot_client import (
    search_meeting_by_zoom_id,
    create_call_for_meeting,
    get_meeting_contact_ids,
    get_meeting_deal_ids,
    get_contact_name,
    associate_call_to_contacts,
    associate_call_to_deals,
)
from zoom_client import get_participant_count


app = FastAPI()

DISPOSITION_CONNECTED = "f240bbac-87c9-4f6e-bf70-924b57d47db7"
DISPOSITION_NO_ANSWER = "73a0d17f-1163-4015-bdd5-ec830791da20"

# Only allow real media types (ignore transcript/chat/cc/etc)
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

    # Filter to media only
    media = []
    for rf in recording_files:
        ft = (rf.get("file_type") or "").upper().strip()
        if ft in ALLOWED_MEDIA_FILE_TYPES:
            media.append(rf)

    if not media:
        return None

    # Prefer audio_only first
    audio_only = [rf for rf in media if (rf.get("recording_type") == "audio_only")]
    if audio_only:
        # Prefer M4A inside audio_only
        m4a = [rf for rf in audio_only if (str(rf.get("file_type") or "").upper() == "M4A")]
        if m4a:
            return m4a[0]
        return audio_only[0]

    # Otherwise: smallest MP4
    mp4s = [rf for rf in media if (str(rf.get("file_type") or "").upper() == "MP4")]
    if mp4s:
        def size_or_big(x: Dict[str, Any]) -> int:
            try:
                return int(x.get("file_size") or 10**18)
            except Exception:
                return 10**18
        mp4s.sort(key=size_or_big)
        return mp4s[0]

    # Else fall back to first remaining media file
    return media[0]


def _extract_media_download_url(zoom_object: Dict[str, Any]) -> Optional[str]:
    recording_files = zoom_object.get("recording_files") or []
    chosen = _choose_media_recording_file(recording_files)
    if not chosen:
        return None
    return chosen.get("download_url") or chosen.get("play_url")


def _zoom_encrypted_token(plain_token: str) -> str:
    if not ZOOM_WEBHOOK_SECRET_TOKEN:
        raise HTTPException(status_code=500, detail="Zoom webhook secret token not configured")

    digest = hmac.new(
        ZOOM_WEBHOOK_SECRET_TOKEN.encode("utf-8"),
        plain_token.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return digest


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

    # STRICT media URL selection
    recording_url = _extract_media_download_url(zoom_object)
    if not recording_url:
        print(f"No media recording file found for meeting {zoom_meeting_id}")
        return {
            "status": "ignored_no_media_recording_file",
            "zoom_meeting_id": zoom_meeting_id,
        }

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

    # Create call with better title format
    call = await create_call_for_meeting(
        meeting=meeting,
        meeting_type=meeting_type,
        recording_url=recording_url,
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
