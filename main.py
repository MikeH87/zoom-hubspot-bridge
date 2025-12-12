from typing import Any, Dict, Optional
import hmac
import hashlib

from fastapi import FastAPI, HTTPException, Request

from config import ZOOM_WEBHOOK_SECRET_TOKEN
from hubspot_client import (
    search_meeting_by_zoom_id,
    create_call_for_meeting,
    get_meeting_contact_ids,
    get_meeting_deal_ids,
    associate_call_to_contacts,
    associate_call_to_deals,
)
from zoom_client import get_participant_count


app = FastAPI()

# HubSpot call disposition internal values you provided
DISPOSITION_CONNECTED = "f240bbac-87c9-4f6e-bf70-924b57d47db7"
DISPOSITION_NO_ANSWER = "73a0d17f-1163-4015-bdd5-ec830791da20"


def _extract_recording_url(zoom_object: Dict[str, Any]) -> Optional[str]:
    recording_files = zoom_object.get("recording_files") or []
    if not recording_files:
        return None

    for rf in recording_files:
        if rf.get("recording_type") == "audio_only":
            return rf.get("download_url") or rf.get("play_url")

    first = recording_files[0]
    return first.get("download_url") or first.get("play_url")


def _extract_duration_ms(zoom_object: Dict[str, Any]) -> int:
    """
    Zoom 'duration' is in minutes. Convert to milliseconds.
    """
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

    # 1) Handle Zoom URL validation
    if zoom_event == "endpoint.url_validation":
        validation_payload = (payload.get("payload") or {})
        plain_token = validation_payload.get("plainToken")

        if not plain_token:
            raise HTTPException(status_code=400, detail="plainToken missing in validation payload")

        encrypted_token = _zoom_encrypted_token(plain_token)

        return {
            "plainToken": plain_token,
            "encryptedToken": encrypted_token,
        }

    # 2) Handle recording events
    if not zoom_event or "recording" not in zoom_event:
        raise HTTPException(status_code=400, detail="Not a recording event")

    zoom_object = (payload.get("payload") or {}).get("object") or {}

    zoom_meeting_id = str(zoom_object.get("id") or "")
    zoom_meeting_uuid = zoom_object.get("uuid")
    zoom_start_time = zoom_object.get("start_time")

    if not zoom_meeting_id:
        raise HTTPException(status_code=400, detail="Zoom meeting ID is missing")

    recording_url = _extract_recording_url(zoom_object)
    if not recording_url:
        raise HTTPException(status_code=400, detail="No recording URL found in Zoom payload")

    # If there is no matching meeting in HubSpot, ignore the webhook
    meeting = await search_meeting_by_zoom_id(zoom_meeting_id)
    if not meeting:
        print(f"No HubSpot meeting found for Zoom meeting ID {zoom_meeting_id}. Ignoring webhook.")
        return {"status": "ignored_no_matching_meeting", "zoom_meeting_id": zoom_meeting_id}

    meeting_id = meeting.get("id")
    meeting_props = meeting.get("properties") or {}
    meeting_type = meeting_props.get("hs_activity_type")

    # 3) Determine disposition using Zoom participant count (past meeting API)
    disposition = DISPOSITION_CONNECTED
    participant_count = None

    if zoom_meeting_uuid:
        try:
            participant_count = await get_participant_count(zoom_meeting_uuid)
            if participant_count <= 1:
                disposition = DISPOSITION_NO_ANSWER
        except Exception as e:
            # If Zoom API fails, default to Connected (safer than marking No Answer incorrectly)
            print("Zoom participant lookup failed; defaulting disposition to CONNECTED.")
            print("Error:", repr(e))

    # 4) Duration
    duration_ms = _extract_duration_ms(zoom_object)

    # 5) Create call with duration + disposition
    call = await create_call_for_meeting(
        meeting=meeting,
        meeting_type=meeting_type,
        recording_url=recording_url,
        zoom_meeting_id=zoom_meeting_id,
        zoom_meeting_uuid=zoom_meeting_uuid,
        zoom_start_time=zoom_start_time,
        duration_ms=duration_ms,
        call_outcome=disposition,  # we'll map this inside hubspot_client to hs_call_disposition
    )

    call_id = call.get("id")

    # 6) Associate to contacts + deals
    contact_ids = await get_meeting_contact_ids(meeting_id)
    deal_ids = await get_meeting_deal_ids(meeting_id)

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

