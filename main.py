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


app = FastAPI()


def _extract_recording_url(zoom_object: Dict[str, Any]) -> Optional[str]:
    """
    Try to pick a sensible recording URL from the Zoom webhook payload.
    Prefer an audio-only recording if available, otherwise fall back to the first file.
    """
    recording_files = zoom_object.get("recording_files") or []
    if not recording_files:
        return None

    # Prefer audio-only if present
    for rf in recording_files:
        if rf.get("recording_type") == "audio_only":
            return rf.get("download_url") or rf.get("play_url")

    # Otherwise just use the first file
    first = recording_files[0]
    return first.get("download_url") or first.get("play_url")


def _zoom_encrypted_token(plain_token: str) -> str:
    """
    Create HMAC-SHA256 hash of plainToken using Zoom webhook secret token.
    Zoom expects the hex digest as encryptedToken.
    """
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
    """
    Endpoint to receive Zoom webhooks.

    Handles:
    - endpoint.url_validation (initial URL verification)
    - recording.* events (e.g. recording.completed)
    """
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

    # 2) For all other events, optionally check a simple auth header if configured
    if ZOOM_WEBHOOK_SECRET_TOKEN:
        # This is optional extra protection – you can remove this block if you only rely on signature
        auth_header = request.headers.get("Authorization")
        # If you later want to enforce an auth header, add logic here.
        # For now we do not reject based on this.
        _ = auth_header

    # 3) Now handle recording events
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

    # 4) Find the matching HubSpot meeting
    meeting = await search_meeting_by_zoom_id(zoom_meeting_id)
    if not meeting:
        raise HTTPException(
            status_code=404,
            detail=f"No HubSpot meeting found with Zoom meeting ID {zoom_meeting_id}",
        )

    meeting_id = meeting.get("id")
    meeting_props = meeting.get("properties") or {}
    meeting_type = meeting_props.get("hs_activity_type")

    # 5) Create a call for this meeting
    call = await create_call_for_meeting(
        meeting=meeting,
        meeting_type=meeting_type,
        recording_url=recording_url,
        zoom_meeting_id=zoom_meeting_id,
        zoom_meeting_uuid=zoom_meeting_uuid,
        zoom_start_time=zoom_start_time,
    )

    call_id = call.get("id")

    # 6) Fetch associated contacts and deals from the meeting
    contact_ids = await get_meeting_contact_ids(meeting_id)
    deal_ids = await get_meeting_deal_ids(meeting_id)

    # 7) Associate the call with those contacts and deals
    await associate_call_to_contacts(call_id, contact_ids)
    await associate_call_to_deals(call_id, deal_ids)

    return {
        "status": "ok",
        "hubspot_call_id": call_id,
        "associated_contact_ids": contact_ids,
        "associated_deal_ids": deal_ids,
    }


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}
