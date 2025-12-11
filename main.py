from typing import Any, Dict, Optional

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


@app.post("/zoom/recording-webhook")
async def zoom_recording_webhook(request: Request) -> Dict[str, Any]:
    """
    Endpoint to receive Zoom "recording.completed" webhooks.
    """
    # Optional simple token check (currently disabled if env value is blank)
    if ZOOM_WEBHOOK_SECRET_TOKEN:
        auth_header = request.headers.get("Authorization")
        if auth_header != ZOOM_WEBHOOK_SECRET_TOKEN:
            raise HTTPException(status_code=401, detail="Invalid Zoom webhook token")

    payload = await request.json()

    zoom_event = payload.get("event")
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

    # 1) Find the matching HubSpot meeting
    meeting = await search_meeting_by_zoom_id(zoom_meeting_id)
    if not meeting:
        raise HTTPException(
            status_code=404,
            detail=f"No HubSpot meeting found with Zoom meeting ID {zoom_meeting_id}",
        )

    meeting_id = meeting.get("id")
    meeting_props = meeting.get("properties") or {}
    meeting_type = meeting_props.get("hs_activity_type")

    # 2) Create a call for this meeting
    call = await create_call_for_meeting(
        meeting=meeting,
        meeting_type=meeting_type,
        recording_url=recording_url,
        zoom_meeting_id=zoom_meeting_id,
        zoom_meeting_uuid=zoom_meeting_uuid,
        zoom_start_time=zoom_start_time,
    )

    call_id = call.get("id")

    # 3) Fetch associated contacts and deals from the meeting
    contact_ids = await get_meeting_contact_ids(meeting_id)
    deal_ids = await get_meeting_deal_ids(meeting_id)

    # 4) Associate the call with those contacts and deals
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
