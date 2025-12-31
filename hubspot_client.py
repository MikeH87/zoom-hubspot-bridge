import datetime
from typing import List, Optional

import httpx

from config import HUBSPOT_BASE_URL, HUBSPOT_PRIVATE_APP_TOKEN


HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_PRIVATE_APP_TOKEN}",
    "Content-Type": "application/json",
}


async def search_meeting_by_zoom_id(zoom_meeting_id: str) -> Optional[dict]:
    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/meetings/search"

    payload = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "hs_meeting_location",
                        "operator": "CONTAINS_TOKEN",
                        "value": zoom_meeting_id,
                    }
                ]
            }
        ],
        "properties": [
            "hs_activity_type",
            "hs_meeting_location",
            "hubspot_owner_id",
        ],
        "limit": 5,
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, json=payload, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()

    results = data.get("results", [])
    if not results:
        return None

    return results[0]


def _iso_to_epoch_ms(iso_str: Optional[str]) -> int:
    if not iso_str:
        return int(datetime.datetime.utcnow().timestamp() * 1000)

    try:
        if iso_str.endswith("Z"):
            iso_str = iso_str.replace("Z", "+00:00")
        dt = datetime.datetime.fromisoformat(iso_str)
        return int(dt.timestamp() * 1000)
    except Exception:
        return int(datetime.datetime.utcnow().timestamp() * 1000)


async def get_contact_name(contact_id: str) -> Optional[str]:
    """
    Returns 'First Last' if available, otherwise best-effort name.
    """
    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/{contact_id}?properties=firstname,lastname"

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers=HEADERS)
        if resp.status_code >= 400:
            return None
        data = resp.json()

    props = data.get("properties") or {}
    first = (props.get("firstname") or "").strip()
    last = (props.get("lastname") or "").strip()

    full = f"{first} {last}".strip()
    return full or None


async def get_meeting_contact_ids(meeting_id: str) -> List[str]:
    url = f"{HUBSPOT_BASE_URL}/crm/v4/objects/meetings/{meeting_id}/associations/contacts"

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()

    results = data.get("results") or []
    contact_ids: List[str] = []

    for row in results:
        to_id = row.get("toObjectId")
        if to_id is not None:
            contact_ids.append(str(to_id))

    return contact_ids


async def get_meeting_deal_ids(meeting_id: str) -> List[str]:
    url = f"{HUBSPOT_BASE_URL}/crm/v4/objects/meetings/{meeting_id}/associations/deals"

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()

    results = data.get("results") or []
    deal_ids: List[str] = []

    for row in results:
        to_id = row.get("toObjectId")
        if to_id is not None:
            deal_ids.append(str(to_id))

    return deal_ids


async def create_call_for_meeting(
    *,
    meeting: dict,
    meeting_type: Optional[str],
    recording_url: str,
    zoom_meeting_id: str,
    zoom_meeting_uuid: Optional[str],
    zoom_start_time: Optional[str],
    duration_ms: Optional[int] = None,
    disposition: str = "",
    primary_contact_name: Optional[str] = None,
) -> dict:
    """
    Create a HubSpot call record representing a Zoom meeting recording.
    """
    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/calls"

    timestamp_ms = _iso_to_epoch_ms(zoom_start_time)

    # Title format: "First Last – Zoom int – Call Type"
    if primary_contact_name:
        call_title = f"{primary_contact_name} – Zoom int – {meeting_type or ''}".strip()
        call_title = call_title.rstrip(" –")
    else:
        call_title = f"Zoom int – {meeting_type or ''}".strip()
        call_title = call_title.rstrip(" –")

    properties = {
        "hs_timestamp": timestamp_ms,
        "hs_call_status": "COMPLETED",
        "hs_call_direction": "OUTBOUND",
        "hs_call_recording_url": recording_url,
        "hs_activity_type": meeting_type or "",
        "hs_call_title": call_title,
        "hs_call_body": f"Integration-created call record for Zoom meeting ID {zoom_meeting_id}. This mirrors the HubSpot meeting activity and exists to attach recording + analysis.",
        "zoom_meeting_id": zoom_meeting_id,
        "integration_call": True,
        "hs_call_duration": duration_ms if duration_ms is not None else 0,
    }

    # Copy owner from the source Meeting onto the created Call (so the scorecard inherits it)
    meeting_props = (meeting.get("properties") or {}) if isinstance(meeting, dict) else {}
    owner_id = meeting_props.get("hubspot_owner_id") or meeting_props.get("hs_owner_id")
    if owner_id:
        properties["hubspot_owner_id"] = str(owner_id)

    if disposition:
        properties["hs_call_disposition"] = disposition

    if zoom_meeting_uuid:
        properties["zoom_meeting_uuid"] = zoom_meeting_uuid

    payload = {"properties": properties}

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, json=payload, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()

    return data


async def associate_call_to_contacts(call_id: str, contact_ids: List[str]) -> None:
    if not contact_ids:
        return

    url = f"{HUBSPOT_BASE_URL}/crm/associations/2025-09/Calls/Contacts/batch/associate/default"
    payload = {"inputs": [{"from": {"id": call_id}, "to": {"id": cid}} for cid in contact_ids]}

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, json=payload, headers=HEADERS)

    if resp.status_code >= 400:
        print("Error associating call to contacts (default):")
        print("Status:", resp.status_code)
        print("Body:", resp.text)


async def associate_call_to_deals(call_id: str, deal_ids: List[str]) -> None:
    if not deal_ids:
        return

    url = f"{HUBSPOT_BASE_URL}/crm/associations/2025-09/Calls/Deals/batch/associate/default"
    payload = {"inputs": [{"from": {"id": call_id}, "to": {"id": did}} for did in deal_ids]}

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, json=payload, headers=HEADERS)

    if resp.status_code >= 400:
        print("Error associating call to deals (default):")
        print("Status:", resp.status_code)
        print("Body:", resp.text)
