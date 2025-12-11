import datetime
from typing import List, Optional

import httpx

from config import HUBSPOT_BASE_URL, HUBSPOT_PRIVATE_APP_TOKEN


HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_PRIVATE_APP_TOKEN}",
    "Content-Type": "application/json",
}


async def search_meeting_by_zoom_id(zoom_meeting_id: str) -> Optional[dict]:
    """
    Find the first HubSpot meeting whose location / conference link contains the Zoom meeting ID.
    """
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
    """
    Convert an ISO 8601 string from Zoom (e.g. 2025-12-09T13:30:00Z) to epoch milliseconds.
    If missing or invalid, fall back to "now".
    """
    if not iso_str:
        return int(datetime.datetime.utcnow().timestamp() * 1000)

    try:
        if iso_str.endswith("Z"):
            iso_str = iso_str.replace("Z", "+00:00")
        dt = datetime.datetime.fromisoformat(iso_str)
        return int(dt.timestamp() * 1000)
    except Exception:
        return int(datetime.datetime.utcnow().timestamp() * 1000)


async def get_meeting_contact_ids(meeting_id: str) -> List[str]:
    """
    Return a list of contact IDs associated with the given meeting.
    Uses the v4 associations endpoint: meetings -> contacts.
    """
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
    """
    Return a list of deal IDs associated with the given meeting.
    Uses the v4 associations endpoint: meetings -> deals.
    """
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
) -> dict:
    """
    Create a HubSpot call record representing a Zoom meeting recording.
    This just creates the call; associations are handled separately.
    """
    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/calls"

    timestamp_ms = _iso_to_epoch_ms(zoom_start_time)

    properties = {
        "hs_timestamp": timestamp_ms,
        "hs_call_status": "COMPLETED",
        "hs_call_direction": "OUTBOUND",
        "hs_call_recording_url": recording_url,
        "hs_activity_type": meeting_type or "",
        "zoom_meeting_id": zoom_meeting_id,
    }

    if zoom_meeting_uuid:
        properties["zoom_meeting_uuid"] = zoom_meeting_uuid

    payload = {
        "properties": properties,
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, json=payload, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()

    return data


async def associate_call_to_contacts(call_id: str, contact_ids: List[str]) -> None:
    """
    Associate the given call with a list of contacts using the default association type.
    Uses the date-versioned associations API (2025-09).
    """
    if not contact_ids:
        print("No contacts to associate.")
        return

    # Note: 'Calls' and 'Contacts' are the object type names expected by the associations API.
    url = f"{HUBSPOT_BASE_URL}/crm/associations/2025-09/Calls/Contacts/batch/associate/default"

    inputs = []
    for cid in contact_ids:
        inputs.append(
            {
                "from": {"id": call_id},
                "to": {"id": cid},
            }
        )

    payload = {"inputs": inputs}

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, json=payload, headers=HEADERS)

    if resp.status_code >= 400:
        print("Error associating call to contacts (default):")
        print("Status:", resp.status_code)
        try:
            print("Body:", resp.text)
        except Exception:
            pass


async def associate_call_to_deals(call_id: str, deal_ids: List[str]) -> None:
    """
    Associate the given call with a list of deals using the default association type.
    Uses the date-versioned associations API (2025-09).
    """
    if not deal_ids:
        print("No deals to associate.")
        return

    url = f"{HUBSPOT_BASE_URL}/crm/associations/2025-09/Calls/Deals/batch/associate/default"

    inputs = []
    for did in deal_ids:
        inputs.append(
            {
                "from": {"id": call_id},
                "to": {"id": did},
            }
        )

    payload = {"inputs": inputs}

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, json=payload, headers=HEADERS)

    if resp.status_code >= 400:
        print("Error associating call to deals (default):")
        print("Status:", resp.status_code)
        try:
            print("Body:", resp.text)
        except Exception:
            pass
