import httpx
from urllib.parse import quote

from config import (
    ZOOM_ACCOUNT_ID,
    ZOOM_CLIENT_ID,
    ZOOM_CLIENT_SECRET,
)

ZOOM_BASE_URL = "https://api.zoom.us/v2"


async def get_zoom_access_token() -> str:
    """
    Obtain OAuth access token using Server-to-Server OAuth credentials.
    """
    if not (ZOOM_ACCOUNT_ID and ZOOM_CLIENT_ID and ZOOM_CLIENT_SECRET):
        raise RuntimeError("Missing Zoom OAuth env vars (ZOOM_ACCOUNT_ID/ZOOM_CLIENT_ID/ZOOM_CLIENT_SECRET).")

    url = f"https://zoom.us/oauth/token?grant_type=account_credentials&account_id={ZOOM_ACCOUNT_ID}"
    auth = (ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET)

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, auth=auth)
        resp.raise_for_status()
        data = resp.json()

    return data["access_token"]


def _encode_meeting_uuid_for_path(meeting_uuid: str) -> str:
    """
    Zoom requires the meeting UUID to be URL-encoded when used in path params.
    In some cases Zoom expects it to be double-encoded. We'll do that to be safe.
    """
    once = quote(meeting_uuid, safe="")
    twice = quote(once, safe="")
    return twice


async def get_participant_count(meeting_uuid: str) -> int:
    """
    Fetch participant list from Zoom's 'past meeting participants' API.
    Return the number of unique participants.
    """
    token = await get_zoom_access_token()
    safe_uuid = _encode_meeting_uuid_for_path(meeting_uuid)

    url = f"{ZOOM_BASE_URL}/past_meetings/{safe_uuid}/participants?page_size=300"
    headers = {"Authorization": f"Bearer {token}"}

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers=headers)

    # If Zoom doesn’t have the past meeting yet / no access, treat as host-only
    if resp.status_code == 404:
        return 1

    if resp.status_code >= 400:
        # Print useful detail for debugging in Render logs
        print("Zoom participants API error:")
        print("URL:", url)
        print("Status:", resp.status_code)
        try:
            print("Body:", resp.text)
        except Exception:
            pass
        resp.raise_for_status()

    data = resp.json()
    participants = data.get("participants") or []

    unique = set()
    for p in participants:
        name = p.get("name")
        email = p.get("user_email")
        unique.add((name, email))

    return len(unique)
