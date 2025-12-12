import httpx
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
    url = f"https://zoom.us/oauth/token?grant_type=account_credentials&account_id={ZOOM_ACCOUNT_ID}"

    auth = (ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET)

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, auth=auth)
        resp.raise_for_status()
        data = resp.json()

    return data["access_token"]


async def get_participant_count(meeting_uuid: str) -> int:
    """
    Fetch participant list from Zoom's 'past meeting participants' API.
    Return the number of unique participants.
    """
    # Zoom requires URL-encoded UUID, replacing '+' with '%2B', '/' with '%2F'
    safe_uuid = meeting_uuid.replace("+", "%2B").replace("/", "%2F")

    token = await get_zoom_access_token()

    url = f"{ZOOM_BASE_URL}/past_meetings/{safe_uuid}/participants?page_size=300"

    headers = {"Authorization": f"Bearer {token}"}

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers=headers)

    # If Zoom doesn’t have the past meeting yet, return 1 (host only)
    if resp.status_code == 404:
        return 1

    resp.raise_for_status()
    data = resp.json()

    participants = data.get("participants") or []
    unique = set()

    for p in participants:
        # Unique by name or email — whatever data Zoom provides.
        name = p.get("name")
        email = p.get("user_email")
        unique.add((name, email))

    return len(unique)
