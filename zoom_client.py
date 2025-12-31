import httpx
from urllib.parse import quote, urlparse, parse_qsl, urlencode, urlunparse

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
    Zoom requires the meeting UUID to be URL-encoded (often double-encoded) in path params.
    """
    once = quote(meeting_uuid, safe="")
    twice = quote(once, safe="")
    return twice


async def get_participant_count(meeting_uuid: str) -> int:
    token = await get_zoom_access_token()
    safe_uuid = _encode_meeting_uuid_for_path(meeting_uuid)

    url = f"{ZOOM_BASE_URL}/past_meetings/{safe_uuid}/participants?page_size=300"
    headers = {"Authorization": f"Bearer {token}"}

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers=headers)

    if resp.status_code == 404:
        return 1

    if resp.status_code >= 400:
        print("Zoom participants API error:")
        print("URL:", url)
        print("Status:", resp.status_code)
        print("Body:", resp.text)
        resp.raise_for_status()

    data = resp.json()
    participants = data.get("participants") or []

    unique = set()
    for p in participants:
        unique.add((p.get("name"), p.get("user_email")))

    return len(unique)


async def get_meeting_recordings(meeting_id: str) -> dict:
    """
    Returns Zoom 'Get meeting recordings' response for a given meeting_id.
    Requires recording scopes on your S2S app.
    """
    token = await get_zoom_access_token()
    url = f"{ZOOM_BASE_URL}/meetings/{meeting_id}/recordings"
    headers = {"Authorization": f"Bearer {token}"}

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers=headers)

    if resp.status_code >= 400:
        print("Zoom meeting recordings API error:")
        print("URL:", url)
        print("Status:", resp.status_code)
        print("Body:", resp.text)
        resp.raise_for_status()

    return resp.json()


def _with_access_token_param(download_url: str, access_token: str) -> str:
    """
    Some Zoom recording downloads require access_token as a query param.
    We add it without clobbering existing params.
    """
    parts = urlparse(download_url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    q["access_token"] = access_token
    new_query = urlencode(q)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))


async def stream_recording_bytes(download_url: str):
    """
    Stream bytes from Zoom recording download_url using OAuth token.

    Zoom often returns HTTP 200 with an HTML page (login / error) instead of media.
    If we see HTML/JSON instead of media, retry using ?access_token=... as well.

    Returns (client, resp) where resp supports resp.aiter_bytes().
    """
    token = await get_zoom_access_token()

    # Some Zoom endpoints behave better with a normal UA
    bearer_headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
    }

    client = httpx.AsyncClient(timeout=None, follow_redirects=True)

    async def _is_probably_not_media(resp: httpx.Response) -> bool:
        ct = (resp.headers.get("content-type") or "").lower()

        # If Zoom gives us HTML or JSON, it's almost certainly not the recording bytes
        if "text/html" in ct or "application/json" in ct:
            return True

        # If content-type is missing/odd, sniff first bytes
        try:
            head = resp.content[:256]  # safe; response is already loaded
        except Exception:
            return False

        s = head.lstrip()
        if s.startswith(b"<") or s.startswith(b"<!doctype") or s.startswith(b"<!DOCTYPE"):
            return True
        if s.startswith(b"{") and b"error" in s[:200].lower():
            return True

        return False

    # Attempt 1: Bearer header
    resp = await client.get(download_url, headers=bearer_headers)

    # If 401/403 OR we got HTML/JSON instead of media, retry with access_token param
    if resp.status_code in (401, 403) or await _is_probably_not_media(resp):
        alt = _with_access_token_param(download_url, token)
        resp = await client.get(alt, headers={"User-Agent": "Mozilla/5.0", "Accept": "*/*"})

    # Still bad? Raise with useful preview.
    if resp.status_code >= 400 or await _is_probably_not_media(resp):
        preview = resp.content[:400] if hasattr(resp, "content") else b""
        await client.aclose()
        raise RuntimeError(
            f"Zoom download did not return media. status={resp.status_code} "
            f"content-type={resp.headers.get('content-type')} preview={preview!r}"
        )

    return client, resp
