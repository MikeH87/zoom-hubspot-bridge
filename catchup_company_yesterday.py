import os
import json
import datetime
import httpx

from zoom_client import get_zoom_access_token

ZOOM_API = "https://api.zoom.us/v2"
WEBHOOK_URL = "https://zoom-hubspot-bridge.onrender.com/zoom/recording-webhook"

YESTERDAY = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
DRY_RUN = os.getenv("DRY_RUN", "1") == "1"

ALLOWED_MEDIA_FILE_TYPES = {"M4A", "MP4", "MP3", "WAV"}

def choose_media_download_url(recording_files):
    """
    - Only allow M4A/MP4/MP3/WAV
    - Prefer audio_only first (and M4A if present)
    - Otherwise choose smallest MP4 by file_size
    """
    if not recording_files:
        return None

    media = []
    for rf in recording_files:
        ft = (rf.get("file_type") or "").upper().strip()
        if ft in ALLOWED_MEDIA_FILE_TYPES:
            media.append(rf)

    if not media:
        return None

    audio_only = [rf for rf in media if rf.get("recording_type") == "audio_only"]
    if audio_only:
        m4a = [rf for rf in audio_only if (rf.get("file_type") or "").upper() == "M4A"]
        pick = m4a[0] if m4a else audio_only[0]
        return pick.get("download_url") or pick.get("play_url")

    mp4s = [rf for rf in media if (rf.get("file_type") or "").upper() == "MP4"]
    if mp4s:
        def size_or_big(x):
            try:
                return int(x.get("file_size") or 10**18)
            except Exception:
                return 10**18
        mp4s.sort(key=size_or_big)
        pick = mp4s[0]
        return pick.get("download_url") or pick.get("play_url")

    pick = media[0]
    return pick.get("download_url") or pick.get("play_url")# Force IPv4 (avoids occasional Windows IPv6 routing weirdness)
TRANSPORT = httpx.AsyncHTTPTransport(local_address="0.0.0.0")



async def list_users():
    token = await get_zoom_access_token()
    headers = {"Authorization": f"Bearer {token}"}

    users = []
    next_token = None

    async with httpx.AsyncClient(timeout=60.0, transport=TRANSPORT) as client:
        while True:
            url = f"{ZOOM_API}/users?page_size=300"
            if next_token:
                url += f"&next_page_token={next_token}"

            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            data = resp.json()

            users.extend(data.get("users") or [])
            next_token = data.get("next_page_token") or None
            if not next_token:
                break

    return users


async def list_user_recordings(user_id: str, from_date: str, to_date: str):
    token = await get_zoom_access_token()
    headers = {"Authorization": f"Bearer {token}"}

    url = f"{ZOOM_API}/users/{user_id}/recordings?from={from_date}&to={to_date}&page_size=300"

    async with httpx.AsyncClient(timeout=60.0, transport=TRANSPORT) as client:
        resp = await client.get(url, headers=headers)

    # If a user has no recordings or no permission, skip gracefully
    if resp.status_code == 404:
        return []
    if resp.status_code >= 400:
        print(f"Recordings API error for user {user_id}: {resp.status_code} {resp.text}")
        return []

    data = resp.json()
    return data.get("meetings") or []


async def post_to_webhook(meeting: dict):
    meeting_id = meeting.get("id")
    meeting_uuid = meeting.get("uuid")
    start_time = meeting.get("start_time")
    duration = meeting.get("duration")
    recording_files = meeting.get("recording_files") or []

    recording_url = choose_media_download_url(recording_files)
    if not meeting_id or not recording_url:
        return {"skipped": True, "reason": "missing meeting_id or recording_url"}

    payload = {
        "event": "recording.completed",
        "payload": {
            "object": {
                "id": meeting_id,
                "uuid": meeting_uuid,
                "start_time": start_time,
                "duration": duration,
                "recording_files": [
                    {
                        "id": (recording_files[0].get("id") if recording_files else "file-1"),
                        "recording_type": "audio_only",
                "file_type": "MP4",
          "download_url": recording_url,}
                ],
            }
        },
    }

    if DRY_RUN:
        return {"dry_run": True, "zoom_meeting_id": str(meeting_id), "start_time": start_time}

    async with httpx.AsyncClient(timeout=60.0, transport=TRANSPORT) as client:
        resp = await client.post(WEBHOOK_URL, json=payload)
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        return {"status_code": resp.status_code, "body": body, "zoom_meeting_id": str(meeting_id)}


async def main():
    print(f"Company catch-up date range: {YESTERDAY} to {YESTERDAY}")
    print(f"DRY_RUN={DRY_RUN}")

    users = await list_users()
    print(f"Found {len(users)} users")

    # Gather recordings across all users, de-dupe by meeting UUID (or meeting id if UUID missing)
    all_meetings = []
    seen = set()

    for u in users:
        user_id = u.get("id")
        if not user_id:
            continue

        meetings = await list_user_recordings(user_id, YESTERDAY, YESTERDAY)
        for m in meetings:
            key = (m.get("uuid") or "", str(m.get("id") or ""))
            if key in seen:
                continue
            seen.add(key)
            all_meetings.append(m)

    print(f"Found {len(all_meetings)} unique recordings for {YESTERDAY}")

    results = []
    for m in all_meetings:
        try:
            res = await post_to_webhook(m)
        except Exception as e:
            res = {"error": repr(e), "zoom_meeting_id": str(m.get("id") or "")}
        results.append(res)

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())


