import os
import json
import datetime
import httpx

from zoom_client import get_zoom_access_token

ZOOM_API = "https://api.zoom.us/v2"
WEBHOOK_URL = "https://zoom-hubspot-bridge.onrender.com/zoom/recording-webhook"

# Europe/London "yesterday" (your timezone)
YESTERDAY = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()

DRY_RUN = os.getenv("DRY_RUN", "1") == "1"


def pick_recording_url(recording_files: list[dict]) -> str | None:
    if not recording_files:
        return None
    # Prefer audio-only if present
    for rf in recording_files:
        if rf.get("recording_type") == "audio_only":
            return rf.get("download_url") or rf.get("play_url")
    first = recording_files[0]
    return first.get("download_url") or first.get("play_url")


async def list_cloud_recordings(from_date: str, to_date: str) -> list[dict]:
    token = await get_zoom_access_token()
    headers = {"Authorization": f"Bearer {token}"}

    # /users/me/recordings gives meetings with recording_files etc.
    url = f"{ZOOM_API}/users/me/recordings?from={from_date}&to={to_date}&page_size=300"

    async with httpx.AsyncClient(timeout=60.0, transport=httpx.AsyncHTTPTransport(local_address="0.0.0.0")) as client:
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()

    return data.get("meetings") or []


async def post_to_webhook(meeting: dict) -> dict:
    meeting_id = meeting.get("id")
    meeting_uuid = meeting.get("uuid")
    start_time = meeting.get("start_time")
    duration = meeting.get("duration")  # minutes
    recording_files = meeting.get("recording_files") or []

    recording_url = pick_recording_url(recording_files)
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
                        "id": recording_files[0].get("id", "file-1") if recording_files else "file-1",
                        "recording_type": "audio_only",
                "file_extension": "MP4",
                    }
                ],
            }
        },
    }

    if DRY_RUN:
        return {"dry_run": True, "zoom_meeting_id": str(meeting_id), "start_time": start_time}

    async with httpx.AsyncClient(timeout=60.0, transport=httpx.AsyncHTTPTransport(local_address="0.0.0.0")) as client:
        resp = await client.post(WEBHOOK_URL, json=payload)
        # Always capture body for visibility
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        return {"status_code": resp.status_code, "body": body, "zoom_meeting_id": str(meeting_id)}


async def main():
    print(f"Catch-up date range: {YESTERDAY} to {YESTERDAY}")
    print(f"DRY_RUN={DRY_RUN}")

    meetings = await list_cloud_recordings(YESTERDAY, YESTERDAY)
    print(f"Found {len(meetings)} Zoom recordings for {YESTERDAY}")

    results = []
    for m in meetings:
        try:
            res = await post_to_webhook(m)
        except Exception as e:
            res = {"error": repr(e), "zoom_meeting_id": str(m.get("id") or "")}
        results.append(res)

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())


