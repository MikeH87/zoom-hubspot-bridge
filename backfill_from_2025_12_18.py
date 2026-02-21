import os
import json
import datetime
import httpx

from dotenv import load_dotenv

from zoom_client import get_zoom_access_token
from hubspot_client import (
    search_meeting_by_zoom_id,
    get_meeting_contact_ids,
    get_meeting_deal_ids,
    get_latest_deal,
    get_latest_deal_from_contacts,
)

ZOOM_API = "https://api.zoom.us/v2"
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "https://zoom-hubspot-bridge.onrender.com/zoom/recording-webhook")

# Force IPv4 (avoids occasional Windows IPv6 routing weirdness)
TRANSPORT = httpx.AsyncHTTPTransport(local_address="0.0.0.0")

FROM_DATE = os.getenv("FROM_DATE", "2025-12-18")
TO_DATE = os.getenv("TO_DATE", datetime.date.today().isoformat())
DRY_RUN = os.getenv("DRY_RUN", "1") == "1"
ONLY_CHANGES = os.getenv("ONLY_CHANGES", "1") == "1"

# Deal stages
DEAL_STAGE_QUALIFIED = "1054943518"  # Qualified (Zoom call arranged)
DEAL_STAGE_FOLLOWUP = "1054943519"   # Followup (Zoom call completed)
DEAL_STAGE_SENT_CLIENT_AGREEMENT = "1054943520"
DEAL_STAGE_AGREEMENT_SIGNED = "1054943521"
DEAL_STAGE_CLOSED_LOST = "1054943524"
DEAL_STAGE_CANCELLED = "1080046258"

PROTECTED_DEAL_STAGES = {
    DEAL_STAGE_SENT_CLIENT_AGREEMENT,
    DEAL_STAGE_AGREEMENT_SIGNED,
    DEAL_STAGE_CANCELLED,
}

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
    return pick.get("download_url") or pick.get("play_url")


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
                        "download_url": recording_url,
                    }
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


def _stage_from_deal(deal: dict | None) -> str | None:
    if not deal:
        return None
    props = deal.get("properties") or {}
    return props.get("dealstage")


async def preview_actions_for_meeting(meeting: dict) -> dict:
    meeting_id = str(meeting.get("id") or "")
    start_time = meeting.get("start_time")

    hs_meeting = await search_meeting_by_zoom_id(meeting_id)
    if not hs_meeting:
        return {
            "zoom_meeting_id": meeting_id,
            "start_time": start_time,
            "action": "no_hubspot_meeting",
        }

    hs_meeting_id = str(hs_meeting.get("id") or "")
    contact_ids = await get_meeting_contact_ids(hs_meeting_id)
    deal_ids = await get_meeting_deal_ids(hs_meeting_id)

    latest_meeting_deal = await get_latest_deal(deal_ids)
    if latest_meeting_deal:
        deal_id = str(latest_meeting_deal.get("id") or "")
        stage = _stage_from_deal(latest_meeting_deal)

        if stage in PROTECTED_DEAL_STAGES:
            return {
                "zoom_meeting_id": meeting_id,
                "start_time": start_time,
                "hubspot_meeting_id": hs_meeting_id,
                "deal_id": deal_id,
                "deal_stage": stage,
                "action": "skip_protected_stage",
            }
        if stage == DEAL_STAGE_QUALIFIED:
            return {
                "zoom_meeting_id": meeting_id,
                "start_time": start_time,
                "hubspot_meeting_id": hs_meeting_id,
                "deal_id": deal_id,
                "deal_stage": stage,
                "action": "update_stage",
                "to_stage": DEAL_STAGE_FOLLOWUP,
                "via": "meeting_deal",
            }
        return {
            "zoom_meeting_id": meeting_id,
            "start_time": start_time,
            "hubspot_meeting_id": hs_meeting_id,
            "deal_id": deal_id,
            "deal_stage": stage,
            "action": "skip_not_eligible",
        }

    # Fallback: latest deal from contact associations
    contact_deal = await get_latest_deal_from_contacts(contact_ids)
    if not contact_deal:
        return {
            "zoom_meeting_id": meeting_id,
            "start_time": start_time,
            "hubspot_meeting_id": hs_meeting_id,
            "action": "no_deal_found",
        }

    deal_id = str(contact_deal.get("id") or "")
    stage = _stage_from_deal(contact_deal)

    if stage in PROTECTED_DEAL_STAGES:
        return {
            "zoom_meeting_id": meeting_id,
            "start_time": start_time,
            "hubspot_meeting_id": hs_meeting_id,
            "deal_id": deal_id,
            "deal_stage": stage,
            "action": "associate_only_protected_stage",
            "associate_call": True,
            "associate_meeting": True,
            "via": "contact_deal",
        }

    if stage in {DEAL_STAGE_QUALIFIED, DEAL_STAGE_CLOSED_LOST}:
        return {
            "zoom_meeting_id": meeting_id,
            "start_time": start_time,
            "hubspot_meeting_id": hs_meeting_id,
            "deal_id": deal_id,
            "deal_stage": stage,
            "action": "associate_and_update_stage",
            "associate_call": True,
            "associate_meeting": True,
            "to_stage": DEAL_STAGE_FOLLOWUP,
            "via": "contact_deal",
        }

    return {
        "zoom_meeting_id": meeting_id,
        "start_time": start_time,
        "hubspot_meeting_id": hs_meeting_id,
        "deal_id": deal_id,
        "deal_stage": stage,
        "action": "associate_only_not_eligible",
        "associate_call": True,
        "associate_meeting": True,
        "via": "contact_deal",
    }


async def main():
    load_dotenv()
    print(f"Backfill date range: {FROM_DATE} to {TO_DATE}")
    print(f"DRY_RUN={DRY_RUN} ONLY_CHANGES={ONLY_CHANGES}")

    users = await list_users()
    print(f"Found {len(users)} users")

    all_meetings = []
    seen = set()

    for u in users:
        user_id = u.get("id")
        if not user_id:
            continue

        meetings = await list_user_recordings(user_id, FROM_DATE, TO_DATE)
        for m in meetings:
            key = (m.get("uuid") or "", str(m.get("id") or ""))
            if key in seen:
                continue
            seen.add(key)
            all_meetings.append(m)

    print(f"Found {len(all_meetings)} unique recordings for {FROM_DATE}..{TO_DATE}")

    results = []
    for m in all_meetings:
        try:
            if DRY_RUN:
                res = await preview_actions_for_meeting(m)
                if ONLY_CHANGES:
                    action = res.get("action")
                    if action not in {
                        "update_stage",
                        "associate_and_update_stage",
                        "associate_only_protected_stage",
                        "associate_only_not_eligible",
                    }:
                        continue
            else:
                res = await post_to_webhook(m)
        except Exception as e:
            res = {"error": repr(e), "zoom_meeting_id": str(m.get("id") or "")}
        results.append(res)

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
