import json
import time
import urllib.parse
import urllib.request
import urllib.error
from typing import Any, Dict, List, Optional


class MetaApiError(RuntimeError):
    pass


def _now() -> float:
    return time.time()


def _sleep_s(attempt: int, base: float = 1.0, cap: float = 20.0) -> float:
    # Simple backoff: 1, 2, 4, 8... capped.
    return min(cap, base * (2**attempt))


def graph_get(
    path: str,
    *,
    access_token: str,
    params: Optional[Dict[str, Any]] = None,
    api_version: str = "v24.0",
    max_retries: int = 4,
    timeout_s: float = 30.0,
) -> Dict[str, Any]:
    """
    Minimal Graph API GET with retries.
    Path examples:
      - "me/accounts"
      - "178414.../media"
      - "3710...?fields=..."
    """
    if not access_token:
        raise MetaApiError("Missing Meta access token.")

    path = path.lstrip("/")
    base = f"https://graph.facebook.com/{api_version}/"
    url = base + path
    q = dict(params or {})
    q["access_token"] = access_token
    url = url + ("&" if "?" in url else "?") + urllib.parse.urlencode(q, doseq=True)

    last_err: Optional[Exception] = None
    for attempt in range(max_retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "galzu-lead-finder/1.0"})
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                raw = resp.read()
            payload = json.loads(raw.decode("utf-8"))
            if isinstance(payload, dict) and payload.get("error"):
                raise MetaApiError(str(payload["error"].get("message") or payload["error"]))
            if not isinstance(payload, dict):
                raise MetaApiError("Unexpected response from Meta API.")
            return payload
        except urllib.error.HTTPError as e:
            last_err = e
            # Meta uses 400 for many oauth/permission errors; do not retry those.
            if 400 <= e.code < 500 and e.code != 429:
                try:
                    raw = e.read()
                    payload = json.loads(raw.decode("utf-8", errors="replace"))
                    if isinstance(payload, dict) and payload.get("error"):
                        raise MetaApiError(str(payload["error"].get("message") or payload["error"]))
                except Exception:
                    pass
                raise MetaApiError(f"Meta API HTTP error {e.code}")
            time.sleep(_sleep_s(attempt))
            continue
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(_sleep_s(attempt))
            continue

    raise MetaApiError(str(last_err) if last_err else "Meta API request failed.")


def get_ig_media(ig_user_id: str, *, access_token: str, limit: int = 10) -> List[Dict[str, Any]]:
    payload = graph_get(
        f"{ig_user_id}/media",
        access_token=access_token,
        params={"fields": "id,caption,permalink,timestamp", "limit": int(limit)},
    )
    return list(payload.get("data") or [])


def get_media_comments(media_id: str, *, access_token: str, limit: int = 50) -> List[Dict[str, Any]]:
    payload = graph_get(
        f"{media_id}/comments",
        access_token=access_token,
        params={"fields": "id,text,username,timestamp", "limit": int(limit)},
    )
    return list(payload.get("data") or [])


def business_discovery(
    ig_user_id: str,
    *,
    access_token: str,
    username: str,
) -> Optional[Dict[str, Any]]:
    """
    Enrich a username via Business Discovery (only works for Business/Creator accounts, and needs permissions).
    """
    u = (username or "").strip().lstrip("@")
    if not u:
        return None

    fields = "name,username,biography,website,followers_count,media_count,profile_picture_url"
    # Graph format: /{ig_user_id}?fields=business_discovery.username(USER){FIELDS}
    payload = graph_get(
        f"{ig_user_id}",
        access_token=access_token,
        params={"fields": f"business_discovery.username({u}){{{fields}}}"},
    )
    bd = payload.get("business_discovery")
    if isinstance(bd, dict):
        return bd
    return None

