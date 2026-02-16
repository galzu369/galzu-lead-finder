import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


@dataclass(frozen=True)
class DbPaths:
    base_dir: Path

    @property
    def db_path(self) -> Path:
        return self.base_dir / "data" / "galzu_leads.sqlite"


def _now_unix() -> int:
    return int(time.time())


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # Better concurrency characteristics for a local app.
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def migrate(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          started_at INTEGER NOT NULL,
          ended_at INTEGER,
          status TEXT NOT NULL,
          params_json TEXT NOT NULL,
          output_csv_path TEXT,
          error TEXT
        );

        CREATE TABLE IF NOT EXISTS leads (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT NOT NULL DEFAULT 'x',
          handle TEXT NOT NULL,
          profile_url TEXT,
          name TEXT,
          bio TEXT,
          followers INTEGER,
          location TEXT,
          website TEXT,
          phone TEXT,
          email TEXT,
          recent_post_snippet TEXT,
          signal_keywords_matched TEXT,
          score INTEGER,
          reason TEXT,
          website_score INTEGER,
          website_verdict TEXT,
          website_findings TEXT,
          website_checked_at INTEGER,
          website_final_url TEXT,
          website_http_status INTEGER,
          status TEXT NOT NULL DEFAULT 'new',
          notes TEXT NOT NULL DEFAULT '',
          tags TEXT NOT NULL DEFAULT '[]',
          last_seen_at INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          UNIQUE(source, handle)
        );
        """
    )
    _ensure_columns(
        conn,
        "leads",
        [
            ("profile_url", "TEXT", None),
            ("phone", "TEXT", None),
            ("website_score", "INTEGER", None),
            ("website_verdict", "TEXT", None),
            ("website_findings", "TEXT", None),
            ("website_checked_at", "INTEGER", None),
            ("website_final_url", "TEXT", None),
            ("website_http_status", "INTEGER", None),
        ],
    )
    _ensure_unique_source_handle(conn)
    conn.commit()


def create_run(conn: sqlite3.Connection, params: Dict[str, Any]) -> int:
    cur = conn.execute(
        "INSERT INTO runs (started_at, status, params_json) VALUES (?, ?, ?)",
        (_now_unix(), "running", json.dumps(params, ensure_ascii=True)),
    )
    conn.commit()
    return int(cur.lastrowid)


def finish_run(conn: sqlite3.Connection, run_id: int, *, status: str, output_csv_path: Optional[str] = None, error: str = "") -> None:
    conn.execute(
        "UPDATE runs SET ended_at=?, status=?, output_csv_path=?, error=? WHERE id=?",
        (_now_unix(), status, output_csv_path or None, error or "", run_id),
    )
    conn.commit()


def get_run(conn: sqlite3.Connection, run_id: int) -> Optional[Dict[str, Any]]:
    row = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
    if not row:
        return None
    d = dict(row)
    try:
        d["params"] = json.loads(d.get("params_json") or "{}")
    except Exception:
        d["params"] = {}
    return d


def upsert_leads_from_rows(
    conn: sqlite3.Connection,
    rows: Iterable[Dict[str, Any]],
    *,
    source: str,
    compute_score_if_missing: bool = False,
    score_fn=None,
) -> int:
    """
    Upsert by handle. Preserve user-entered fields (status/notes/tags).
    Uses batch processing for efficiency.
    """
    now = _now_unix()
    n = 0
    # Convert to list if needed for scoring (but keep memory-efficient for large batches)
    rows_list = list(rows) if not isinstance(rows, list) else rows
    
    for r in rows_list:
        src = (source or "manual").strip().lower()
        r2 = _normalize_row(r, source=src)
        handle = (r2.get("handle") or "").strip().lstrip("@")
        if not handle:
            continue
        profile_url = (r2.get("profile_url") or "").strip()

        if compute_score_if_missing and (_to_int(r2.get("score")) is None) and score_fn is not None:
            try:
                s, reasons = score_fn(r2)
                r2["score"] = s
                r2["reason"] = " | ".join(reasons[:18])
            except Exception:
                pass

        # First insert if missing.
        conn.execute(
            """
            INSERT INTO leads (
              source, handle, profile_url, name, bio, followers, location, website, phone, email,
              recent_post_snippet, signal_keywords_matched, score, reason,
              last_seen_at, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source, handle) DO UPDATE SET
              profile_url=COALESCE(NULLIF(excluded.profile_url, ''), leads.profile_url),
              name=excluded.name,
              bio=excluded.bio,
              followers=excluded.followers,
              location=excluded.location,
              website=COALESCE(NULLIF(excluded.website, ''), leads.website),
              phone=COALESCE(NULLIF(excluded.phone, ''), leads.phone),
              email=COALESCE(NULLIF(excluded.email, ''), leads.email),
              recent_post_snippet=excluded.recent_post_snippet,
              signal_keywords_matched=excluded.signal_keywords_matched,
              score=excluded.score,
              reason=excluded.reason,
              last_seen_at=excluded.last_seen_at
            """,
            (
                src,
                handle,
                profile_url or None,
                (r2.get("name") or "").strip(),
                (r2.get("bio") or "").strip(),
                _to_int(r2.get("followers")),
                (r2.get("location") or "").strip(),
                (r2.get("website") or "").strip(),
                (r2.get("phone") or "").strip(),
                (r2.get("email") or "").strip(),
                (r2.get("recent_post_snippet") or "").strip(),
                (r2.get("signal_keywords_matched") or "").strip(),
                _to_int(r2.get("score")),
                (r2.get("reason") or "").strip(),
                now,
                now,
            ),
        )
        n += 1

    conn.commit()
    return n


def list_leads(
    conn: sqlite3.Connection,
    *,
    q: str = "",
    status: str = "",
    source: str = "",
    min_score: Optional[int] = None,
    website_verdict: str = "",
    max_website_score: Optional[int] = None,
    limit: int = 200,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    where = ["1=1"]
    params: List[Any] = []

    if q.strip():
        where.append("(handle LIKE ? OR name LIKE ? OR bio LIKE ? OR location LIKE ? OR website LIKE ? OR profile_url LIKE ? OR phone LIKE ? OR email LIKE ?)")
        needle = f"%{q.strip()}%"
        params += [needle, needle, needle, needle, needle, needle, needle, needle]
    if status.strip():
        where.append("status = ?")
        params.append(status.strip())
    if source.strip():
        where.append("source = ?")
        params.append(source.strip())
    if min_score is not None:
        where.append("score >= ?")
        params.append(int(min_score))
    if website_verdict.strip():
        where.append("website_verdict = ?")
        params.append(website_verdict.strip())
    if max_website_score is not None:
        where.append("website_score <= ?")
        params.append(int(max_website_score))

    # Safe: where clauses are hardcoded strings, params are bound separately
    sql = f"""
      SELECT *
      FROM leads
      WHERE {' AND '.join(where)}
      ORDER BY score DESC, followers DESC, last_seen_at DESC
      LIMIT ? OFFSET ?
    """
    params += [int(limit), int(offset)]
    rows = conn.execute(sql, params).fetchall()
    out = []
    for row in rows:
        d = dict(row)
        try:
            d["tags"] = json.loads(d.get("tags") or "[]")
        except Exception:
            d["tags"] = []
        out.append(d)
    return out


def update_lead(conn: sqlite3.Connection, lead_id: int, patch: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    allowed = {"status", "notes", "tags"}
    fields = []
    params: List[Any] = []
    for k, v in patch.items():
        if k not in allowed:
            continue
        if k == "tags":
            fields.append("tags=?")
            params.append(json.dumps(v or [], ensure_ascii=True))
        else:
            fields.append(f"{k}=?")
            params.append(v)
    if not fields:
        return get_lead(conn, lead_id)
    params.append(int(lead_id))
    # Safe: fields are validated against allowed set, params are bound separately
    conn.execute(f"UPDATE leads SET {', '.join(fields)} WHERE id=?", params)
    conn.commit()
    return get_lead(conn, lead_id)


def get_lead(conn: sqlite3.Connection, lead_id: int) -> Optional[Dict[str, Any]]:
    row = conn.execute("SELECT * FROM leads WHERE id=?", (int(lead_id),)).fetchone()
    if not row:
        return None
    d = dict(row)
    try:
        d["tags"] = json.loads(d.get("tags") or "[]")
    except Exception:
        d["tags"] = []
    return d


def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, int):
        return v
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def status_counts(conn: sqlite3.Connection, *, source: str = "") -> Dict[str, int]:
    where = ["1=1"]
    params: List[Any] = []
    if source.strip():
        where.append("source = ?")
        params.append(source.strip())
    # Safe: where clauses are hardcoded strings, params are bound separately
    rows = conn.execute(
        f"SELECT status, COUNT(*) AS c FROM leads WHERE {' AND '.join(where)} GROUP BY status",
        params,
    ).fetchall()
    out: Dict[str, int] = {}
    for r in rows:
        out[str(r["status"])] = int(r["c"])
    return out


def leads_needing_website_audit(conn: sqlite3.Connection, *, max_sites: int) -> List[Dict[str, Any]]:
    # Prioritize higher-scoring leads first.
    rows = conn.execute(
        """
        SELECT id, handle, website, website_checked_at
        FROM leads
        WHERE website_checked_at IS NULL
        ORDER BY score DESC, followers DESC, last_seen_at DESC
        LIMIT ?
        """,
        (int(max_sites),),
    ).fetchall()
    return [dict(r) for r in rows]


def update_website_audit(conn: sqlite3.Connection, lead_id: int, *, result: Dict[str, Any]) -> None:
    conn.execute(
        """
        UPDATE leads
        SET website_score=?,
            website_verdict=?,
            website_findings=?,
            website_checked_at=?,
            website_final_url=?,
            website_http_status=?
        WHERE id=?
        """,
        (
            _to_int(result.get("website_score")),
            (result.get("website_verdict") or "").strip(),
            (result.get("website_findings") or "").strip(),
            _to_int(result.get("website_checked_at")),
            (result.get("website_final_url") or "").strip(),
            _to_int(result.get("website_http_status")),
            int(lead_id),
        ),
    )
    conn.commit()


def _ensure_columns(conn: sqlite3.Connection, table: str, cols: List[tuple]) -> None:
    """
    Ensure columns exist on an existing SQLite table.
    cols = [(name, type_sql, default_sql_or_None), ...]
    """
    existing = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, type_sql, default_sql in cols:
        if name in existing:
            continue
        if default_sql is None:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {type_sql}")
        else:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {type_sql} DEFAULT {default_sql}")


def _ensure_unique_source_handle(conn: sqlite3.Connection) -> None:
    """
    If an existing DB was created with UNIQUE(handle), rebuild table to UNIQUE(source, handle).
    This prevents collisions between platforms (same handle on X and IG, etc).
    """
    try:
        # Look for a UNIQUE index that covers (source, handle)
        for idx in conn.execute("PRAGMA index_list(leads)").fetchall():
            if int(idx["unique"]) != 1:
                continue
            idx_name = idx["name"]
            cols = [r["name"] for r in conn.execute(f"PRAGMA index_info({idx_name})").fetchall()]
            if cols == ["source", "handle"] or cols == ["handle", "source"]:
                return
    except Exception:
        # If we can't inspect indices, don't attempt a rebuild.
        return

    # Rebuild table (small local DB; safe and fast).
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS leads_new (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT NOT NULL DEFAULT 'x',
          handle TEXT NOT NULL,
          profile_url TEXT,
          name TEXT,
          bio TEXT,
          followers INTEGER,
          location TEXT,
          website TEXT,
          phone TEXT,
          email TEXT,
          recent_post_snippet TEXT,
          signal_keywords_matched TEXT,
          score INTEGER,
          reason TEXT,
          website_score INTEGER,
          website_verdict TEXT,
          website_findings TEXT,
          website_checked_at INTEGER,
          website_final_url TEXT,
          website_http_status INTEGER,
          status TEXT NOT NULL DEFAULT 'new',
          notes TEXT NOT NULL DEFAULT '',
          tags TEXT NOT NULL DEFAULT '[]',
          last_seen_at INTEGER NOT NULL,
          created_at INTEGER NOT NULL,
          UNIQUE(source, handle)
        );
        """
    )

    old_cols = {r["name"] for r in conn.execute("PRAGMA table_info(leads)").fetchall()}
    select_source = "source" if "source" in old_cols else "'x' AS source"
    select_profile = "profile_url" if "profile_url" in old_cols else "NULL AS profile_url"
    select_phone = "phone" if "phone" in old_cols else "NULL AS phone"

    conn.execute(
        f"""
        INSERT INTO leads_new (
          id, source, handle, profile_url, name, bio, followers, location, website, phone, email,
          recent_post_snippet, signal_keywords_matched, score, reason,
          website_score, website_verdict, website_findings, website_checked_at, website_final_url, website_http_status,
          status, notes, tags, last_seen_at, created_at
        )
        SELECT
          id,
          {select_source},
          handle,
          {select_profile},
          name, bio, followers, location, website, {select_phone}, email,
          recent_post_snippet, signal_keywords_matched, score, reason,
          website_score, website_verdict, website_findings, website_checked_at, website_final_url, website_http_status,
          status, notes, tags, last_seen_at, created_at
        FROM leads
        """
    )
    conn.executescript(
        """
        DROP TABLE leads;
        ALTER TABLE leads_new RENAME TO leads;
        """
    )


def _normalize_row(r: Dict[str, Any], *, source: str) -> Dict[str, Any]:
    """
    Best-effort normalization across X / Instagram / Facebook / manual CSVs.
    """
    # CSV headers frequently vary in casing (ex: "Phone" vs "phone").
    # Normalize keys to lowercase for consistent access.
    out = {str(k).strip().lower(): v for k, v in (r or {}).items()}

    handle = (out.get("handle") or out.get("username") or out.get("user") or out.get("page") or "").strip()
    handle = handle.lstrip("@").strip()

    profile_url = (out.get("profile_url") or out.get("profile") or out.get("profilelink") or out.get("profile_link") or "").strip()
    if not profile_url:
        # Some exports use `url` as profile URL; if it looks like a platform URL, treat as profile_url.
        u = (out.get("url") or "").strip()
        if ("instagram.com/" in u) or ("facebook.com/" in u):
            profile_url = u

    if profile_url and not handle:
        try:
            p = str(profile_url)
            if "instagram.com/" in p:
                handle = p.split("instagram.com/", 1)[1].split("?", 1)[0].strip("/").split("/", 1)[0]
            elif "facebook.com/" in p:
                handle = p.split("facebook.com/", 1)[1].split("?", 1)[0].strip("/").split("/", 1)[0]
        except Exception:
            pass

    # Local business imports (Google Maps) don't have a "username".
    # Use the profile URL as a stable unique key when available.
    if not handle and source.strip().lower() in {"google_maps", "gmb", "maps", "local"}:
        handle = (profile_url or out.get("name") or "").strip()

    out["handle"] = handle
    out["profile_url"] = profile_url

    if not out.get("bio"):
        out["bio"] = out.get("description") or out.get("about") or ""
    if not out.get("website"):
        out["website"] = out.get("website_url") or out.get("site") or ""
    if not out.get("phone"):
        out["phone"] = out.get("phone_number") or out.get("tel") or out.get("mobile") or ""

    # Nudge obvious columns into common names.
    if not out.get("recent_post_snippet"):
        out["recent_post_snippet"] = out.get("snippet") or out.get("caption") or out.get("post_text") or ""

    return out

