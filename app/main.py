import threading
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from . import db
from . import audit
from . import env
from . import meta_api
from . import scoring
from . import runner
from . import maps_scraper


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATHS = db.DbPaths(base_dir=BASE_DIR)

app = FastAPI(title="Galzu Lead Finder", version="0.1.0")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

_conn = db.connect(DB_PATHS.db_path)
db.migrate(_conn)

_lock = threading.Lock()
_maps_lock = threading.Lock()

# Load dashboard/env vars (Meta + X) on startup.
env.load_dashboard_env()


def _ingest_existing_ranked_csv() -> None:
    # If the user ran the script before opening the dashboard, pull it in.
    root = runner.workspace_root(Path(__file__).resolve().parent)
    csv_path = root / "ranked_leads.csv"
    if not csv_path.exists():
        return
    try:
        rows = runner.read_ranked_csv(csv_path)
        n = db.upsert_leads_from_rows(_conn, rows, source="x", compute_score_if_missing=False)
        print(f"[galzu-lead-finder] Ingested ranked_leads.csv -> {n} leads")
    except Exception as e:  # noqa: BLE001
        # Keep startup resilient; user can re-run from UI.
        print(f"[galzu-lead-finder] Ingest failed: {e}")
        return


_ingest_existing_ranked_csv()


def _run_job(run_id: int, params: Dict[str, Any]) -> None:
    try:
        out_csv, meta = runner.run_discover_and_score(dashboard_app_dir=Path(__file__).resolve().parent, **params)
        rows = runner.read_ranked_csv(out_csv)
        ingested = db.upsert_leads_from_rows(_conn, rows, source="x", compute_score_if_missing=False)
        db.finish_run(_conn, run_id, status="ok", output_csv_path=str(out_csv), error="")
        # Store a small summary in run error field? keep empty on success.
        _ = meta, ingested
    except Exception as e:  # noqa: BLE001
        db.finish_run(_conn, run_id, status="error", output_csv_path=None, error=str(e))


def _audit_job(run_id: int, params: Dict[str, Any]) -> None:
    try:
        max_sites = int(params.get("max_sites", 25))
        cfg = audit.AuditConfig(
            timeout_s=float(params.get("timeout_s", 10.0)),
            max_bytes=int(params.get("max_bytes", 450_000)),
            sleep_s=float(params.get("sleep_s", 1.0)),
        )
        candidates = db.leads_needing_website_audit(_conn, max_sites=max_sites)
        id_urls = [(int(r["id"]), str(r.get("website") or "")) for r in candidates]
        results = audit.audit_leads(id_urls, cfg)
        for lead_id, result in results:
            db.update_website_audit(_conn, lead_id, result=result)
        db.finish_run(_conn, run_id, status="ok", output_csv_path=None, error="")
    except Exception as e:  # noqa: BLE001
        db.finish_run(_conn, run_id, status="error", output_csv_path=None, error=str(e))


def _maps_job(run_id: int, params: Dict[str, Any]) -> None:
    """
    Scrape Google Maps (headful Playwright) and import into DB as source=google_maps.
    """
    try:
        niche = str(params.get("niche") or "").strip()
        location = str(params.get("location") or "").strip()
        max_results = int(params.get("max_results", 30))
        headful = bool(params.get("headful", True))

        if not niche or not location:
            raise RuntimeError("Missing niche or location.")

        query = f"{niche} in {location}"
        # Time budget: keep it tight so it never feels like it's looping.
        max_total_s = min(90.0 + 18.0 * max_results, 420.0)
        cfg = maps_scraper.MapsScrapeConfig(query=query, max_results=max_results, headful=headful, max_total_s=max_total_s)
        leads = maps_scraper.scrape_google_maps(cfg, base_dir=BASE_DIR)

        rows = []
        for l in leads:
            rows.append(
                {
                    "name": l.get("name") or "",
                    "profile_url": l.get("profile_url") or "",
                    "website": l.get("website") or "",
                    "phone": l.get("phone") or "",
                    "email": "",
                    "location": location,
                    "signal_keywords_matched": "google_maps",
                    # handle fallback logic for google_maps is in db._normalize_row
                }
            )

        # Enrich email from website when possible (fetch site, regex email); limit to avoid long runs.
        need_email = [r for r in rows if (r.get("website") or "").strip() and not (r.get("email") or "").strip()]
        for r in need_email[:15]:
            try:
                email = maps_scraper.enrich_email_from_website(
                    (r.get("website") or "").strip(),
                    timeout_s=8.0,
                )
                if email:
                    r["email"] = email
            except Exception:  # noqa: BLE001
                pass

        imported = db.upsert_leads_from_rows(
            _conn,
            rows,
            source="google_maps",
            compute_score_if_missing=True,
            score_fn=scoring.score_row,
        )
        # Auto-run website audit for imported sites so the dashboard shows ratings immediately.
        audited = 0
        try:
            # Pick most recently seen google_maps leads with websites.
            cand = _conn.execute(
                """
                SELECT id, website
                FROM leads
                WHERE source='google_maps'
                  AND website IS NOT NULL AND TRIM(website) <> ''
                  AND website_checked_at IS NULL
                ORDER BY last_seen_at DESC
                LIMIT ?
                """,
                (min(25, max_results),),
            ).fetchall()
            id_urls = [(int(r["id"]), str(r["website"])) for r in cand]
            if id_urls:
                cfg_a = audit.AuditConfig(timeout_s=10.0, max_bytes=450_000, sleep_s=0.6)
                results = audit.audit_leads(id_urls, cfg_a)
                for lead_id, result in results:
                    db.update_website_audit(_conn, lead_id, result=result)
                    audited += 1
        except Exception:
            # Non-fatal; keep the lead import successful.
            audited = audited

        db.finish_run(
            _conn,
            run_id,
            status="ok",
            output_csv_path=None,
            error=f"Imported {imported} google_maps leads. Website audits: {audited}.",
        )
    except Exception as e:  # noqa: BLE001
        db.finish_run(_conn, run_id, status="error", output_csv_path=None, error=str(e))
    finally:
        # Ensure we never leave the scraper locked.
        try:
            if _maps_lock.locked():
                _maps_lock.release()
        except Exception:
            pass


def _ig_commenters_job(run_id: int, params: Dict[str, Any]) -> None:
    """
    Pull commenters from recent IG media (Meta API), enrich with Business Discovery when possible,
    score using the same heuristics, and upsert as instagram leads.
    """
    import os

    try:
        token = (params.get("meta_access_token") or os.environ.get("META_ACCESS_TOKEN") or "").strip()
        ig_user_id = (params.get("ig_user_id") or os.environ.get("META_IG_USER_ID") or "").strip()
        if not token:
            raise RuntimeError("Missing META_ACCESS_TOKEN. Put it in galzu-lead-finder-dashboard/.env or pass in request.")
        if not ig_user_id:
            raise RuntimeError("Missing META_IG_USER_ID (your instagram_business_account id).")

        media_limit = int(params.get("media_limit", 10))
        comments_limit = int(params.get("comments_limit", 50))
        max_users = int(params.get("max_users", 150))
        do_enrich = bool(params.get("enrich", True))
        sleep_s = float(params.get("sleep_s", 0.4))

        media = meta_api.get_ig_media(ig_user_id, access_token=token, limit=media_limit)

        by_user: Dict[str, Dict[str, Any]] = {}
        for m in media:
            mid = str(m.get("id") or "")
            if not mid:
                continue
            comments = meta_api.get_media_comments(mid, access_token=token, limit=comments_limit)
            for c in comments:
                u = (c.get("username") or "").strip().lstrip("@")
                if not u:
                    continue
                if u not in by_user:
                    by_user[u] = {
                        "source": "instagram",
                        "handle": u,
                        "profile_url": f"https://www.instagram.com/{u}/",
                        "name": "",
                        "bio": "",
                        "followers": "",
                        "location": "",
                        "website": "",
                        "email": "",
                        "recent_post_snippet": (c.get("text") or "").strip()[:240],
                        "signal_keywords_matched": "ig_commenter",
                    }
                if len(by_user) >= max_users:
                    break
            if len(by_user) >= max_users:
                break

        # Enrich via Business Discovery (only works for Business/Creator accounts).
        if do_enrich:
            for username, row in list(by_user.items()):
                try:
                    bd = meta_api.business_discovery(ig_user_id, access_token=token, username=username)
                    if bd:
                        row["name"] = (bd.get("name") or row.get("name") or "").strip()
                        row["bio"] = (bd.get("biography") or "").strip()
                        row["website"] = (bd.get("website") or "").strip()
                        row["followers"] = bd.get("followers_count") or row.get("followers") or ""
                        # Keep profile_url as instagram.com/<username>/ from our row.
                except Exception:
                    pass
                if sleep_s:
                    import time

                    time.sleep(max(0.0, sleep_s))

        # Score and upsert.
        rows = list(by_user.values())
        ingested = db.upsert_leads_from_rows(
            _conn,
            rows,
            source="instagram",
            compute_score_if_missing=True,
            score_fn=scoring.score_row,
        )
        db.finish_run(_conn, run_id, status="ok", output_csv_path=None, error=f"Imported {ingested} instagram leads.")
    except Exception as e:  # noqa: BLE001
        db.finish_run(_conn, run_id, status="error", output_csv_path=None, error=str(e))


@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/ping")
def api_ping() -> JSONResponse:
    # Lightweight sanity-check for integrations (ex: Chrome extension).
    paths = {getattr(r, "path", "") for r in app.routes}
    return JSONResponse(
        {
            "ok": True,
            "main_file": str(Path(__file__).resolve()),
            "has_import_leads_json": "/api/import/leads-json" in paths,
        }
    )


@app.get("/api/runs/{run_id}")
def api_get_run(run_id: int) -> JSONResponse:
    r = db.get_run(_conn, int(run_id))
    if not r:
        raise HTTPException(status_code=404, detail="Run not found")
    return JSONResponse(r)


@app.post("/api/runs/discover")
def api_run_discover(payload: Dict[str, Any]) -> JSONResponse:
    # Prevent multiple runs at once (keeps the PC responsive + avoids rate limits).
    with _lock:
        # Validate with defaults.
        params = {
            "days": int(payload.get("days", 2)),
            "lang": str(payload.get("lang") or "en"),
            "max_leads": int(payload.get("max_leads", 25)),
            "min_followers": int(payload.get("min_followers", 0)),
            "keywords_file": str(payload.get("keywords_file") or ""),
            "seed_csv": str(payload.get("seed_csv") or ""),
        }
        run_id = db.create_run(_conn, params)

        t = threading.Thread(target=_run_job, args=(run_id, params), daemon=True)
        t.start()
        return JSONResponse({"run_id": run_id, "status": "running", "params": params})


@app.post("/api/runs/audit-websites")
def api_run_audit_websites(payload: Dict[str, Any]) -> JSONResponse:
    with _lock:
        params = {
            "kind": "audit-websites",
            "max_sites": int(payload.get("max_sites", 25)),
            "timeout_s": float(payload.get("timeout_s", 10.0)),
            "sleep_s": float(payload.get("sleep_s", 1.0)),
            "max_bytes": int(payload.get("max_bytes", 450_000)),
        }
        run_id = db.create_run(_conn, params)
        t = threading.Thread(target=_audit_job, args=(run_id, params), daemon=True)
        t.start()
        return JSONResponse({"run_id": run_id, "status": "running", "params": params})


@app.post("/api/runs/maps-scrape")
def api_run_maps_scrape(payload: Dict[str, Any]) -> JSONResponse:
    """
    Scrape Google Maps for local service providers and import into leads.
    """
    # Maps scraping is exclusive: a persistent browser profile cannot run concurrently.
    if not _maps_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="Google Maps scraper is already running. Wait for it to finish.")
    with _lock:
        params = {
            "kind": "maps-scrape",
            "niche": str(payload.get("niche") or "").strip(),
            "location": str(payload.get("location") or "").strip(),
            "max_results": int(payload.get("max_results", 30)),
            "headful": bool(payload.get("headful", True)),
        }
        run_id = db.create_run(_conn, params)
        t = threading.Thread(target=_maps_job, args=(run_id, params), daemon=True)
        t.start()
        return JSONResponse({"run_id": run_id, "status": "running", "params": params})


@app.post("/api/runs/ig-commenters")
def api_run_ig_commenters(payload: Dict[str, Any]) -> JSONResponse:
    with _lock:
        params = {
            "kind": "ig-commenters",
            "ig_user_id": str(payload.get("ig_user_id") or "").strip(),
            # Optional: keep empty to rely on META_ACCESS_TOKEN env var.
            "meta_access_token": str(payload.get("meta_access_token") or "").strip(),
            "media_limit": int(payload.get("media_limit", 10)),
            "comments_limit": int(payload.get("comments_limit", 50)),
            "max_users": int(payload.get("max_users", 150)),
            "enrich": bool(payload.get("enrich", True)),
            "sleep_s": float(payload.get("sleep_s", 0.4)),
        }
        run_id = db.create_run(_conn, params)
        t = threading.Thread(target=_ig_commenters_job, args=(run_id, params), daemon=True)
        t.start()
        return JSONResponse({"run_id": run_id, "status": "running", "params": params})


@app.get("/api/leads")
def api_list_leads(
    q: str = "",
    status: str = "",
    source: str = "",
    min_score: Optional[int] = None,
    website_verdict: str = "",
    max_website_score: Optional[int] = None,
    limit: int = 200,
    offset: int = 0,
) -> JSONResponse:
    leads = db.list_leads(
        _conn,
        q=q,
        status=status,
        source=source,
        min_score=min_score,
        website_verdict=website_verdict,
        max_website_score=max_website_score,
        limit=limit,
        offset=offset,
    )
    return JSONResponse({"items": leads})


@app.get("/api/stats")
def api_stats(source: str = "") -> JSONResponse:
    counts = db.status_counts(_conn, source=source)
    # Primary KPI: appointment booked (call or WhatsApp).
    primary = int(counts.get("appointment_booked", 0))
    return JSONResponse({"primary_kpi": {"name": "appointments_booked", "count": primary}, "counts": counts})


@app.patch("/api/leads/{lead_id}")
def api_update_lead(lead_id: int, payload: Dict[str, Any]) -> JSONResponse:
    updated = db.update_lead(_conn, int(lead_id), payload or {})
    if not updated:
        raise HTTPException(status_code=404, detail="Lead not found")
    return JSONResponse(updated)


@app.post("/api/ingest/ranked")
def api_ingest_ranked() -> JSONResponse:
    root = runner.workspace_root(Path(__file__).resolve().parent)
    csv_path = root / "ranked_leads.csv"
    if not csv_path.exists():
        raise HTTPException(status_code=404, detail=f"Not found: {csv_path}")
    rows = runner.read_ranked_csv(csv_path)
    n = db.upsert_leads_from_rows(_conn, rows, source="x", compute_score_if_missing=False)
    return JSONResponse({"ingested": n, "path": str(csv_path)})


@app.post("/api/import/csv")
async def api_import_csv(
    source: str = Form("manual"),
    file: UploadFile = File(...),
) -> JSONResponse:
    """
    Import any CSV (IG/FB/manual). Expected columns (best effort):
    handle/username, name, bio/description, followers, location, website/url, email, recent_post_snippet/snippet.
    """
    content = await file.read()
    try:
        text = content.decode("utf-8", errors="replace")
    except Exception:
        raise HTTPException(status_code=400, detail="Could not decode CSV as UTF-8")

    import io
    import csv as csv_mod

    f = io.StringIO(text)
    reader = csv_mod.DictReader(f)
    rows = [dict(r) for r in reader]
    n = db.upsert_leads_from_rows(
        _conn,
        rows,
        source=source.strip().lower() or "manual",
        compute_score_if_missing=True,
        score_fn=scoring.score_row,
    )
    return JSONResponse({"imported": n, "source": source})


@app.post("/api/import/leads-json")
def api_import_leads_json(payload: Dict[str, Any]) -> JSONResponse:
    """
    Import leads from external tools (ex: Chrome extension).
    Expected:
      { "source": "google_maps", "leads": [ {name, phone, website, email, profile_url/profile}, ... ] }
    """
    source = str(payload.get("source") or "manual").strip().lower()
    leads = payload.get("leads") or []
    if not isinstance(leads, list):
        raise HTTPException(status_code=400, detail="Expected `leads` to be a list.")

    # Best-effort normalization for the TRW Google Maps extractor format.
    rows = []
    for l in leads:
        if not isinstance(l, dict):
            continue
        rows.append(
            {
                "handle": (l.get("name") or "").strip() or (l.get("profile") or l.get("profile_url") or ""),
                "profile_url": (l.get("profile_url") or l.get("profile") or "").strip(),
                "name": (l.get("name") or "").strip(),
                "website": (l.get("website") or "").strip(),
                "phone": (l.get("phone") or "").strip(),
                "email": (l.get("email") or "").strip(),
                "bio": "",
                "location": "",
                "followers": "",
                "recent_post_snippet": "",
                "signal_keywords_matched": "imported",
            }
        )

    n = db.upsert_leads_from_rows(
        _conn,
        rows,
        source=source,
        compute_score_if_missing=True,
        score_fn=scoring.score_row,
    )
    return JSONResponse({"imported": n, "source": source})

