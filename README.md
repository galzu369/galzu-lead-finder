# Galzu Lead Finder (Local Dashboard)

Local-only dashboard to run the X lead finder and review/triage leads.

## Setup

From the repo/workspace root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r galzu-lead-finder-dashboard\requirements.txt
```

## Run

```powershell
python -m uvicorn app.main:app --app-dir galzu-lead-finder-dashboard --host 127.0.0.1 --port 8787 --reload
```

Open:

http://127.0.0.1:8787

## Notes

- Keep `x-lead-finder\.env` with `X_BEARER_TOKEN=...` locally (do not commit).
- Database is stored at `galzu-lead-finder-dashboard\data\galzu_leads.sqlite`.
- Facebook/Instagram: use the dashboard CSV import (templates in `galzu-lead-finder-dashboard\static\templates\`).
  - We are not scraping FB/IG; this stays ToS-safe and reliable.

## Meta / Instagram API (official)

1) Create `galzu-lead-finder-dashboard\.env` (see `.env.example`) with:

- `META_ACCESS_TOKEN=...`
- `META_IG_USER_ID=...` (your `instagram_business_account` id)

2) Start the dashboard and use "Instagram (Meta API) - import strong leads" to import commenters.

