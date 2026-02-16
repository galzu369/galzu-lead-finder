import csv
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple


def workspace_root(dashboard_dir: Path) -> Path:
    # dashboard_dir = .../galzu-lead-finder-dashboard/app
    return dashboard_dir.parent.parent


def run_discover_and_score(
    *,
    dashboard_app_dir: Path,
    days: int,
    lang: str,
    max_leads: int,
    min_followers: int,
    keywords_file: str,
    seed_csv: str,
) -> Tuple[Path, Dict[str, Any]]:
    """
    Runs the existing X lead finder script and returns the output CSV path + run metadata.
    """
    root = workspace_root(dashboard_app_dir)
    lead_finder = root / "x-lead-finder" / "lead_finder.py"
    if not lead_finder.exists():
        raise RuntimeError(f"Missing lead finder script: {lead_finder}")

    out_csv = root / "ranked_leads.csv"
    cmd: List[str] = [
        sys.executable,
        str(lead_finder),
        "discover-and-score",
        "--output",
        str(out_csv),
        "--days",
        str(int(days)),
        "--lang",
        str(lang or "en"),
        "--max-leads",
        str(int(max_leads)),
        "--min-followers",
        str(int(min_followers)),
    ]
    if keywords_file:
        cmd += ["--keywords-file", keywords_file]
    if seed_csv:
        cmd += ["--seed-csv", seed_csv]

    proc = subprocess.run(
        cmd,
        cwd=str(root),
        capture_output=True,
        text=True,
        timeout=60 * 15,
    )
    meta = {
        "cmd": cmd,
        "returncode": proc.returncode,
        "stdout": proc.stdout[-8000:],
        "stderr": proc.stderr[-8000:],
    }
    if proc.returncode != 0:
        raise RuntimeError(f"lead_finder.py failed (code {proc.returncode}).\n{meta['stderr']}\n{meta['stdout']}")
    if not out_csv.exists():
        raise RuntimeError(f"Expected output CSV not found: {out_csv}")
    return out_csv, meta


def read_ranked_csv(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
    return rows

