import os
from pathlib import Path


def load_env_file(env_path: Path) -> None:
    # Minimal .env loader (no dependency).
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def load_dashboard_env() -> None:
    # Prefer a dedicated dashboard .env file.
    base = Path(__file__).resolve().parent.parent
    load_env_file(base / ".env")
    # Also load the x-lead-finder env (convenience), but do not override existing vars.
    load_env_file(base.parent / "x-lead-finder" / ".env")

