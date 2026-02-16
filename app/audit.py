import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


WEAK_LINK_HOSTS = {
    "linktr.ee",
    "carrd.co",
    "notion.site",
    "beacons.ai",
    "taplink.cc",
    "stan.store",
    "gumroad.com",
    "calendly.com",
}


PARKED_PHRASES = [
    "domain parked",
    "this domain is parked",
    "buy this domain",
    "this website is coming soon",
    "coming soon",
    "under construction",
    "site is not configured",
    "account suspended",
]


@dataclass(frozen=True)
class AuditConfig:
    timeout_s: float = 10.0
    max_bytes: int = 450_000
    sleep_s: float = 1.0


def _now_unix() -> int:
    return int(time.time())


def _normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+\-.]*://", url):
        return "https://" + url
    return url


def _host(url: str) -> str:
    try:
        return (urllib.parse.urlparse(url).netloc or "").lower()
    except Exception:
        return ""


def audit_website(url: str, cfg: AuditConfig) -> Dict[str, object]:
    """
    Returns:
      website_score (0-100), website_verdict, website_findings, website_http_status, website_final_url, website_checked_at
    """
    checked_at = _now_unix()
    raw = (url or "").strip()
    if not raw:
        return {
            "website_score": 0,
            "website_verdict": "no_website",
            "website_findings": "No website listed.",
            "website_http_status": None,
            "website_final_url": "",
            "website_checked_at": checked_at,
        }

    norm = _normalize_url(raw)
    host = _host(norm)
    if any(host == h or host.endswith("." + h) for h in WEAK_LINK_HOSTS):
        return {
            "website_score": 20,
            "website_verdict": "weak_link_in_bio",
            "website_findings": "Link-in-bio page (Linktree/Carrd/Notion/etc). Strong upgrade opportunity.",
            "website_http_status": None,
            "website_final_url": norm,
            "website_checked_at": checked_at,
        }

    headers = {
        "User-Agent": "GalzuLeadFinder/1.0 (+local dashboard)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    try:
        req = urllib.request.Request(norm, headers=headers)
        with urllib.request.urlopen(req, timeout=cfg.timeout_s) as resp:
            status = getattr(resp, "status", None) or None
            final_url = resp.geturl() or norm
            ctype = (resp.headers.get("content-type") or "").lower()
            data = resp.read(cfg.max_bytes + 1)
    except urllib.error.HTTPError as e:
        return {
            "website_score": 5,
            "website_verdict": "unreachable",
            "website_findings": f"HTTP error {e.code}.",
            "website_http_status": int(e.code),
            "website_final_url": norm,
            "website_checked_at": checked_at,
        }
    except Exception as e:  # noqa: BLE001
        return {
            "website_score": 5,
            "website_verdict": "unreachable",
            "website_findings": f"Could not fetch site ({type(e).__name__}).",
            "website_http_status": None,
            "website_final_url": norm,
            "website_checked_at": checked_at,
        }

    # Non-HTML websites are usually still workable but harder to judge.
    if ("text/html" not in ctype) and ("application/xhtml" not in ctype):
        verdict = "basic_site"
        findings = [f"Non-HTML content-type: {ctype or 'unknown'}"]
        return {
            "website_score": 45,
            "website_verdict": verdict,
            "website_findings": "; ".join(findings),
            "website_http_status": status,
            "website_final_url": final_url,
            "website_checked_at": checked_at,
        }

    truncated = len(data) > cfg.max_bytes
    html = data[: cfg.max_bytes].decode("utf-8", errors="replace")
    html_l = html.lower()

    findings: List[str] = []
    score = 50

    # Parked/placeholder detection
    if any(p in html_l for p in PARKED_PHRASES):
        findings.append("Looks parked/placeholder/coming-soon.")
        return {
            "website_score": 10,
            "website_verdict": "parked_or_placeholder",
            "website_findings": "; ".join(findings),
            "website_http_status": status,
            "website_final_url": final_url,
            "website_checked_at": checked_at,
        }

    # Basic HTML checks
    has_viewport = 'name="viewport"' in html_l or "name='viewport'" in html_l
    if has_viewport:
        score += 10
        findings.append("Has mobile viewport meta.")
    else:
        score -= 10
        findings.append("Missing mobile viewport meta (mobile UX risk).")

    has_title = bool(re.search(r"<title>\s*[^<]{3,}</title>", html_l))
    if has_title:
        score += 5
    else:
        score -= 6
        findings.append("Missing/empty <title>.")

    has_meta_desc = "name=\"description\"" in html_l or "name='description'" in html_l
    if has_meta_desc:
        score += 5
    else:
        findings.append("Missing meta description (SEO baseline).")

    has_h1 = bool(re.search(r"<h1[^>]*>\s*[^<]{2,}", html_l))
    if has_h1:
        score += 5
    else:
        findings.append("Missing/weak H1 (clarity).")

    # Contact/booking friction
    has_tel = "href=\"tel:" in html_l or "href='tel:" in html_l
    has_mail = "href=\"mailto:" in html_l or "href='mailto:" in html_l
    has_whatsapp = ("wa.me/" in html_l) or ("api.whatsapp.com" in html_l) or ("whatsapp" in html_l)
    if has_whatsapp:
        score += 10
        findings.append("Has WhatsApp contact.")
    if has_tel:
        score += 8
        findings.append("Has phone link.")
    if has_mail:
        score += 4

    booking_kw = ["book", "booking", "quote", "estimate", "call", "whatsapp", "appointment", "schedule"]
    if any(k in html_l for k in booking_kw):
        score += 6
    else:
        score -= 6
        findings.append("No obvious booking/quote CTA language.")

    trust_kw = ["review", "reviews", "testimonial", "testimonials", "before and after", "before & after", "rated"]
    if any(k in html_l for k in trust_kw):
        score += 6
        findings.append("Has trust signals (reviews/testimonials).")
    else:
        findings.append("Weak trust signals (no clear reviews/testimonials found).")

    # Heuristics for heaviness
    scripts = len(re.findall(r"<script\b", html_l))
    if scripts >= 35:
        score -= 8
        findings.append(f"Very script-heavy ({scripts} scripts).")
    elif scripts >= 20:
        score -= 4
        findings.append(f"Somewhat script-heavy ({scripts} scripts).")

    if truncated:
        score -= 6
        findings.append("HTML is large/truncated (page weight risk).")

    score = max(0, min(100, int(score)))
    if score <= 35:
        verdict = "weak_site"
    elif score <= 65:
        verdict = "basic_site"
    else:
        verdict = "good_site"

    # Keep findings short and actionable.
    findings = findings[:8]
    if verdict in ("weak_site", "basic_site") and not (has_tel or has_whatsapp or has_mail):
        findings.insert(0, "No obvious contact links (phone/WhatsApp/email).")

    return {
        "website_score": score,
        "website_verdict": verdict,
        "website_findings": "; ".join(findings),
        "website_http_status": status,
        "website_final_url": final_url,
        "website_checked_at": checked_at,
    }


def audit_leads(urls: List[Tuple[int, str]], cfg: AuditConfig) -> List[Tuple[int, Dict[str, object]]]:
    out: List[Tuple[int, Dict[str, object]]] = []
    for lead_id, url in urls:
        result = audit_website(url, cfg)
        out.append((lead_id, result))
        time.sleep(max(0.0, float(cfg.sleep_s)))
    return out

