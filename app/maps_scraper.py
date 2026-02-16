import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

# Plausible email regex; we skip obvious false positives (images, schema.org, etc.).
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
_SKIP_EMAIL_DOMAINS = frozenset(
    ("example.com", "example.org", "sentry.io", "w3.org", "schema.org", "gravatar.com")
)


@dataclass
class MapsScrapeConfig:
    query: str
    max_results: int = 30
    headful: bool = True
    slow_mo_ms: int = 50
    # Keep conservative defaults; Google Maps UI shifts frequently.
    nav_timeout_ms: int = 60_000
    step_sleep_s: float = 0.15
    scroll_sleep_s: float = 0.8
    max_scroll_rounds: int = 60
    max_total_s: float = 300.0


def _clean(s: Optional[str]) -> str:
    return (s or "").strip()


def _extract_phone(text: str) -> str:
    """
    Best-effort phone extraction from sidebar text.
    Works across many locales by favoring digit-heavy sequences.
    """
    t = text or ""
    # International-ish pattern (keeps +, spaces, hyphens, parentheses)
    cand = re.findall(r"\+?\d[\d\s\-\(\)]{7,24}\d", t)
    if not cand:
        return ""
    # Prefer the candidate with the most digits (and a reasonable length).
    def score_phone(p: str) -> int:
        digits = re.sub(r"[^\d]", "", p)
        return len(digits)

    best = sorted(cand, key=score_phone, reverse=True)[0]
    cleaned = re.sub(r"[^\d+]", "", best)
    # Avoid super short junk.
    digits = re.sub(r"[^\d]", "", cleaned)
    return cleaned if 9 <= len(digits) <= 16 else ""


def _looks_like_website(url: str) -> bool:
    u = (url or "").lower()
    if not u.startswith("http"):
        return False
    # Avoid internal Google links.
    if "google." in u or "g.page" in u or "goo.gl" in u or "maps.app.goo.gl" in u:
        return False
    return True


def _looks_like_domain_text(s: str) -> bool:
    t = (s or "").strip().lower()
    if not t:
        return False
    if " " in t or "/" in t:
        return False
    if t.count(".") < 1:
        return False
    # Avoid common non-domains
    if t.endswith(".png") or t.endswith(".jpg") or t.endswith(".jpeg") or t.endswith(".webp"):
        return False
    return True


def _normalize_website_value(href: str, text: str) -> str:
    h = _clean(href)
    if _looks_like_website(h):
        return h
    t = _clean(text)
    if _looks_like_domain_text(t):
        return "https://" + t
    return ""


def _extract_domain_from_text(text: str) -> str:
    """
    Fallback for locales where the website appears as plain text (ex: malveicampo.com).
    """
    t = (text or "").lower()
    # Basic domain pattern; avoid grabbing common Google/system domains.
    cand = re.findall(r"\b([a-z0-9\-]+\.[a-z]{2,}(?:\.[a-z]{2,})?)\b", t)
    for d in cand:
        if "google" in d or d.endswith(".g.page"):
            continue
        if d.endswith(".png") or d.endswith(".jpg") or d.endswith(".jpeg") or d.endswith(".webp"):
            continue
        return "https://" + d
    return ""


def scrape_google_maps(cfg: MapsScrapeConfig, *, base_dir: Path) -> List[Dict[str, str]]:
    """
    Scrape Google Maps search results into:
      { name, profile_url, website, phone }

    Notes:
    - Runs headful by default so the user can solve login/captcha if needed.
    - Uses a persistent Chromium profile, so login persists across runs.
    """
    # Lazy import so the dashboard can still start without Playwright installed.
    from playwright.sync_api import sync_playwright  # type: ignore

    cache_dir = base_dir / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    profile_dir = cache_dir / "playwright_chromium_profile"

    # Force locale similar to your manual usage (improves DOM consistency + results count).
    search_url = f"https://www.google.com/maps/search/{cfg.query.replace(' ', '%20')}?hl=pt-PT&gl=PT"
    leads: List[Dict[str, str]] = []
    t0 = time.monotonic()

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=not cfg.headful,
            slow_mo=cfg.slow_mo_ms,
            viewport={"width": 1280, "height": 860},
        )
        try:
            page = context.new_page()
            page.set_default_timeout(cfg.nav_timeout_ms)

            page.goto(search_url, wait_until="domcontentloaded")
            time.sleep(1.2)

            # Cookie/consent prompts can block the UI (especially in EU).
            for label in [
                "Accept all",
                "I agree",
                "Agree",
                "Aceitar tudo",
                "Aceitar",
                "Concordo",
            ]:
                try:
                    btn = page.get_by_role("button", name=label)
                    if btn and btn.is_visible():
                        btn.click()
                        time.sleep(0.8)
                        break
                except Exception:
                    pass

            # Results feed container (varies, but role=feed is stable).
            feed = page.locator('[role="feed"]').first
            try:
                feed.wait_for(state="visible", timeout=25_000)
            except Exception:
                pass

            card_sel = 'div[role="article"], div.Nv2PK'
            # Wait a bit for cards to appear (otherwise we just scroll emptiness).
            try:
                page.locator(card_sel).first.wait_for(state="visible", timeout=25_000)
            except Exception as e:
                raise RuntimeError("Google Maps results did not load (no listing cards found). Try headful mode and log in.") from e

            # Scroll until we have enough distinct cards or no progress.
            last_count = 0
            no_progress_rounds = 0
            for _ in range(cfg.max_scroll_rounds):
                if time.monotonic() - t0 > cfg.max_total_s:
                    raise RuntimeError("Timed out while scrolling listings. Try reducing max results or run in headful mode.")
                cards = page.locator(card_sel)
                count = cards.count()
                if count >= cfg.max_results:
                    break
                if count == last_count:
                    no_progress_rounds += 1
                else:
                    no_progress_rounds = 0
                if no_progress_rounds >= 6:
                    break
                last_count = count

                # Scroll the feed (preferred) or the page as fallback.
                try:
                    # Some layouts require wheel events to trigger lazy loading.
                    feed.hover(timeout=2000)
                    page.mouse.wheel(0, 1400)
                    feed.evaluate("(el) => { el.scrollTop = el.scrollTop + el.clientHeight * 1.8; }")
                except Exception:
                    page.mouse.wheel(0, 1200)
                time.sleep(cfg.scroll_sleep_s)

            # Collect cards up to max_results.
            cards = page.locator(card_sel)
            n = min(cards.count(), cfg.max_results)
            seen_keys = set()

            for i in range(n):
                if time.monotonic() - t0 > cfg.max_total_s:
                    raise RuntimeError("Timed out while collecting listing details. Try fewer results (or complete any Google login/captcha).")
                card = cards.nth(i)
                try:
                    name = _clean(card.locator("div.qBF1Pd").first.inner_text()).split("\n")[0] or "Unnamed"
                except Exception:
                    name = "Unnamed"
                try:
                    profile_url = _clean(card.locator("a.hfpxzc").first.get_attribute("href") or "")
                except Exception:
                    profile_url = ""

                key = (name + "|" + profile_url).strip("|")
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                website = ""
                phone = ""

                # Fast path: often the card already contains phone/website.
                try:
                    t = _clean(card.locator("span.UsdlK").first.inner_text(timeout=250))
                    if t:
                        phone = t
                except Exception:
                    pass
                try:
                    links_in_card = card.locator("a")
                    for j in range(min(15, links_in_card.count())):
                        href = _clean(links_in_card.nth(j).get_attribute("href") or "")
                        txt = ""
                        try:
                            txt = _clean(links_in_card.nth(j).inner_text(timeout=200))
                        except Exception:
                            txt = ""
                        cand = _normalize_website_value(href, txt)
                        if cand:
                            website = cand
                            break
                except Exception:
                    pass

                # Click to load side panel only when missing fields.
                if not phone or not website:
                    try:
                        card.click()
                        time.sleep(1.1)
                    except Exception:
                        pass

                # Sidebar region (best-effort).
                # NOTE: Google Maps uses different structures per locale. Instead of relying on a single
                # container, we (a) try strong selectors anywhere on the page after click, and (b) fall
                # back to regex over a best-effort details text snapshot.
                # Prefer a panel that contains the business title. This avoids scanning huge page regions.
                panel = None
                for candidate in [
                    page.locator('div[role="main"]').filter(has=page.locator("h1")).first,
                    page.locator('div[role="region"]').filter(has=page.locator("h1")).first,
                ]:
                    try:
                        if candidate and candidate.is_visible():
                            panel = candidate
                            break
                    except Exception:
                        continue
                if panel is None:
                    panel = page.locator('div[role="region"]').first

                # Website: use fast, targeted selectors only (avoid expensive full-panel reads).
                try:
                    a = panel.locator('a[data-item-id="authority"]').first
                    # Most reliable: href points to the external website.
                    website = _normalize_website_value(a.get_attribute("href") or "", "")
                except Exception:
                    website = ""

                if not website:
                    try:
                        # Common website anchor in Maps UI.
                        a2 = panel.locator("a.CsEnBe").first
                        website = _normalize_website_value(a2.get_attribute("href") or "", a2.inner_text(timeout=600))
                    except Exception:
                        website = ""

                if not website:
                    try:
                        a3 = panel.locator('a[aria-label*="Website"], a[aria-label*="website"], a[aria-label*="Site"], a[aria-label*="site"]').first
                        website = _normalize_website_value(a3.get_attribute("href") or "", a3.inner_text(timeout=400))
                    except Exception:
                        website = ""

                # Phone: prefer tel: link / phone button, then regex from details text.
                try:
                    tel = panel.locator('a[href^="tel:"]').first.get_attribute("href") or ""
                    if tel.lower().startswith("tel:"):
                        phone = tel.split(":", 1)[1].strip()
                except Exception:
                    pass

                if not phone:
                    for sel in [
                        'button[data-item-id^="phone"]',
                        'button[aria-label*="Phone"]',
                        'button[aria-label*="Telefone"]',
                        'button[aria-label*="Ligar"]',
                        'div[aria-label*="Telefone"]',
                    ]:
                        try:
                            node = panel.locator(sel).first
                            # aria-label often contains the number even when inner_text is slow/empty.
                            aria = _clean(node.get_attribute("aria-label") or "")
                            t = _clean(node.inner_text(timeout=450)) or aria
                            if t:
                                phone = t
                                break
                        except Exception:
                            pass

                if not phone:
                    # Last resort: best-effort regex on a small snippet.
                    try:
                        snippet = _clean(panel.locator("span, div").first.inner_text(timeout=400))
                    except Exception:
                        snippet = ""
                    phone = _extract_phone(snippet)

                # Last-chance extraction from plain text fields Google Maps uses (often Io6YTe).
                if not website or not phone:
                    try:
                        nodes = panel.locator("div.Io6YTe, span.Io6YTe, div[aria-label], span[aria-label]")
                        for j in range(min(50, nodes.count())):
                            try:
                                txt = _clean(nodes.nth(j).inner_text(timeout=120))
                            except Exception:
                                continue
                            if not website and _looks_like_domain_text(txt):
                                website = "https://" + txt
                            if not phone:
                                ph = _extract_phone(txt)
                                if ph:
                                    phone = ph
                            if website and phone:
                                break
                    except Exception:
                        pass

                leads.append(
                    {
                        "name": name,
                        "profile_url": profile_url,
                        "website": website,
                        "phone": phone,
                    }
                )
                time.sleep(max(0.0, cfg.step_sleep_s))
        finally:
            context.close()

    return leads


def enrich_email_from_website(url: str, timeout_s: float = 8.0, max_bytes: int = 200_000) -> str:
    """
    Fetch a business website and return the first plausible email found in the HTML.
    Used to enrich google_maps leads that have a website but no email.
    """
    url = (url or "").strip()
    if not url or not url.startswith("http"):
        return ""
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/115.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read(max_bytes)
    except (urllib.error.URLError, OSError, ValueError):
        return ""
    try:
        text = raw.decode("utf-8", errors="ignore")
    except Exception:
        return ""
    for m in _EMAIL_RE.finditer(text):
        email = m.group(0).strip().lower()
        # Skip image/data false positives and known non-business domains.
        if "@" in email:
            domain = email.split("@", 1)[1]
            if domain in _SKIP_EMAIL_DOMAINS:
                continue
            if any(domain.endswith(s) for s in (".png", ".jpg", ".gif", ".webp", ".svg")):
                continue
            return email
    return ""

