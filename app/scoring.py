import re
from typing import Dict, List, Tuple


def _tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9@#\.\-\+]+", (text or "").lower())


def score_row(r: Dict[str, object]) -> Tuple[int, List[str]]:
    """
    Platform-agnostic lead scoring.

    Focus: fast-close leads for galzu.pro:
      - Owner-operators / solo sellers who decide alone
      - Already value calls/messages/bookings
      - Weak/no website (or link-in-bio / Maps-only)
      - NOT builders (devs/agencies/makers) and NOT committees
    """
    name = str(r.get("name") or "")
    bio = str(r.get("bio") or "")
    loc = str(r.get("location") or "")
    website = str(r.get("website") or "")
    phone = str(r.get("phone") or "")
    snippet = str(r.get("recent_post_snippet") or "")
    extra = str(r.get("signal_keywords_matched") or "")
    profile_url = str(r.get("profile_url") or "")

    text = " ".join([name, bio, loc, website, phone, snippet, extra, profile_url]).strip()
    text_l = text.lower()
    toks = set(_tokenize(text_l))

    reasons: List[str] = []
    score = 0

    # Strong "buy now" language (optimize for closing in days, not weeks).
    booking_phrases = [
        "taking new clients",
        "available this week",
        "dm to book",
        "dm to schedule",
        "book a job",
        "book now",
        "free quote",
        "get a quote",
        "estimate",
        "same day",
        "next day",
        "emergency",
        "call",
        "whatsapp",
        "text me",
        "message me",
        "appointment",
        "booking",
    ]
    matched_booking = [p for p in booking_phrases if p in text_l]
    if matched_booking:
        score += min(44, 14 + 6 * min(5, len(matched_booking)))
        reasons.append("intent:" + ", ".join(matched_booking[:5]))

    pain_phrases = [
        "lost leads",
        "missing leads",
        "missed calls",
        "too many dms",
        "need more calls",
        "need more leads",
        "need more bookings",
        "need a website",
        "my website is old",
        "website is outdated",
        "link in bio",
    ]
    matched_pain = [p for p in pain_phrases if p in text_l]
    if matched_pain:
        score += min(18, 8 + 3 * min(4, len(matched_pain)))
        reasons.append("pain:" + ", ".join(matched_pain[:4]))

    # Core services: heavy weighting
    service_tokens = {
        "electrician": 32,
        "electrical": 22,
        "plumber": 32,
        "plumbing": 22,
        "handyman": 32,
        "handy-man": 32,
        "carpenter": 30,
        "carpentry": 20,
        "gardener": 30,
        "gardening": 20,
        "painter": 18,
        "installer": 16,
        "hvac": 18,
        "roofing": 14,
        "contractor": 12,
        "landscaper": 16,
        "landscaping": 12,
        "cleaner": 12,
        "cleaning": 10,
        "flooring": 10,
        "tiler": 10,
        "locksmith": 12,
        "mechanic": 12,
    }
    has_local_service = False
    for k, pts in service_tokens.items():
        if k in toks:
            score += pts
            reasons.append(f"svc:{k}")
            has_local_service = True

    # Secondary: solo sellers who book calls/messages (coaches/consultants).
    solo_seller_tokens = {
        "coach": 16,
        "consultant": 14,
        "therapist": 14,
        "trainer": 12,
        "mentor": 10,
        "advisor": 10,
        "copywriter": 8,
        "designer": 4,  # light: many designers DIY; keep small
    }
    for k, pts in solo_seller_tokens.items():
        if k in toks:
            score += pts
            reasons.append(f"role:{k}")

    # Owner-operator signals (decide alone, minimal committee risk).
    owner_tokens = {"owner", "owneroperator", "selfemployed", "self-employed", "solo", "smallbusiness", "small business"}
    if any(t in text_l for t in owner_tokens) or "owner" in toks:
        score += 8
        reasons.append("decision:solo")

    # Weak/no website boosts (good fit for your offer)
    weak_hosts = ["linktr.ee", "carrd.co", "notion.site", "beacons.ai", "taplink.cc", "stan.store"]
    website_state = "unknown"  # unknown | none | weak | strong
    if not website.strip():
        if any(x in text_l for x in ["whatsapp", "call", "dm", "book", "quote"]):
            score += 16
            reasons.append("no_website_but_contact")
        else:
            score += 10
            reasons.append("no_website")
        website_state = "none"
    else:
        wl = website.lower()
        # "Website" is sometimes just IG/FB/Maps/link-in-bio. Treat as weak.
        weakish = weak_hosts + ["instagram.com", "facebook.com", "tiktok.com", "maps.google.", "g.page", "goo.gl/maps"]
        if any(h in wl for h in weakish):
            score += 16
            reasons.append("weak_link_in_bio")
            website_state = "weak"
        else:
            score += 3
            reasons.append("has_website")
            website_state = "strong"

    # Heavy weight: local/manual service providers with weak/no website.
    # This is the highest-probability close-fast segment for your offer.
    if has_local_service and website_state in {"none", "weak"}:
        score += 22 if website_state == "none" else 18
        reasons.append("fit:local_service_weak_web")

    # Contact channel boosts
    if "whatsapp" in toks or "wa.me" in text_l:
        score += 18
        reasons.append("contact:whatsapp")
    if phone.strip():
        score += 14
        reasons.append("contact:phone")
    if "tel:" in text_l:
        score += 8
        reasons.append("contact:phone_link")
    if re.search(r"([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})", text):
        score += 6
        reasons.append("contact:email")

    # Down-rank: builders/agencies/makers (they DIY or want long cycles).
    negative = {
        "developer": -22,
        "engineer": -18,
        "software": -14,
        "github": -22,
        "agency": -16,
        "webdev": -14,
        "freelance": -6,
        "forhire": -10,
        "buildinginpublic": -14,
        "buildinpublic": -14,
        "webflow": -16,
        "wordpress": -10,
        "wix": -10,
        "squarespace": -10,
        "framer": -12,
        "bubble": -12,
        "nocode": -12,
        "no-code": -12,
        "indiehackers": -14,
        "indiehacker": -14,
        "growthhacker": -14,
        "prompt": -10,
    }
    for k, pts in negative.items():
        if k in toks:
            score += pts
            reasons.append(f"neg:{k}")

    # Down-rank: committees / corporate titles / proposal culture.
    committee_phrases = [
        "marketing team",
        "head of",
        "director",
        "vp ",
        "cmo",
        "manager",
        "procurement",
        "rfp",
        "enterprise",
        "stakeholders",
        "proposal",
    ]
    matched_committee = [p for p in committee_phrases if p in text_l]
    if matched_committee:
        score -= min(26, 12 + 4 * min(4, len(matched_committee)))
        reasons.append("neg:committee")

    score = max(0, min(100, int(score)))
    return score, reasons

