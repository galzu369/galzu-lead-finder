async function apiGet(path) {
  const res = await fetch(path, { headers: { "Accept": "application/json" } });
  if (!res.ok) throw new Error(await res.text());
  return await res.json();
}

async function apiJson(method, path, body) {
  const res = await fetch(path, {
    method,
    headers: { "Content-Type": "application/json", "Accept": "application/json" },
    body: JSON.stringify(body || {}),
  });
  if (!res.ok) throw new Error(await res.text());
  return await res.json();
}

function el(id) {
  return document.getElementById(id);
}

function escapeHtml(s) {
  return String(s || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function normPhone(raw) {
  const s = String(raw || "").trim();
  if (!s) return "";
  // Keep + and digits for tel/wa links.
  const cleaned = s.replace(/[^\d+]/g, "");
  return cleaned;
}

function toWhatsAppLink(phone) {
  const p = normPhone(phone).replace(/^\+/, "");
  return p ? `https://wa.me/${encodeURIComponent(p)}` : "";
}

async function copyText(txt) {
  const t = String(txt || "");
  if (!t) return;
  try {
    await navigator.clipboard.writeText(t);
  } catch (_) {
    // Fallback for older permissions.
    const ta = document.createElement("textarea");
    ta.value = t;
    document.body.appendChild(ta);
    ta.select();
    document.execCommand("copy");
    document.body.removeChild(ta);
  }
}

function getTemplate() {
  // Defaults tuned to https://galzu.pro (fast execution + free 24h review).
  const subject = localStorage.getItem("tplSubject") || "Quick question about {name}";
  const body =
    localStorage.getItem("tplBody") ||
    "Hey {name} — quick question.\n\nI found you via Google and took a quick look at {website}.\n\nIf you want, I can send a free 24h project review: the 3 quickest changes that typically increase calls/bookings + a simple plan.\n\nIf you prefer execution instead of meetings, my main offer is: {offer}.\n\nWant me to send the review here, or what's the best email to reach you?\n\n- {me}\n{my_site}\n{my_email}";
  return { subject, body };
}

function fillTemplate(tpl, lead) {
  const name = String(lead.name || lead.handle || "").trim() || "there";
  const website = String(lead.website || "").trim() || "(no website listed)";
  const me = (localStorage.getItem("myName") || localStorage.getItem("tplMe") || "Galzu").trim();
  const mySite = (localStorage.getItem("mySite") || "https://galzu.pro").trim();
  const myEmail = (localStorage.getItem("myEmail") || "galzuconsult@gmail.com").trim();
  const offer = (localStorage.getItem("myOffer") || "Landing Page Express (48h) — from €150").trim();
  return String(tpl || "")
    .replaceAll("{name}", name)
    .replaceAll("{website}", website)
    .replaceAll("{me}", me)
    .replaceAll("{my_site}", mySite)
    .replaceAll("{my_email}", myEmail)
    .replaceAll("{offer}", offer);
}

function debounce(fn, ms) {
  let t = null;
  return (...args) => {
    if (t) clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  };
}

function renderLeads(items) {
  const tbody = el("leadsTable").querySelector("tbody");
  tbody.innerHTML = "";
  for (const it of items) {
    const handle = it.handle || "";
    const source = it.source || "";
    const name = (it.name || "").trim();
    const profileUrl =
      it.profile_url ||
      (source === "instagram" ? (handle ? `https://www.instagram.com/${handle}/` : "") :
      source === "facebook" ? (handle ? `https://www.facebook.com/${handle}` : "") :
      source === "x" ? (handle ? `https://x.com/${handle}` : "") :
      "");
    const website = it.website || "";
    const phone = it.phone || "";
    const email = it.email || "";
    const matched = it.signal_keywords_matched || "";
    const reason = it.reason || "";
    const notes = it.notes || "";
    const status = it.status || "new";

    const tr = document.createElement("tr");
    tr.setAttribute("data-status", status);
    const websiteRating = it.website_verdict
      ? `${it.website_verdict} (${it.website_score ?? ""})`
      : "";
    const websiteFindings = it.website_findings || "";
    const leadTitle = escapeHtml(name || (handle && handle.startsWith("http") ? "" : handle) || "Lead");
    const leadSubParts = [];
    if (profileUrl) leadSubParts.push(`<a href="${escapeHtml(profileUrl)}" target="_blank" rel="noreferrer">profile</a>`);
    if (matched) leadSubParts.push(`<span class="pill">${escapeHtml(matched)}</span>`);
    if (reason) leadSubParts.push(`<span class="pill" title="${escapeHtml(reason)}">why</span>`);

    tr.innerHTML = `
      <td>
        <div class="lead-title">${leadTitle}</div>
        <div class="lead-sub">
          ${handle && !handle.startsWith("http") ? `<span class="pill">@${escapeHtml(handle)}</span>` : ""}
          ${leadSubParts.join(" ")}
        </div>
      </td>
      <td>
        <div>${phone ? `<span class="pill">${escapeHtml(phone)}</span>` : `<span class="muted">no phone</span>`}</div>
        <div style="margin-top:6px">${email ? `<span class="pill">${escapeHtml(email)}</span>` : `<span class="muted">no email</span>`}</div>
      </td>
      <td>${escapeHtml(it.score ?? "")}</td>
      <td>${escapeHtml(source)}</td>
      <td>${escapeHtml(it.location ?? "")}</td>
      <td>${website ? `<a href="${escapeHtml(website)}" target="_blank" rel="noreferrer">${escapeHtml(website)}</a>` : ""}</td>
      <td><small title="${escapeHtml(websiteFindings)}">${escapeHtml(websiteRating)}</small></td>
      <td>
        <select data-lead-id="${it.id}" class="statusSel">
          ${["new","qualified","contacted","appointment_booked","won","not_fit"].map(s => `<option value="${s}" ${s===status?"selected":""}>${s}</option>`).join("")}
        </select>
      </td>
      <td>
        <textarea data-lead-id="${it.id}" class="notesTxt" placeholder="Objections, context, follow-up...">${escapeHtml(notes)}</textarea>
        <div class="muted" data-notes-status="${it.id}"></div>
      </td>
    `;
    tbody.appendChild(tr);
  }

  for (const sel of document.querySelectorAll(".statusSel")) {
    sel.addEventListener("change", async (e) => {
      const id = e.target.getAttribute("data-lead-id");
      const val = e.target.value;
      try {
        await apiJson("PATCH", `/api/leads/${id}`, { status: val });
        const row = e.target.closest("tr");
        if (row) row.setAttribute("data-status", val);
      } catch (err) {
        alert(`Failed to update status: ${err.message}`);
      }
    });
  }

  const saveNotesDebounced = debounce(async (id, val) => {
    const statusEl = document.querySelector(`[data-notes-status="${id}"]`);
    if (statusEl) statusEl.textContent = "Saving...";
    try {
      await apiJson("PATCH", `/api/leads/${id}`, { notes: val });
      if (statusEl) {
        statusEl.textContent = "Saved";
        setTimeout(() => (statusEl.textContent = ""), 900);
      }
    } catch (err) {
      if (statusEl) statusEl.textContent = "Save failed";
      console.error(err);
    }
  }, 450);

  for (const ta of document.querySelectorAll(".notesTxt")) {
    ta.addEventListener("input", (e) => {
      const id = e.target.getAttribute("data-lead-id");
      saveNotesDebounced(id, e.target.value || "");
    });
  }

}

async function refreshStats() {
  try {
    const s = await apiGet("/api/stats");
    const counts = s.counts || {};
    const primary = (s.primary_kpi && s.primary_kpi.count) || 0;

    const kpis = el("kpis");
    if (!kpis) return;
    kpis.innerHTML = `
      <div class="kpi">
        <div class="kpi-label">Appointments booked</div>
        <div class="kpi-value">${escapeHtml(primary)}</div>
      </div>
      <div class="kpi">
        <div class="kpi-label">New</div>
        <div class="kpi-value">${escapeHtml(counts.new || 0)}</div>
      </div>
    `;
  } catch (_) {
    // Non-fatal.
  }
}

async function refresh() {
  const q = el("q").value || "";
  const status = el("statusFilter").value || "";
  const source = el("sourceFilter").value || "";
  const minScore = Number(el("minScore").value || 0);
  const websiteVerdict = el("websiteVerdictFilter") ? (el("websiteVerdictFilter").value || "") : "";
  const maxWebsiteScore = el("maxWebsiteScore") ? Number(el("maxWebsiteScore").value || 100) : 100;
  const hasPhone = el("hasPhone") ? !!el("hasPhone").checked : false;
  const hasEmail = el("hasEmail") ? !!el("hasEmail").checked : false;
  const qs = new URLSearchParams();
  if (q) qs.set("q", q);
  if (status) qs.set("status", status);
  if (source) qs.set("source", source);
  if (minScore > 0) qs.set("min_score", String(minScore));
  if (websiteVerdict) qs.set("website_verdict", websiteVerdict);
  if (Number.isFinite(maxWebsiteScore) && maxWebsiteScore < 100) qs.set("max_website_score", String(maxWebsiteScore));
  const data = await apiGet(`/api/leads?${qs.toString()}`);
  const items = data.items || [];
  // Keep a copy for row actions.
  window.__lastItems = items;

  let filtered = items;
  if (hasPhone) filtered = filtered.filter((x) => String(x.phone || "").trim().length > 0);
  if (hasEmail) filtered = filtered.filter((x) => String(x.email || "").trim().length > 0);
  // Default sort for speed: highest score first.
  filtered = filtered.slice().sort((a, b) => Number(b.score || 0) - Number(a.score || 0));
  // One-time convenience: if DB is empty but ranked_leads.csv exists, ingest it.
  if (!window.__ingestAttempted && filtered.length === 0) {
    window.__ingestAttempted = true;
    try {
      await apiJson("POST", "/api/ingest/ranked", {});
      const data2 = await apiGet(`/api/leads?${qs.toString()}`);
      const items2 = data2.items || [];
      window.__lastItems = items2;
      let filtered2 = items2;
      if (hasPhone) filtered2 = filtered2.filter((x) => String(x.phone || "").trim().length > 0);
      if (hasEmail) filtered2 = filtered2.filter((x) => String(x.email || "").trim().length > 0);
      filtered2 = filtered2.slice().sort((a, b) => Number(b.score || 0) - Number(a.score || 0));
      renderLeads(filtered2);
      return;
    } catch (_) {
      // Ignore; user can run discovery from the UI.
    }
  }
  renderLeads(filtered);
  await refreshStats();
}

async function runDiscovery() {
  const runBtn = el("runBtn");
  runBtn.disabled = true;
  el("runStatus").textContent = "Running...";

  const payload = {
    days: Number(el("days").value || 2),
    lang: el("lang").value || "en",
    max_leads: Number(el("max_leads").value || 25),
    min_followers: Number(el("min_followers").value || 0),
    keywords_file: el("keywords_file").value || "",
    seed_csv: el("seed_csv").value || "",
  };

  try {
    const out = await apiJson("POST", "/api/runs/discover", payload);
    const runId = out.run_id;
    await pollRun(runId);
    await refresh();
  } catch (err) {
    alert(`Run failed: ${err.message}`);
  } finally {
    runBtn.disabled = false;
  }
}

async function pollRun(runId) {
  for (let i = 0; i < 120; i++) {
    const r = await apiGet(`/api/runs/${runId}`);
    if (r.status === "ok") {
      el("runStatus").textContent = `Done. Run #${runId}`;
      return;
    }
    if (r.status === "error") {
      el("runStatus").textContent = `Error. Run #${runId}`;
      alert(`Run error:\n\n${r.error || "unknown error"}`);
      return;
    }
    el("runStatus").textContent = `Running... (run #${runId})`;
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  el("runStatus").textContent = `Still running (run #${runId}).`;
}

async function importCsv() {
  const fileInput = el("importFile");
  const f = fileInput.files && fileInput.files[0];
  if (!f) {
    alert("Select a CSV file first.");
    return;
  }

  const source = el("importSource").value || "manual";
  el("importStatus").textContent = "Importing...";

  const fd = new FormData();
  fd.append("source", source);
  fd.append("file", f);
  const res = await fetch("/api/import/csv", { method: "POST", body: fd });
  if (!res.ok) {
    el("importStatus").textContent = "Import failed.";
    alert(await res.text());
    return;
  }
  const out = await res.json();
  el("importStatus").textContent = `Imported ${out.imported} rows.`;
  await refresh();
}

async function runWebsiteAudit() {
  const btn = el("auditBtn");
  btn.disabled = true;
  el("auditStatus").textContent = "Auditing...";
  const payload = {
    max_sites: Number(el("auditMax").value || 25),
    sleep_s: Number(el("auditSleep").value || 1.0),
  };
  try {
    const out = await apiJson("POST", "/api/runs/audit-websites", payload);
    await pollAudit(out.run_id);
    await refresh();
  } catch (err) {
    alert(`Website audit failed: ${err.message}`);
  } finally {
    btn.disabled = false;
  }
}

async function pollAudit(runId) {
  for (let i = 0; i < 1800; i++) {
    const r = await apiGet(`/api/runs/${runId}`);
    if (r.status === "ok") {
      el("auditStatus").textContent = `Done. Audit run #${runId}`;
      return;
    }
    if (r.status === "error") {
      el("auditStatus").textContent = `Error. Audit run #${runId}`;
      alert(`Audit error:\n\n${r.error || "unknown error"}`);
      return;
    }
    el("auditStatus").textContent = `Auditing... (run #${runId})`;
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  el("auditStatus").textContent = `Still auditing (run #${runId}).`;
}

async function runInstagramImport() {
  const btn = el("igRunBtn");
  btn.disabled = true;
  el("igStatus").textContent = "Importing...";

  const payload = {
    ig_user_id: el("igUserId").value || "",
    media_limit: Number(el("igMediaLimit").value || 10),
    comments_limit: Number(el("igCommentsLimit").value || 50),
    max_users: Number(el("igMaxUsers").value || 150),
    enrich: (el("igEnrich").value || "yes") === "yes",
  };

  try {
    const out = await apiJson("POST", "/api/runs/ig-commenters", payload);
    await pollIg(out.run_id);
    await refresh();
  } catch (err) {
    alert(`IG import failed: ${err.message}`);
  } finally {
    btn.disabled = false;
  }
}

async function pollIg(runId) {
  for (let i = 0; i < 1800; i++) {
    const r = await apiGet(`/api/runs/${runId}`);
    if (r.status === "ok") {
      el("igStatus").textContent = `Done. IG run #${runId}`;
      return;
    }
    if (r.status === "error") {
      el("igStatus").textContent = `Error. IG run #${runId}`;
      alert(`IG error:\n\n${r.error || "unknown error"}`);
      return;
    }
    el("igStatus").textContent = `Importing... (run #${runId})`;
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  el("igStatus").textContent = `Still importing (run #${runId}).`;
}

el("refreshBtn").addEventListener("click", refresh);
el("runBtn").addEventListener("click", runDiscovery);
el("importBtn").addEventListener("click", importCsv);
if (el("auditBtn")) el("auditBtn").addEventListener("click", runWebsiteAudit);
if (el("igRunBtn")) el("igRunBtn").addEventListener("click", runInstagramImport);
el("q").addEventListener("keydown", (e) => {
  if (e.key === "Enter") refresh();
});

// Outreach template UI
function initTemplateUI() {
  const s = el("tplSubject");
  const b = el("tplBody");
  const saveBtn = el("tplSaveBtn");
  const resetBtn = el("tplResetBtn");
  const status = el("tplStatus");
  const myName = el("myName");
  const mySite = el("mySite");
  const myEmail = el("myEmail");
  const myOffer = el("myOffer");
  const presetSel = el("tplPreset");
  const applyPresetBtn = el("tplApplyPresetBtn");
  if (!s || !b || !saveBtn || !resetBtn) return;

  const tpl = getTemplate();
  s.value = tpl.subject;
  b.value = tpl.body;
  if (myName) myName.value = (localStorage.getItem("myName") || "Galzu").toString();
  if (mySite) mySite.value = (localStorage.getItem("mySite") || "https://galzu.pro").toString();
  if (myEmail) myEmail.value = (localStorage.getItem("myEmail") || "galzuconsult@gmail.com").toString();
  if (myOffer) myOffer.value = (localStorage.getItem("myOffer") || myOffer.value || "").toString();

  saveBtn.addEventListener("click", () => {
    localStorage.setItem("tplSubject", s.value || "");
    localStorage.setItem("tplBody", b.value || "");
    if (myName) localStorage.setItem("myName", (myName.value || "").trim());
    if (mySite) localStorage.setItem("mySite", (mySite.value || "").trim());
    if (myEmail) localStorage.setItem("myEmail", (myEmail.value || "").trim());
    if (myOffer) localStorage.setItem("myOffer", (myOffer.value || "").trim());
    if (status) {
      status.textContent = "Saved";
      setTimeout(() => (status.textContent = ""), 900);
    }
  });
  resetBtn.addEventListener("click", () => {
    localStorage.removeItem("tplSubject");
    localStorage.removeItem("tplBody");
    localStorage.removeItem("myName");
    localStorage.removeItem("mySite");
    localStorage.removeItem("myEmail");
    localStorage.removeItem("myOffer");
    const tpl2 = getTemplate();
    s.value = tpl2.subject;
    b.value = tpl2.body;
    if (myName) myName.value = "Galzu";
    if (mySite) mySite.value = "https://galzu.pro";
    if (myEmail) myEmail.value = "galzuconsult@gmail.com";
    if (status) {
      status.textContent = "Reset";
      setTimeout(() => (status.textContent = ""), 900);
    }
  });

  function presetTemplates(kind) {
    if (kind === "coaches") {
      return {
        subject: "Quick question about {name}",
        body:
          "Hey {name} — quick question.\n\nI saw your offer and took a quick look at {website}.\n\nIf you want, I can send a free 24h project review: the 3 quickest fixes to get more DMs/calls + a clearer \"here's how to work with me\" page.\n\nIf you'd rather skip meetings, I can build it in 48h (copy + design + deployment + lead form). Offer: {offer}.\n\nWant me to send the review here, or what's the best email?\n\n- {me}\n{my_site}",
      };
    }
    if (kind === "founders") {
      return {
        subject: "48h page to validate {name}?",
        body:
          "Hey {name} — quick question.\n\nIf you're trying to validate/launch fast, I can build a simple conversion page in 48h (clear CTA + form) so you can start collecting leads this week.\n\nI can also send a free 24h project review first (quick wins + scope suggestion).\n\nIf useful, reply with: what you're selling + who it's for + your CTA (DM / call / email).\n\n- {me}\n{my_site}",
      };
    }
    // Default: local service providers (trades).
    return {
      subject: "More calls this week for {name}?",
      body:
        "Hey {name} — quick question.\n\nI found you on Google. If you're getting inquiries via calls/WhatsApp/DMs, a simple page can stop you losing leads when you're busy.\n\nI can send a free 24h review (3 quick fixes to get more calls/messages). If you want execution instead of meetings, I can build the page in 48h (copy + design + deployment + lead form).\n\nWant the free review? If yes, what's your main service + service area?\n\n- {me}\n{my_site}\n{my_email}",
    };
  }

  if (applyPresetBtn && presetSel) {
    applyPresetBtn.addEventListener("click", () => {
      const p = presetTemplates(presetSel.value || "trades");
      s.value = p.subject;
      b.value = p.body;
      if (status) {
        status.textContent = "Preset applied";
        setTimeout(() => (status.textContent = ""), 900);
      }
    });
  }
}

initTemplateUI();

// Tabs (Leads / Google Maps / Imports)
function setActiveTab(tabId) {
  for (const b of document.querySelectorAll(".tabBtn")) {
    b.classList.toggle("active", b.getAttribute("data-tab") === tabId);
  }
  for (const s of document.querySelectorAll(".tab")) {
    s.style.display = s.getAttribute("data-tab") === tabId ? "" : "none";
  }
  localStorage.setItem("activeTab", tabId);
}

for (const b of document.querySelectorAll(".tabBtn")) {
  b.addEventListener("click", () => setActiveTab(b.getAttribute("data-tab")));
}
setActiveTab(localStorage.getItem("activeTab") || "leads");

// Google Maps wizard (auto-import)
function setWizardStep(step) {
  for (const s of document.querySelectorAll("#mapsWizard .wizard-step")) {
    s.classList.toggle("active", s.getAttribute("data-step") === String(step));
  }
  for (const p of document.querySelectorAll("#mapsWizard .wizard-page")) {
    p.style.display = p.getAttribute("data-step") === String(step) ? "" : "none";
  }
}

async function runMapsScrape() {
  const niche = (el("mapsNiche").value || "").trim();
  const location = (el("mapsLocation").value || "").trim();
  const maxResults = Number(el("mapsMax").value || 30);
  const headful = !!el("mapsHeadful").checked;

  if (!niche || !location) {
    alert("Enter trade/niche and location.");
    return;
  }

  if (el("mapsStatus")) el("mapsStatus").textContent = "Starting...";
  setWizardStep(2);
  if (el("mapsProgressBar")) el("mapsProgressBar").style.width = "12%";
  if (el("mapsRunStatus")) el("mapsRunStatus").textContent = "Launching browser...";

  try {
    const out = await apiJson("POST", "/api/runs/maps-scrape", {
      niche,
      location,
      max_results: maxResults,
      headful,
    });
    const runId = out.run_id;
    await pollMapsRun(runId);
  } catch (err) {
    alert(`Maps scrape failed: ${err.message}`);
    setWizardStep(1);
  }
}

async function pollMapsRun(runId) {
  // Cap polling time so the UI doesn't feel like it's looping forever.
  // Backend has its own timeout; this is just UX.
  for (let i = 0; i < 420; i++) {
    const r = await apiGet(`/api/runs/${runId}`);
    if (r.status === "ok") {
      if (el("mapsProgressBar")) el("mapsProgressBar").style.width = "100%";
      if (el("mapsRunStatus")) el("mapsRunStatus").textContent = `Done. Run #${runId}`;
      if (el("mapsSuccessText")) el("mapsSuccessText").textContent = r.error || "Imported google_maps leads.";
      setWizardStep(3);
      await refresh();
      return;
    }
    if (r.status === "error") {
      if (el("mapsRunStatus")) el("mapsRunStatus").textContent = `Error. Run #${runId}`;
      alert(`Maps scrape error:\n\n${r.error || "unknown error"}`);
      setWizardStep(1);
      return;
    }

    // Not true progress, but gives a sense of movement.
    const pct = Math.min(92, 12 + Math.floor((i / 40) * 8));
    if (el("mapsProgressBar")) el("mapsProgressBar").style.width = `${pct}%`;
    if (el("mapsRunStatus")) el("mapsRunStatus").textContent = `Scraping... (run #${runId})`;
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  if (el("mapsRunStatus")) {
    el("mapsRunStatus").textContent =
      `Still running (run #${runId}). ` +
      `Try Max results = 5 and keep “Show browser” on (login/captcha may be blocking).`;
  }
}

if (el("mapsRunBtn")) el("mapsRunBtn").addEventListener("click", runMapsScrape);
if (el("mapsNewRunBtn")) el("mapsNewRunBtn").addEventListener("click", () => setWizardStep(1));
if (el("mapsViewLeadsBtn")) {
  el("mapsViewLeadsBtn").addEventListener("click", async () => {
    setActiveTab("leads");
    if (el("sourceFilter")) el("sourceFilter").value = "google_maps";
    await refresh();
  });
}

refresh().catch((e) => {
  console.error(e);
});

