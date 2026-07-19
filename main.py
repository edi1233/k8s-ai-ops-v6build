import asyncio
import html
import json
import logging
import os

from fastapi import Depends, FastAPI, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from . import db, kube
from .analyzer import analyze
from .auth import require_auth
from .k8s_client import load_client

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("k8s-ai-ops")

APP_VERSION = "v6"
REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", "120"))
DETAIL_REFRESH_SECONDS = int(os.environ.get("DETAIL_REFRESH_SECONDS", "10"))
DEEPDIVE_PATH = os.environ.get("DEEPDIVE_PATH", "/data/deepdive/report.json")
COMPANION_ANSWERS_PATH = os.environ.get("COMPANION_ANSWERS_PATH", "/data/companion-answers/answers.json")

app = FastAPI(title="k8s-ai-ops")

state = {"report": None, "error": None}
core = apps_api = custom = None

CLUSTER_SCOPE_NS = "-"  # sentinel path segment for cluster-scoped kinds


@app.on_event("startup")
async def startup():
    global core, apps_api, custom
    db.init_db()
    core, apps_api, custom = load_client()
    asyncio.create_task(_loop())
    asyncio.create_task(_companion_sync_loop())


async def _loop():
    while True:
        try:
            state["report"] = await asyncio.to_thread(analyze, core, apps_api, custom)
            state["error"] = None
        except Exception as e:  # noqa: BLE001
            log.exception("analysis failed")
            state["error"] = str(e)
        await asyncio.sleep(REFRESH_SECONDS)


async def _companion_sync_loop():
    """Pick up answers the scheduled AI responder writes into a ConfigMap (mounted read-only)
    and copy any new ones into SQLite so the portal can render them."""
    while True:
        try:
            if os.path.exists(COMPANION_ANSWERS_PATH):
                with open(COMPANION_ANSWERS_PATH) as f:
                    answers = json.load(f)
                for qid_str, answer_text in answers.items():
                    if not answer_text:
                        continue
                    q = db.get_question(int(qid_str))
                    if q and not q["answer"]:
                        db.answer_question(int(qid_str), answer_text)
        except Exception:  # noqa: BLE001
            log.exception("companion answer sync failed")
        await asyncio.sleep(20)


def _read_deepdive():
    if not os.path.exists(DEEPDIVE_PATH):
        return None
    try:
        with open(DEEPDIVE_PATH) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        log.warning("deepdive read failed: %s", e)
        return None


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/api/analyze")
async def api_analyze():
    if state["report"] is None:
        state["report"] = await asyncio.to_thread(analyze, core, apps_api, custom)
    return JSONResponse(state["report"])


@app.get("/api/deepdive")
async def api_deepdive():
    return JSONResponse(_read_deepdive() or {})


# ---- internal companion queue API (no auth: internal-network only, used by the scheduled AI responder) ----

@app.get("/api/companion/pending")
async def api_companion_pending():
    return JSONResponse(db.list_pending_questions())


# ---- live object detail feed (powers the AJAX auto-refresh on detail pages) ----

@app.get("/api/obj/{cluster}/{namespace}/{kind}/{name}")
async def api_object_detail(cluster: str, namespace: str, kind: str, name: str, _=Depends(require_auth)):
    ns = None if namespace == CLUSTER_SCOPE_NS else namespace

    def _collect():
        out = {"error": None, "yaml": "", "events": [], "logs": None, "has_logs": False}
        try:
            meta = kube.kind_by_slug(cluster, kind)
            out["yaml"] = kube.get_object_yaml(cluster, ns, kind, name)
            out["has_logs"] = meta["kind"] == "Pod" and bool(ns)
        except Exception as e:  # noqa: BLE001
            out["error"] = str(e)
        out["events"] = kube.related_events(cluster, ns, name) if ns else []
        if out["has_logs"]:
            out["logs"] = kube.pod_log_tail(cluster, ns, name)
        return out

    return JSONResponse(await asyncio.to_thread(_collect))


# =====================================================================================
# Design system + page shell
# =====================================================================================

KIND_ICONS = {
    "Pod": "\U0001F535", "Deployment": "\U0001F680", "StatefulSet": "\U0001F5C4",
    "DaemonSet": "\U0001F6F0", "ReplicaSet": "\U0001F9EC", "Service": "\U0001F310",
    "Ingress": "\U0001F6AA", "HTTPRoute": "\U0001F6AA", "ConfigMap": "\U0001F4C4",
    "Secret": "\U0001F512", "PersistentVolumeClaim": "\U0001F4BD", "PersistentVolume": "\U0001F4BE",
    "Job": "\U00002699", "CronJob": "\U000023F0", "Node": "\U0001F5A5", "Namespace": "\U0001F4E6",
    "Event": "\U0001F4E2", "ReplicationController": "\U0001F9EC", "Endpoints": "\U0001F517",
    "ServiceAccount": "\U0001F464", "Role": "\U0001F511", "RoleBinding": "\U0001F517",
    "ClusterRole": "\U0001F511", "NetworkPolicy": "\U0001F6E1", "StorageClass": "\U0001F5C3",
}
DEFAULT_ICON = "\U0001F9E9"


def _icon(kind: str) -> str:
    return KIND_ICONS.get(kind, DEFAULT_ICON)


STYLE = """
<style>
:root {
  --bg: #0a0e17; --surface: #121826; --surface-2: #19212f; --border: #232c3d;
  --text: #e7ebf3; --text-muted: #8b93a7; --accent: #4f8dfd; --accent-2: #7c6ff0;
  --ok: #22c55e; --warn: #eab308; --bad: #ef4444; --radius: 12px;
}
* { box-sizing: border-box; }
body { margin:0; font-family: -apple-system, "Segoe UI", Roboto, sans-serif; background:var(--bg); color:var(--text); }
a { color: var(--accent); text-decoration:none; }
a:hover { text-decoration:underline; }
.app { display:flex; min-height:100vh; }
.sidebar { width:240px; flex-shrink:0; background:var(--surface); border-right:1px solid var(--border); padding:1.25rem 1rem; position:sticky; top:0; height:100vh; overflow-y:auto; }
.brand { font-weight:700; font-size:16px; margin-bottom:.25rem; display:flex; align-items:center; gap:8px; }
.brand .ver { font-size:10px; font-weight:600; color:var(--accent); background:rgba(79,141,253,.14); padding:1px 6px; border-radius:99px; }
.sidebar .tag { font-size:11px; color:var(--text-muted); margin-bottom:1.5rem; }
.sidebar nav a { display:flex; align-items:center; gap:8px; padding:8px 10px; border-radius:8px; color:var(--text); font-size:14px; margin-bottom:2px; }
.sidebar nav a:hover, .sidebar nav a.active { background:var(--surface-2); text-decoration:none; }
.sidebar h4 { font-size:11px; text-transform:uppercase; letter-spacing:.06em; color:var(--text-muted); margin:1.5rem 0 .4rem; }
.sidebar .cluster-link { display:flex; align-items:center; gap:8px; padding:7px 10px; border-radius:8px; font-size:13px; color:var(--text-muted); }
.sidebar .cluster-link:hover { background:var(--surface-2); color:var(--text); text-decoration:none; }
.sidebar .cluster-link .dot { width:7px; height:7px; border-radius:50%; background:var(--ok); flex-shrink:0; }
main { flex:1; min-width:0; }
.topbar { padding:.85rem 2rem; border-bottom:1px solid var(--border); font-size:13px; color:var(--text-muted); position:sticky; top:0; background:rgba(10,14,23,.9); backdrop-filter:blur(6px); z-index:5; display:flex; justify-content:space-between; align-items:center; }
.topbar a { color:var(--text-muted); } .topbar a:hover { color:var(--accent); }
.topbar .clock { font-variant-numeric:tabular-nums; font-size:12px; }
.content { padding:2rem; max-width:1400px; }
h1 { font-size:22px; margin:0 0 .3rem; display:flex; align-items:center; gap:.5rem; }
h2 { font-size:15px; margin:2rem 0 .75rem; color:var(--text); }
.muted { color:var(--text-muted); font-size:13px; }
.grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(210px,1fr)); gap:12px; }
.stat-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:12px; margin-bottom:1.5rem; }
.stat { background:var(--surface); border:1px solid var(--border); border-radius:var(--radius); padding:14px 16px; font-size:13px; color:var(--text-muted); }
.stat .n { font-size:26px; font-weight:700; display:block; margin-top:4px; color:var(--text); }
.stat.bad .n { color:var(--bad); } .stat.warn .n { color:var(--warn); } .stat.ok .n { color:var(--ok); }
.card { background:var(--surface); border:1px solid var(--border); border-radius:var(--radius); padding:1.1rem 1.3rem; margin-bottom:1rem; }
.banner { border-radius:var(--radius); padding:.9rem 1.2rem; margin-bottom:1.25rem; font-size:14px; border:1px solid var(--border); display:flex; align-items:center; gap:10px; }
.banner.ok { background:rgba(34,197,94,.08); border-color:rgba(34,197,94,.35); }
.banner.warn { background:rgba(234,179,8,.08); border-color:rgba(234,179,8,.35); }
.banner.bad { background:rgba(239,68,68,.08); border-color:rgba(239,68,68,.35); }
.kind-card { background:var(--surface); border:1px solid var(--border); border-radius:var(--radius); padding:12px 14px; cursor:pointer; transition:.12s; display:flex; align-items:center; gap:10px; font-size:14px; }
.kind-card:hover { border-color:var(--accent); background:var(--surface-2); transform:translateY(-1px); }
.kind-card .em { font-size:20px; }
table { border-collapse:collapse; width:100%; background:var(--surface); border:1px solid var(--border); border-radius:var(--radius); overflow:hidden; }
th { background:var(--surface-2); text-align:left; font-size:12px; text-transform:uppercase; letter-spacing:.04em; color:var(--text-muted); padding:10px 14px; }
td { padding:10px 14px; border-top:1px solid var(--border); font-size:14px; }
tr.clickable { cursor:pointer; } tr.clickable:hover td { background:var(--surface-2); }
.pill { display:inline-block; padding:2px 10px; border-radius:99px; font-size:12px; font-weight:600; }
.pill-ok { background:rgba(34,197,94,.15); color:var(--ok); }
.pill-warn { background:rgba(234,179,8,.15); color:var(--warn); }
.pill-bad { background:rgba(239,68,68,.15); color:var(--bad); }
.pill-neutral { background:rgba(139,147,167,.15); color:var(--text-muted); }
input, select, textarea { background:var(--surface-2); color:var(--text); border:1px solid var(--border); border-radius:8px; padding:8px 10px; font-size:14px; font-family:inherit; }
input:focus, select:focus, textarea:focus { outline:none; border-color:var(--accent); }
button, .btn { background:var(--accent); color:#fff; border:none; padding:9px 18px; border-radius:8px; cursor:pointer; font-size:14px; font-weight:600; }
button:hover, .btn:hover { opacity:.9; text-decoration:none; }
.btn-ghost { background:var(--surface-2); color:var(--text); border:1px solid var(--border); padding:6px 12px; font-size:13px; }
pre { background:#0d1220; border:1px solid var(--border); padding:1rem; border-radius:var(--radius); overflow-x:auto; white-space:pre-wrap; font-size:13px; line-height:1.5; margin:0; font-family:"SF Mono",ui-monospace,Menlo,Consolas,monospace; }
pre.logs { white-space:pre; max-height:60vh; overflow:auto; }
.tabs { display:flex; gap:4px; border-bottom:1px solid var(--border); margin-bottom:1.25rem; flex-wrap:wrap; }
.tab-btn { background:none; border:none; color:var(--text-muted); padding:10px 16px; font-size:14px; font-weight:600; cursor:pointer; border-bottom:2px solid transparent; }
.tab-btn:hover { color:var(--text); }
.tab-btn.active { color:var(--accent); border-bottom-color:var(--accent); }
.tab-panel { display:none; } .tab-panel.active { display:block; }
.qa { border-left:3px solid var(--accent); padding:.6rem 1rem; margin-bottom:.6rem; background:var(--surface); border-radius:0 8px 8px 0; }
.live-dot { display:inline-block; width:8px; height:8px; border-radius:50%; background:var(--ok); margin-right:6px; animation:pulse 2s infinite; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.35} }
.toolbar { display:flex; align-items:center; gap:12px; margin:0 0 .9rem; flex-wrap:wrap; }
.live-badge { font-size:12px; color:var(--text-muted); display:inline-flex; align-items:center; }
</style>
"""

SCRIPT = """
<script>
function filterCards(inputId, containerId) {
  const q = document.getElementById(inputId).value.toLowerCase();
  document.querySelectorAll('#' + containerId + ' .kind-card').forEach(function(el) {
    el.style.display = el.dataset.label.toLowerCase().includes(q) ? 'flex' : 'none';
  });
}
function filterRows(inputId, tableId) {
  const q = document.getElementById(inputId).value.toLowerCase();
  document.querySelectorAll('#' + tableId + ' tbody tr').forEach(function(el) {
    el.style.display = el.innerText.toLowerCase().includes(q) ? '' : 'none';
  });
}
function showTab(name) {
  document.querySelectorAll('.tab-panel').forEach(function(el) { el.classList.remove('active'); });
  document.querySelectorAll('.tab-btn').forEach(function(el) { el.classList.remove('active'); });
  document.getElementById('tab-' + name).classList.add('active');
  document.getElementById('btn-' + name).classList.add('active');
  location.hash = name;
}
function copyYaml() {
  const el = document.getElementById('overview-content');
  if (el) navigator.clipboard.writeText(el.textContent).then(function(){
    const b = document.getElementById('copy-btn'); if (b){ const t=b.textContent; b.textContent='Copied!'; setTimeout(function(){b.textContent=t;},1200); }
  });
}
function pillClass(t) {
  t = (t||'').toLowerCase();
  if (/running|bound|ready|active|true|succeeded|healthy|synced|complete|normal/.test(t)) return 'ok';
  if (/pending|progressing|unknown|warning|containercreating/.test(t)) return 'warn';
  if (/failed|error|false|evicted|crashloop|notready|degraded|backoff/.test(t)) return 'bad';
  return 'neutral';
}
function esc(s){ const d=document.createElement('div'); d.textContent = s==null?'':s; return d.innerHTML; }

let _detailTimer = null;
async function refreshDetail(url) {
  const ts = document.getElementById('live-ts');
  try {
    const r = await fetch(url, {cache:'no-store'});
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const d = await r.json();
    if (d.yaml !== undefined) document.getElementById('overview-content').textContent = d.yaml || (d.error ? ('Error: ' + d.error) : '(empty)');
    const eb = document.getElementById('events-body');
    if (eb) {
      if (!d.events || d.events.length === 0) {
        eb.innerHTML = '<tr><td colspan=5 class="muted">No related events</td></tr>';
      } else {
        eb.innerHTML = d.events.map(function(e){
          return '<tr><td><span class="pill pill-'+pillClass(e.type)+'">'+esc(e.type)+'</span></td><td>'+esc(e.reason)+'</td><td>'+esc(e.message)+'</td><td>'+esc(e.count)+'</td><td class="muted">'+esc(e.last_seen||'')+'</td></tr>';
        }).join('');
      }
    }
    const lg = document.getElementById('logs-content');
    if (lg && d.logs != null) lg.textContent = d.logs;
    if (ts) ts.textContent = 'updated ' + new Date().toLocaleTimeString();
  } catch (err) {
    if (ts) ts.textContent = 'refresh failed: ' + err.message;
  }
}
function initDetail(url, intervalSec) {
  refreshDetail(url);
  if (_detailTimer) clearInterval(_detailTimer);
  _detailTimer = setInterval(function(){ refreshDetail(url); }, intervalSec * 1000);
}
function tickClock() {
  const c = document.getElementById('clock');
  if (c) c.textContent = new Date().toLocaleString();
}
document.addEventListener('DOMContentLoaded', function() {
  var h = location.hash.replace('#', '');
  if (h && document.getElementById('tab-' + h)) { showTab(h); }
  tickClock(); setInterval(tickClock, 1000);
});
</script>
"""


def _status_pill(text) -> str:
    if not text:
        return ""
    t = str(text).lower()
    if any(s in t for s in ("running", "bound", "ready", "active", "true", "succeeded", "healthy", "synced", "complete")):
        cls = "ok"
    elif any(s in t for s in ("pending", "progressing", "unknown", "warning", "containercreating")):
        cls = "warn"
    elif any(s in t for s in ("failed", "error", "false", "evicted", "crashloop", "notready", "degraded", "backoff")):
        cls = "bad"
    else:
        cls = "neutral"
    return f'<span class="pill pill-{cls}">{html.escape(str(text))}</span>'


def _page(title: str, body: str, breadcrumb: list[tuple[str, str]] | None = None) -> HTMLResponse:
    try:
        clusters = db.list_clusters()
    except Exception:  # noqa: BLE001
        clusters = []
    cluster_links = "".join(
        f"<a class='cluster-link' href='/browse/{c['name']}'><span class='dot'></span>{html.escape(c['name'])}</a>"
        for c in clusters
    )
    crumbs = ""
    if breadcrumb:
        parts = []
        for label, href in breadcrumb:
            parts.append(f"<a href='{href}'>{html.escape(label)}</a>" if href else f"<span>{html.escape(label)}</span>")
        crumbs = " / ".join(parts)
    return HTMLResponse(f"""<html><head><title>{html.escape(title)} · k8s-ai-ops</title>{STYLE}</head><body>
<div class="app">
  <aside class="sidebar">
    <div class="brand">\U0001F916 k8s-ai-ops <span class="ver">{APP_VERSION}</span></div>
    <div class="tag">Kubernetes management platform</div>
    <nav>
      <a href="/">\U0001F4CA Dashboard</a>
      <a href="/clusters">\U0001F5A7 Clusters</a>
    </nav>
    <h4>Registered clusters</h4>
    {cluster_links or '<span class="muted">none registered</span>'}
  </aside>
  <main>
    <div class="topbar"><span>{crumbs or '&nbsp;'}</span><span class="clock" id="clock"></span></div>
    <div class="content">{body}</div>
  </main>
</div>
{SCRIPT}
</body></html>""")


# =====================================================================================
# Dashboard
# =====================================================================================

def _render_dashboard() -> str:
    r = state["report"]
    dd = _read_deepdive()
    if r is None:
        return "<h1>k8s-ai-ops</h1><p class='muted'>Collecting initial data…</p>"

    s = r["summary"]
    if s["node_issue_count"] > 0:
        banner = f"<div class='banner bad'>\U0001F534 <span><b>{s['node_issue_count']} node issue(s)</b> and {s['pod_issue_count']} pod issue(s) detected — see below.</span></div>"
    elif s["pod_issue_count"] > 0:
        banner = f"<div class='banner warn'>\U0001F7E1 <span>{s['pod_issue_count']} pod issue(s) detected — no node-level problems.</span></div>"
    else:
        banner = "<div class='banner ok'>\U0001F7E2 <span>No node or pod issues detected right now.</span></div>"

    rows_node = "".join(
        f"<tr><td>{n['node']}</td><td>{n['condition']}</td><td>{_status_pill(n['severity'])}</td><td class='muted'>{html.escape(n.get('message',''))}</td></tr>"
        for n in r["node_issues"]
    ) or "<tr><td colspan=4 class='muted'>No node issues detected</td></tr>"

    rows_pod = "".join(
        f"<tr><td>{p['namespace']}/{p['pod']}</td><td>{_status_pill(p['phase'])}</td><td>{html.escape(str(p.get('reason','')))}</td>"
        f"<td>{p.get('restarts',0)}</td><td>{p.get('node','')}</td><td>{_status_pill(p['severity'])}</td></tr>"
        for p in r["pod_issues"]
    ) or "<tr><td colspan=6 class='muted'>No pod issues detected</td></tr>"

    rows_rs = "".join(
        f"<tr><td>{x['namespace']}/{x['pod']}</td><td>{x['container']}</td>"
        f"<td>{x['cpu_usage_m']}m / req {x['cpu_request_m']}m / lim {x['cpu_limit_m']}m</td>"
        f"<td>{x['mem_usage_mi']}Mi / req {x['mem_request_mi']}Mi / lim {x['mem_limit_mi']}Mi</td>"
        f"<td>{'; '.join(x['suggestions'])}</td></tr>"
        for x in r["rightsizing"]
    ) or "<tr><td colspan=5 class='muted'>No right-sizing suggestions right now</td></tr>"

    rows_ev = "".join(
        f"<tr><td>{html.escape(e['reason'])}</td><td>{e['count']}</td></tr>" for e in r["event_summary"]
    ) or "<tr><td colspan=2 class='muted'>No warning events</td></tr>"

    rows_orphan = "".join(
        f"<tr><td>{o['namespace']}</td><td>{o['count']}</td></tr>" for o in r["orphaned_terminal_pods"]
    ) or "<tr><td colspan=2 class='muted'>None</td></tr>"

    deepdive_html = "<p class='muted'>No AI deep-dive report yet. It is written periodically by a scheduled analysis run.</p>"
    if dd:
        deepdive_html = f"<p class='muted'>generated {html.escape(str(dd.get('generated_at','')))}</p><div style='white-space:pre-wrap'>{html.escape(str(dd.get('narrative','')))}</div>"

    return f"""
<h1><span class="live-dot"></span>pxinf · cluster advisor</h1>
<p class="muted">generated {r['generated_at']} · analysis refreshes every {REFRESH_SECONDS}s</p>
{banner}
<div class="stat-grid">
  <div class="stat">Nodes<span class="n">{s['node_count']}</span></div>
  <div class="stat">Pods<span class="n">{s['pod_count']}</span></div>
  <div class="stat {'bad' if s['node_issue_count'] else 'ok'}">Node issues<span class="n">{s['node_issue_count']}</span></div>
  <div class="stat {'warn' if s['pod_issue_count'] else 'ok'}">Pod issues<span class="n">{s['pod_issue_count']}</span></div>
  <div class="stat">Right-size hints<span class="n">{s['rightsizing_count']}</span></div>
  <div class="stat">Containers w/o requests<span class="n">{s['no_requests_count']}</span></div>
</div>
<h2>\U0001F916 AI deep-dive (latest scheduled analysis)</h2>
<div class="card">{deepdive_html}</div>
<h2>Node issues</h2>
<table><thead><tr><th>Node</th><th>Condition</th><th>Severity</th><th>Message</th></tr></thead><tbody>{rows_node}</tbody></table>
<h2>Pod issues</h2>
<table><thead><tr><th>Pod</th><th>Phase</th><th>Reason</th><th>Restarts</th><th>Node</th><th>Severity</th></tr></thead><tbody>{rows_pod}</tbody></table>
<h2>Resource right-sizing suggestions</h2>
<table><thead><tr><th>Pod</th><th>Container</th><th>CPU</th><th>Memory</th><th>Suggestion</th></tr></thead><tbody>{rows_rs}</tbody></table>
<h2>Top warning events</h2>
<table><thead><tr><th>Reason</th><th>Count</th></tr></thead><tbody>{rows_ev}</tbody></table>
<h2>Orphaned terminal pods by namespace (GC candidates)</h2>
<table><thead><tr><th>Namespace</th><th>Count</th></tr></thead><tbody>{rows_orphan}</tbody></table>
<p><a href="/api/analyze">raw JSON →</a></p>
"""


@app.get("/", response_class=HTMLResponse)
async def dashboard(_=Depends(require_auth)):
    return _page("Dashboard", _render_dashboard(), [("Dashboard", "")])


# =====================================================================================
# Clusters
# =====================================================================================

@app.get("/clusters", response_class=HTMLResponse)
async def clusters_page(_=Depends(require_auth)):
    cards = "".join(
        f"""<div class="card">
          <div style="display:flex;justify-content:space-between;align-items:center">
            <div><b>{html.escape(c['name'])}</b><div class="muted">{c['kind']} · {html.escape(c.get('api_server') or 'in-cluster')}</div></div>
            <a class="btn" href="/browse/{c['name']}">Browse →</a>
          </div>
        </div>"""
        for c in db.list_clusters()
    )
    body = f"""
<h1>Clusters</h1>
<p class="muted">Clusters registered with this platform. Add one below — the connection is tested before it is saved.</p>
{cards}
<div class="card">
<h2 style="margin-top:0">Add cluster</h2>
<form method="post" action="/clusters/add">
  <p>Name<br><input name="name" required style="width:320px"></p>
  <p>Auth type<br>
    <select name="auth_kind">
      <option value="token">API server + bearer token</option>
      <option value="kubeconfig">Paste kubeconfig</option>
    </select>
  </p>
  <p>API server (for token auth)<br><input name="api_server" placeholder="https://host:6443" style="width:320px"></p>
  <p>Bearer token (for token auth)<br><input name="token" style="width:320px"></p>
  <p><label><input type="checkbox" name="verify_ssl" checked> Verify TLS</label></p>
  <p>Kubeconfig (for kubeconfig auth)<br><textarea name="kubeconfig" rows="8" cols="60"></textarea></p>
  <button type="submit">Add + test connection</button>
</form>
</div>
"""
    return _page("Clusters", body, [("Clusters", "")])


@app.post("/clusters/add")
async def clusters_add(
    name: str = Form(...),
    auth_kind: str = Form(...),
    api_server: str = Form(""),
    token: str = Form(""),
    verify_ssl: str = Form(None),
    kubeconfig: str = Form(""),
    _=Depends(require_auth),
):
    row = {
        "kind": auth_kind, "api_server": api_server or None, "token": token or None,
        "verify_ssl": 1 if verify_ssl else 0, "kubeconfig": kubeconfig or None,
    }
    ok, msg = kube.test_connection(row)
    if not ok:
        return _page("Clusters", f"<h1>Connection failed</h1><pre>{html.escape(msg)}</pre><p><a href='/clusters'>← back</a></p>", [("Clusters", "/clusters")])
    db.add_cluster(name, auth_kind, api_server or None, token or None, bool(verify_ssl), kubeconfig or None)
    return RedirectResponse(url="/clusters", status_code=303)


# =====================================================================================
# Browse
# =====================================================================================

@app.get("/browse/{cluster}", response_class=HTMLResponse)
async def browse_cluster(cluster: str, ns: str | None = None, _=Depends(require_auth)):
    try:
        namespaces = kube.list_namespaces(cluster)
        kinds = kube.list_kinds(cluster)
    except Exception as e:  # noqa: BLE001
        return _page("Browse", f"<h1>{html.escape(cluster)}</h1><p style='color:var(--bad)'>Error discovering cluster: {html.escape(str(e))}</p>", [("Clusters", "/clusters"), (cluster, "")])

    current_ns = ns or (namespaces[0] if namespaces else None)
    ns_options = "".join(
        f"<option value='{n}' {'selected' if n == current_ns else ''}>{n}</option>" for n in namespaces
    )
    cluster_kinds = [k for k in kinds if not k["namespaced"]]
    ns_kinds = [k for k in kinds if k["namespaced"]]

    cluster_cards = "".join(
        f"""<a class="kind-card" data-label="{k['label']}" href="/browse/{cluster}/{CLUSTER_SCOPE_NS}/{k['slug']}">
              <span class="em">{_icon(k['kind'])}</span> {html.escape(k['label'])}</a>"""
        for k in cluster_kinds
    )
    ns_cards = "".join(
        f"""<a class="kind-card" data-label="{k['label']}" href="/browse/{cluster}/{current_ns}/{k['slug']}">
              <span class="em">{_icon(k['kind'])}</span> {html.escape(k['label'])}</a>"""
        for k in ns_kinds
    ) if current_ns else "<p class='muted'>No namespaces found.</p>"

    body = f"""
<h1>{html.escape(cluster)}</h1>
<p class="muted">{len(kinds)} resource types discovered live from this cluster's API (built-ins + CRDs) · {len(namespaces)} namespaces. Click any type to browse its objects.</p>

<div class="card">
  <label class="muted">Namespace</label><br>
  <select onchange="location.href='/browse/{cluster}?ns='+encodeURIComponent(this.value)">{ns_options}</select>
</div>

<h2>Namespaced — {html.escape(current_ns or '')} <span class="muted">({len(ns_kinds)})</span></h2>
<input id="search-ns" placeholder="Filter resource types…" oninput="filterCards('search-ns','ns-kinds')" style="width:280px;margin-bottom:12px">
<div class="grid" id="ns-kinds">{ns_cards}</div>

<h2>Cluster-scoped <span class="muted">({len(cluster_kinds)})</span></h2>
<input id="search-cluster" placeholder="Filter resource types…" oninput="filterCards('search-cluster','cluster-kinds')" style="width:280px;margin-bottom:12px">
<div class="grid" id="cluster-kinds">{cluster_cards}</div>
"""
    return _page(f"Browse · {cluster}", body, [("Clusters", "/clusters"), (cluster, "")])


@app.get("/browse/{cluster}/{namespace}/{kind}", response_class=HTMLResponse)
async def browse_list(cluster: str, namespace: str, kind: str, _=Depends(require_auth)):
    ns = None if namespace == CLUSTER_SCOPE_NS else namespace
    try:
        meta = kube.kind_by_slug(cluster, kind)
        items = kube.list_objects(cluster, ns, kind)
    except Exception as e:  # noqa: BLE001
        return _page("Browse", f"<h1>{html.escape(kind)}</h1><p style='color:var(--bad)'>Error: {html.escape(str(e))}</p>", [("Clusters", "/clusters"), (cluster, f"/browse/{cluster}"), (kind, "")])

    extra_cols = [c for c in ("phase", "ready", "restarts", "node", "type", "keys") if any(c in i for i in items)]
    status_cols = {"phase", "ready"}
    header = "<th>Namespace</th><th>Name</th>" + "".join(f"<th>{c}</th>" for c in extra_cols)
    rows = ""
    for it in items:
        link_ns = it.get("namespace") or CLUSTER_SCOPE_NS
        href = f"/browse/{cluster}/{link_ns}/{kind}/{it['name']}"
        cells = f"<td>{it.get('namespace') or ''}</td><td><b>{html.escape(it['name'])}</b></td>"
        for c in extra_cols:
            v = it.get(c, "")
            v = ", ".join(v) if isinstance(v, list) else v
            cells += f"<td>{_status_pill(v) if c in status_cols else html.escape(str(v))}</td>"
        rows += f"<tr class='clickable' onclick=\"location.href='{href}'\">{cells}</tr>"

    body = f"""
<h1><span class="em" style="margin-right:8px">{_icon(meta['kind'])}</span>{html.escape(meta['label'])}</h1>
<p class="muted">{html.escape(cluster)}{' / ' + html.escape(namespace) if ns else ''} · {len(items)} object(s) · click a row for live details</p>
<input id="row-filter" placeholder="Filter objects…" oninput="filterRows('row-filter','obj-table')" style="width:300px;margin-bottom:12px">
<table id="obj-table"><thead><tr>{header}</tr></thead><tbody>{rows or f"<tr><td colspan={2+len(extra_cols)} class='muted'>No objects found</td></tr>"}</tbody></table>
"""
    return _page(meta["label"], body, [("Clusters", "/clusters"), (cluster, f"/browse/{cluster}"), (meta["label"], "")])


@app.get("/browse/{cluster}/{namespace}/{kind}/{name}", response_class=HTMLResponse)
async def browse_detail(cluster: str, namespace: str, kind: str, name: str, _=Depends(require_auth)):
    ns = None if namespace == CLUSTER_SCOPE_NS else namespace
    try:
        meta = kube.kind_by_slug(cluster, kind)
        yaml_text = kube.get_object_yaml(cluster, ns, kind, name)
        fetch_error = None
    except Exception as e:  # noqa: BLE001
        meta = {"label": kind, "kind": kind}
        yaml_text = ""
        fetch_error = str(e)

    has_logs = meta["kind"] == "Pod" and ns

    qas = db.list_recent_questions(cluster, ns, kind, name)
    qa_html = "".join(
        f"<div class='qa'><b>Q:</b> {html.escape(q['question'])}<br>"
        f"<b>A:</b> {'<span class=\"muted\">pending — the AI companion checks every ~5 min</span>' if not q['answer'] else html.escape(q['answer'])}</div>"
        for q in qas
    ) or "<p class='muted'>No questions asked yet.</p>"

    overview_error = f"<p style='color:var(--bad)'>{html.escape(fetch_error)}</p>" if fetch_error else ""

    logs_tab_btn = '<button class="tab-btn" id="btn-logs" onclick="showTab(\'logs\')">Logs</button>' if has_logs else ""
    logs_panel = '<div class="tab-panel" id="tab-logs"><pre class="logs" id="logs-content">loading…</pre></div>' if has_logs else ""

    feed_url = f"/api/obj/{cluster}/{namespace}/{kind}/{name}"

    body = f"""
<h1><span class="em" style="margin-right:8px">{_icon(meta['kind'])}</span>{html.escape(meta['label'])}/{html.escape(name)}</h1>
<div class="toolbar">
  <span class="live-badge"><span class="live-dot"></span><span id="live-ts">connecting…</span></span>
  <span class="muted">· auto-refreshing every {DETAIL_REFRESH_SECONDS}s</span>
  <a class="btn-ghost btn" href="/browse/{cluster}/{namespace}/{kind}">← back to list</a>
</div>
{overview_error}
<div class="tabs">
  <button class="tab-btn active" id="btn-overview" onclick="showTab('overview')">Overview (YAML)</button>
  <button class="tab-btn" id="btn-events" onclick="showTab('events')">Events</button>
  {logs_tab_btn}
  <button class="tab-btn" id="btn-companion" onclick="showTab('companion')">\U0001F916 AI companion</button>
</div>
<div class="tab-panel active" id="tab-overview">
  <div class="toolbar"><button class="btn-ghost btn" id="copy-btn" onclick="copyYaml()">Copy YAML</button></div>
  <pre id="overview-content">{html.escape(yaml_text) or 'loading…'}</pre>
</div>
<div class="tab-panel" id="tab-events">
  <table><thead><tr><th>Type</th><th>Reason</th><th>Message</th><th>Count</th><th>Last seen</th></tr></thead>
  <tbody id="events-body"><tr><td colspan=5 class="muted">loading…</td></tr></tbody></table>
</div>
{logs_panel}
<div class="tab-panel" id="tab-companion">
  <p class="muted">Ask a question about this object. A scheduled AI analysis run checks for new questions roughly every 5 minutes and writes back a real, investigated answer.</p>
  {qa_html}
  <form method="post" action="/browse/{cluster}/{namespace}/{kind}/{name}/ask">
    <textarea name="question" rows="2" cols="60" placeholder="e.g. why does this pod keep restarting?" required></textarea><br><br>
    <button type="submit">Ask AI</button>
  </form>
</div>
<script>initDetail("{feed_url}", {DETAIL_REFRESH_SECONDS});</script>
"""
    return _page(
        f"{meta['label']}/{name}", body,
        [("Clusters", "/clusters"), (cluster, f"/browse/{cluster}"), (meta["label"], f"/browse/{cluster}/{namespace}/{kind}"), (name, "")],
    )


@app.post("/browse/{cluster}/{namespace}/{kind}/{name}/ask")
async def browse_ask(cluster: str, namespace: str, kind: str, name: str, question: str = Form(...), _=Depends(require_auth)):
    ns = None if namespace == CLUSTER_SCOPE_NS else namespace
    db.submit_question(cluster, ns, kind, name, question)
    return RedirectResponse(url=f"/browse/{cluster}/{namespace}/{kind}/{name}", status_code=303)
