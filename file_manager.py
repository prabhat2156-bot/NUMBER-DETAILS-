import os, time, mimetypes, json, shutil
from pathlib import Path
from flask import (
    Flask, request, jsonify, send_file, abort,
    render_template_string, Response
)

app = Flask(__name__)
app.secret_key = os.urandom(32)

# In-memory token store — also populated by bot.py
token_store: dict = {}

HIDDEN_NAMES = {
    "venv", "__pycache__", ".git", "node_modules",
    "output.log", ".env.bak",
}

def is_hidden(name: str) -> bool:
    if name in HIDDEN_NAMES:
        return True
    if name.endswith(".pyc"):
        return True
    return False

def validate_token(token: str) -> dict | None:
    """Return token data if valid and not expired, else None."""
    data = token_store.get(token)
    if not data:
        return None
    if time.time() > data["expires_at"]:
        token_store.pop(token, None)
        return None
    return data

def safe_path(base: str, rel: str) -> str | None:
    """Return absolute path if within base, else None."""
    base = os.path.realpath(base)
    target = os.path.realpath(os.path.join(base, rel.lstrip("/")))
    if not target.startswith(base):
        return None
    return target

# ─────────────────────────────────────────────────────────────
# Landing page
# ─────────────────────────────────────────────────────────────

LANDING_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>God Madara Hosting</title>
<style>
  *{margin:0;padding:0;box-sizing:border-box}
  body{background:#0d1117;color:#c9d1d9;font-family:'Segoe UI',sans-serif;
       display:flex;align-items:center;justify-content:center;min-height:100vh}
  .card{text-align:center;padding:3rem 2rem;max-width:500px}
  .emoji{font-size:4rem;margin-bottom:1rem}
  h1{font-size:2rem;background:linear-gradient(135deg,#58a6ff,#bc8cff);
     -webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:.5rem}
  .status{display:inline-flex;align-items:center;gap:.5rem;
          background:#161b22;border:1px solid #30363d;border-radius:2rem;
          padding:.5rem 1.5rem;margin-top:1.5rem;font-size:1.1rem}
  .dot{width:12px;height:12px;background:#3fb950;border-radius:50%;
       animation:pulse 2s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
</style>
</head>
<body>
<div class="card">
  <div class="emoji">🌟</div>
  <h1>God Madara Hosting Bot</h1>
  <p>Your 24/7 Python project hosting platform</p>
  <div class="status"><span class="dot"></span> Online</div>
</div>
</body>
</html>"""

@app.route("/")
def index():
    return render_template_string(LANDING_HTML)

@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "God Madara Hosting Bot"})

# ─────────────────────────────────────────────────────────────
# File Manager UI  —  Mobile-first Phone File Manager
# ─────────────────────────────────────────────────────────────

FM_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>Files — God Madara</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/codemirror.min.css">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/theme/dracula.min.css">
<style>
/* ═══════════════════════════════════════════
   RESET & TOKENS
═══════════════════════════════════════════ */
*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}
:root{
  --bg:#1a1a2e;
  --surface:#16213e;
  --surface2:#0f3460;
  --card:#1e2a4a;
  --accent:#e94560;
  --accent2:#c73652;
  --text:#ffffff;
  --text2:#a8b4cc;
  --muted:#5a6a8a;
  --green:#4ade80;
  --yellow:#fbbf24;
  --red:#f87171;
  --blue:#60a5fa;
  --border:rgba(255,255,255,0.08);
  --radius:16px;
  --radius-sm:10px;
  --fab-size:56px;
  --bar-h:60px;
  --sheet-z:500;
  --fab-z:400;
  --overlay-z:450;
}
html,body{height:100%;overflow:hidden;background:var(--bg);color:var(--text);
  font-family:'Segoe UI',system-ui,-apple-system,sans-serif;font-size:15px}

/* ═══════════════════════════════════════════
   RIPPLE
═══════════════════════════════════════════ */
.ripple-host{position:relative;overflow:hidden}
.ripple{position:absolute;border-radius:50%;background:rgba(255,255,255,0.18);
  transform:scale(0);animation:ripple-anim .5s linear;pointer-events:none}
@keyframes ripple-anim{to{transform:scale(4);opacity:0}}

/* ═══════════════════════════════════════════
   APP SHELL — two screens that slide
═══════════════════════════════════════════ */
#app{width:100%;height:100%;position:relative;overflow:hidden}

.screen{position:absolute;inset:0;display:flex;flex-direction:column;
  transition:transform .3s cubic-bezier(.4,0,.2,1);will-change:transform}

#screen-list{transform:translateX(0)}
#screen-editor{transform:translateX(100%)}

#app.editor-open #screen-list{transform:translateX(-100%)}
#app.editor-open #screen-editor{transform:translateX(0)}

/* ═══════════════════════════════════════════
   TOP APP BAR
═══════════════════════════════════════════ */
.app-bar{
  height:var(--bar-h);
  background:var(--surface);
  display:flex;align-items:center;gap:10px;
  padding:0 12px;
  flex-shrink:0;
  border-bottom:1px solid var(--border);
  z-index:10;
}
.app-bar-title{font-size:1rem;font-weight:700;flex:1;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.app-bar .logo-emoji{font-size:1.3rem}

.icon-btn{
  width:40px;height:40px;border:none;background:transparent;color:var(--text2);
  border-radius:50%;cursor:pointer;display:flex;align-items:center;justify-content:center;
  font-size:1.2rem;flex-shrink:0;transition:background .15s;
}
.icon-btn:hover{background:var(--border)}

/* Timer ring */
#timer-wrap{position:relative;width:38px;height:38px;flex-shrink:0;cursor:default}
#timer-svg{position:absolute;inset:0;transform:rotate(-90deg)}
#timer-ring-bg{fill:none;stroke:var(--border);stroke-width:3}
#timer-ring{fill:none;stroke:var(--green);stroke-width:3;
  stroke-linecap:round;transition:stroke-dashoffset .9s linear,stroke .5s}
#timer-text{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;
  font-size:.55rem;font-weight:700;color:var(--text2);line-height:1}

/* ═══════════════════════════════════════════
   SEARCH BAR
═══════════════════════════════════════════ */
#search-wrap{
  padding:8px 12px;background:var(--surface);border-bottom:1px solid var(--border);
  flex-shrink:0;
}
#search-input{
  width:100%;background:var(--bg);border:1.5px solid var(--border);
  border-radius:24px;padding:9px 16px 9px 40px;color:var(--text);font-size:.9rem;
  outline:none;transition:border-color .2s;
}
#search-input:focus{border-color:var(--accent)}
#search-wrap .search-icon{
  position:relative;display:block;
}
#search-wrap .search-icon::before{
  content:"🔍";position:absolute;left:13px;top:50%;transform:translateY(-50%);
  font-size:.85rem;pointer-events:none;
}

/* ═══════════════════════════════════════════
   BREADCRUMB CHIPS
═══════════════════════════════════════════ */
#breadcrumb-wrap{
  display:flex;align-items:center;gap:6px;
  padding:8px 12px;overflow-x:auto;flex-shrink:0;
  scrollbar-width:none;-webkit-overflow-scrolling:touch;
}
#breadcrumb-wrap::-webkit-scrollbar{display:none}
.bc-chip{
  background:var(--surface2);border-radius:20px;padding:4px 12px;
  font-size:.78rem;color:var(--text2);cursor:pointer;white-space:nowrap;
  border:1px solid var(--border);transition:background .15s,color .15s;flex-shrink:0;
}
.bc-chip:last-child{background:var(--accent);color:#fff;border-color:var(--accent)}
.bc-sep{color:var(--muted);font-size:.7rem;flex-shrink:0}

/* ═══════════════════════════════════════════
   FILE LIST
═══════════════════════════════════════════ */
#file-list-wrap{flex:1;overflow-y:auto;padding:4px 0 80px;
  -webkit-overflow-scrolling:touch;overscroll-behavior:contain}
#file-list-wrap::-webkit-scrollbar{width:3px}
#file-list-wrap::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}

/* View toggle */
#view-toolbar{
  display:flex;align-items:center;justify-content:space-between;
  padding:4px 12px 2px;flex-shrink:0;
}
.view-toggle{display:flex;gap:4px}
.view-btn{width:32px;height:32px;border:none;background:transparent;
  color:var(--muted);border-radius:8px;cursor:pointer;font-size:1rem;
  display:flex;align-items:center;justify-content:center;transition:all .15s}
.view-btn.active{background:var(--surface2);color:var(--accent)}
#item-count{font-size:.78rem;color:var(--muted)}

/* ── LIST VIEW ── */
.file-item{
  display:flex;align-items:center;gap:12px;
  padding:10px 14px;min-height:64px;cursor:pointer;
  border-bottom:1px solid var(--border);
  transition:background .15s;
  animation:fade-slide .3s both;
}
.file-item:active{background:rgba(233,69,96,0.1)}
.file-item .fi-icon-wrap{
  width:44px;height:44px;border-radius:12px;
  display:flex;align-items:center;justify-content:center;
  font-size:1.5rem;flex-shrink:0;
}
/* Folder icon bg colors */
.fi-icon-wrap.folder{background:rgba(251,191,36,0.15)}
.fi-icon-wrap.py    {background:rgba(96,165,250,0.15)}
.fi-icon-wrap.js    {background:rgba(251,191,36,0.15)}
.fi-icon-wrap.html  {background:rgba(251,146,60,0.15)}
.fi-icon-wrap.css   {background:rgba(139,92,246,0.15)}
.fi-icon-wrap.json  {background:rgba(52,211,153,0.15)}
.fi-icon-wrap.md    {background:rgba(148,163,184,0.15)}
.fi-icon-wrap.img   {background:rgba(236,72,153,0.15)}
.fi-icon-wrap.media {background:rgba(167,139,250,0.15)}
.fi-icon-wrap.code  {background:rgba(96,165,250,0.12)}
.fi-icon-wrap.generic{background:rgba(148,163,184,0.1)}

.fi-info{flex:1;min-width:0}
.fi-name{font-size:.95rem;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.fi-meta{font-size:.75rem;color:var(--muted);margin-top:2px;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.fi-right{display:flex;flex-direction:column;align-items:flex-end;gap:4px;flex-shrink:0}
.fi-size{font-size:.75rem;color:var(--text2)}
.fi-more{width:32px;height:32px;border:none;background:transparent;color:var(--muted);
  border-radius:50%;cursor:pointer;font-size:1.1rem;display:flex;align-items:center;
  justify-content:center;transition:background .15s}
.fi-more:hover,.fi-more:active{background:var(--border);color:var(--text)}
.folder-count{font-size:.72rem;color:var(--accent);background:rgba(233,69,96,0.12);
  border-radius:10px;padding:2px 7px}

/* ── GRID VIEW ── */
#file-list-wrap.grid-view{display:grid;
  grid-template-columns:repeat(auto-fill,minmax(100px,1fr));
  gap:10px;padding:10px 12px 80px;align-content:start}
#file-list-wrap.grid-view .file-item{
  flex-direction:column;justify-content:center;gap:6px;
  min-height:100px;padding:10px 8px;border:1px solid var(--border);
  border-bottom:1px solid var(--border);border-radius:var(--radius-sm);
  background:var(--card);text-align:center;
}
#file-list-wrap.grid-view .fi-icon-wrap{width:48px;height:48px;margin:0 auto;font-size:1.8rem}
#file-list-wrap.grid-view .fi-info{width:100%}
#file-list-wrap.grid-view .fi-name{font-size:.8rem;text-align:center}
#file-list-wrap.grid-view .fi-meta{display:none}
#file-list-wrap.grid-view .fi-right{display:none}
#file-list-wrap.grid-view .file-item:nth-child(1){animation-delay:.02s}
#file-list-wrap.grid-view .file-item:nth-child(2){animation-delay:.04s}

/* Staggered animation */
.file-item:nth-child(1){animation-delay:.02s}
.file-item:nth-child(2){animation-delay:.04s}
.file-item:nth-child(3){animation-delay:.06s}
.file-item:nth-child(4){animation-delay:.08s}
.file-item:nth-child(5){animation-delay:.10s}
.file-item:nth-child(6){animation-delay:.12s}
.file-item:nth-child(7){animation-delay:.14s}
.file-item:nth-child(8){animation-delay:.16s}
.file-item:nth-child(9){animation-delay:.18s}
.file-item:nth-child(10){animation-delay:.20s}

@keyframes fade-slide{
  from{opacity:0;transform:translateY(14px)}
  to{opacity:1;transform:translateY(0)}
}
.empty-state{display:flex;flex-direction:column;align-items:center;
  justify-content:center;padding:60px 20px;gap:12px;color:var(--muted)}
.empty-state .es-icon{font-size:3rem}
.empty-state p{font-size:.9rem;text-align:center}

/* ═══════════════════════════════════════════
   FAB SPEED DIAL
═══════════════════════════════════════════ */
#fab-wrap{
  position:absolute;bottom:24px;right:16px;
  display:flex;flex-direction:column-reverse;align-items:flex-end;gap:12px;
  z-index:var(--fab-z);
}
.fab-main{
  width:var(--fab-size);height:var(--fab-size);border-radius:50%;
  background:var(--accent);border:none;color:#fff;font-size:1.5rem;
  cursor:pointer;box-shadow:0 4px 20px rgba(233,69,96,.5);
  display:flex;align-items:center;justify-content:center;
  transition:transform .25s cubic-bezier(.4,0,.2,1),background .15s;
  flex-shrink:0;
}
.fab-main:active{transform:scale(.93)}
#fab-wrap.open .fab-main{transform:rotate(45deg);background:var(--accent2)}
.fab-mini-group{
  display:flex;flex-direction:column;gap:10px;align-items:flex-end;
  transform-origin:bottom right;
  transition:opacity .2s,transform .25s cubic-bezier(.4,0,.2,1);
  opacity:0;transform:scale(.7) translateY(20px);pointer-events:none;
}
#fab-wrap.open .fab-mini-group{opacity:1;transform:scale(1) translateY(0);pointer-events:auto}
.fab-mini-row{display:flex;align-items:center;gap:10px;justify-content:flex-end}
.fab-mini-label{
  background:var(--surface);border:1px solid var(--border);
  border-radius:8px;padding:5px 12px;font-size:.82rem;
  color:var(--text);white-space:nowrap;
  box-shadow:0 2px 8px rgba(0,0,0,.4);
}
.fab-mini{
  width:42px;height:42px;border-radius:50%;border:none;color:#fff;
  cursor:pointer;font-size:1.1rem;display:flex;align-items:center;
  justify-content:center;box-shadow:0 2px 10px rgba(0,0,0,.3);
  transition:transform .15s;flex-shrink:0;
}
.fab-mini:active{transform:scale(.9)}
.fab-mini.new-file{background:#3b82f6}
.fab-mini.new-folder{background:#f59e0b}
.fab-mini.upload{background:#10b981}

/* FAB backdrop */
#fab-backdrop{
  position:absolute;inset:0;z-index:calc(var(--fab-z) - 1);
  background:rgba(26,26,46,.7);backdrop-filter:blur(2px);
  display:none;
}
#fab-wrap.open ~ #fab-backdrop{display:block}

/* ═══════════════════════════════════════════
   BOTTOM SHEET
═══════════════════════════════════════════ */
#sheet-overlay{
  position:fixed;inset:0;background:rgba(0,0,0,.6);
  z-index:var(--overlay-z);display:none;
  animation:overlay-in .2s;
}
@keyframes overlay-in{from{opacity:0}to{opacity:1}}
#bottom-sheet{
  position:fixed;bottom:0;left:0;right:0;
  background:var(--surface);border-radius:20px 20px 0 0;
  z-index:var(--sheet-z);
  transform:translateY(100%);
  transition:transform .3s cubic-bezier(.4,0,.2,1);
  max-height:85vh;overflow-y:auto;
  padding-bottom:env(safe-area-inset-bottom,16px);
}
#bottom-sheet.open{transform:translateY(0)}
.sheet-handle{width:36px;height:4px;background:var(--muted);
  border-radius:2px;margin:10px auto 4px;flex-shrink:0}
.sheet-header{display:flex;align-items:center;gap:12px;padding:12px 16px 8px}
.sheet-file-icon{width:48px;height:48px;border-radius:12px;
  font-size:1.6rem;display:flex;align-items:center;justify-content:center}
.sheet-file-info{flex:1;min-width:0}
.sheet-file-name{font-size:1rem;font-weight:600;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.sheet-file-meta{font-size:.78rem;color:var(--muted);margin-top:2px}
.sheet-actions{padding:4px 0 8px}
.sheet-action{
  display:flex;align-items:center;gap:14px;
  padding:15px 20px;cursor:pointer;font-size:.95rem;
  transition:background .12s;
}
.sheet-action:active{background:rgba(255,255,255,.05)}
.sheet-action .sa-icon{font-size:1.2rem;width:28px;text-align:center;flex-shrink:0}
.sheet-action.danger{color:var(--red)}
.sheet-divider{height:1px;background:var(--border);margin:4px 0}

/* ═══════════════════════════════════════════
   INPUT BOTTOM SHEET (modal)
═══════════════════════════════════════════ */
#input-sheet-overlay{
  position:fixed;inset:0;background:rgba(0,0,0,.6);
  z-index:600;display:none;
}
#input-sheet{
  position:fixed;bottom:0;left:0;right:0;
  background:var(--surface);border-radius:20px 20px 0 0;
  z-index:601;transform:translateY(100%);
  transition:transform .3s cubic-bezier(.4,0,.2,1);
  padding:16px 20px calc(env(safe-area-inset-bottom,16px) + 16px);
}
#input-sheet.open{transform:translateY(0)}
#input-sheet h3{font-size:1rem;font-weight:700;margin-bottom:14px}
#input-sheet input{
  width:100%;background:var(--bg);border:1.5px solid var(--border);
  border-radius:12px;padding:12px 16px;color:var(--text);font-size:1rem;
  outline:none;margin-bottom:14px;transition:border-color .2s;
}
#input-sheet input:focus{border-color:var(--accent)}
.input-sheet-btns{display:flex;gap:10px}
.btn-sheet{
  flex:1;padding:13px;border:none;border-radius:12px;font-size:.95rem;
  font-weight:600;cursor:pointer;transition:opacity .15s;
}
.btn-sheet:active{opacity:.8}
.btn-cancel{background:var(--card);color:var(--text2)}
.btn-confirm{background:var(--accent);color:#fff}

/* ═══════════════════════════════════════════
   DRAG & DROP OVERLAY
═══════════════════════════════════════════ */
#drag-overlay{
  position:fixed;inset:0;background:rgba(26,26,46,.92);
  z-index:800;display:none;align-items:center;justify-content:center;
  flex-direction:column;gap:16px;
  border:3px dashed var(--accent);
}
#drag-overlay.active{display:flex}
#drag-overlay .dnd-icon{font-size:4rem}
#drag-overlay p{font-size:1.2rem;color:var(--text)}
#drag-overlay small{font-size:.85rem;color:var(--muted)}

/* ═══════════════════════════════════════════
   TOAST
═══════════════════════════════════════════ */
#toast{
  position:fixed;bottom:80px;left:50%;transform:translateX(-50%) translateY(60px);
  background:var(--surface);border:1px solid var(--border);border-radius:24px;
  padding:10px 20px;font-size:.88rem;z-index:9999;
  opacity:0;transition:all .3s cubic-bezier(.4,0,.2,1);
  white-space:nowrap;max-width:calc(100vw - 40px);pointer-events:none;
  box-shadow:0 4px 20px rgba(0,0,0,.5);
}
#toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
#toast.success{border-color:var(--green);color:var(--green)}
#toast.error  {border-color:var(--red);color:var(--red)}
#toast.info   {border-color:var(--blue);color:var(--blue)}

/* ═══════════════════════════════════════════
   SESSION EXPIRED FULLSCREEN
═══════════════════════════════════════════ */
#expired-screen{
  position:fixed;inset:0;background:var(--bg);z-index:9999;
  display:none;flex-direction:column;align-items:center;justify-content:center;
  gap:16px;text-align:center;padding:20px;
}
#expired-screen.show{display:flex}
#expired-screen .exp-icon{font-size:4rem}
#expired-screen h2{font-size:1.5rem;color:var(--red)}
#expired-screen p{color:var(--muted);font-size:.95rem;line-height:1.6}

/* ═══════════════════════════════════════════
   EDITOR SCREEN
═══════════════════════════════════════════ */
#editor-bar{
  height:var(--bar-h);background:var(--surface);
  display:flex;align-items:center;gap:8px;padding:0 12px;
  flex-shrink:0;border-bottom:1px solid var(--border);
}
#editor-filename{
  flex:1;font-size:.92rem;font-weight:600;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
}
#editor-filename.modified::after{content:" ●";color:var(--yellow)}
.btn-save{
  background:var(--accent);border:none;color:#fff;
  border-radius:10px;padding:8px 16px;font-size:.85rem;font-weight:700;
  cursor:pointer;white-space:nowrap;transition:opacity .15s;
}
.btn-save:disabled{opacity:.4;cursor:default}
.btn-save:not(:disabled):active{opacity:.8}

.editor-wrap{flex:1;overflow:hidden;position:relative}
.CodeMirror{height:100%!important;font-size:13px;line-height:1.65;
  background:#282a36!important;font-family:'Fira Code','Cascadia Code',monospace}
.CodeMirror-scroll{height:100%}

/* Keyboard toolbar */
#kbd-toolbar{
  background:var(--surface);border-top:1px solid var(--border);
  padding:6px 8px;display:flex;gap:6px;overflow-x:auto;flex-shrink:0;
  scrollbar-width:none;-webkit-overflow-scrolling:touch;
}
#kbd-toolbar::-webkit-scrollbar{display:none}
.kbd-btn{
  background:var(--card);border:1px solid var(--border);color:var(--text2);
  border-radius:8px;padding:6px 12px;font-size:.82rem;font-family:monospace;
  cursor:pointer;white-space:nowrap;flex-shrink:0;transition:all .15s;
}
.kbd-btn:active{background:var(--surface2);color:var(--text)}

/* hidden file input */
#file-upload-input{display:none}

/* Upload progress */
#upload-progress-wrap{
  position:fixed;bottom:70px;left:12px;right:12px;z-index:700;
  display:flex;flex-direction:column;gap:6px;pointer-events:none;
}
.upload-prog-item{
  background:var(--surface);border:1px solid var(--border);
  border-radius:10px;padding:10px 14px;font-size:.82rem;
}
.upload-prog-bar-wrap{height:3px;background:var(--border);border-radius:2px;margin-top:6px}
.upload-prog-bar{height:3px;background:var(--accent);border-radius:2px;
  transition:width .2s;width:0}
</style>
</head>
<body>

<!-- ════════════════════════════════════════
     SESSION EXPIRED
════════════════════════════════════════ -->
<div id="expired-screen">
  <div class="exp-icon">⏰</div>
  <h2>Session Expired</h2>
  <p>Your file manager session has expired.<br>Request a new link from the Telegram bot.</p>
</div>

<!-- ════════════════════════════════════════
     APP
════════════════════════════════════════ -->
<div id="app">

  <!-- ── SCREEN 1: FILE LIST ── -->
  <div id="screen-list" class="screen">

    <!-- App Bar -->
    <div class="app-bar">
      <span class="logo-emoji">🌟</span>
      <span class="app-bar-title">God Madara Files</span>
      <!-- Timer -->
      <div id="timer-wrap" title="Session time remaining">
        <svg id="timer-svg" viewBox="0 0 38 38" width="38" height="38">
          <circle id="timer-ring-bg" cx="19" cy="19" r="16"/>
          <circle id="timer-ring"    cx="19" cy="19" r="16"
            stroke-dasharray="100.53"
            stroke-dashoffset="0"/>
        </svg>
        <div id="timer-text">--:--</div>
      </div>
    </div>

    <!-- Search -->
    <div id="search-wrap">
      <label class="search-icon">
        <input id="search-input" type="text" placeholder="Search files…" oninput="filterFiles(this.value)">
      </label>
    </div>

    <!-- Breadcrumb -->
    <div id="breadcrumb-wrap"></div>

    <!-- View toolbar -->
    <div id="view-toolbar">
      <span id="item-count"></span>
      <div class="view-toggle">
        <button class="view-btn active" id="btn-list-view" onclick="setView('list')" title="List view">☰</button>
        <button class="view-btn"        id="btn-grid-view" onclick="setView('grid')" title="Grid view">⊞</button>
      </div>
    </div>

    <!-- File list -->
    <div id="file-list-wrap"></div>

    <!-- FAB -->
    <div id="fab-wrap">
      <button class="fab-main ripple-host" id="fab-main-btn" onclick="toggleFab()" aria-label="Actions">＋</button>
      <div class="fab-mini-group">
        <div class="fab-mini-row">
          <span class="fab-mini-label">Upload files</span>
          <button class="fab-mini upload ripple-host" onclick="closeFab();triggerUpload()" aria-label="Upload">⬆️</button>
        </div>
        <div class="fab-mini-row">
          <span class="fab-mini-label">New folder</span>
          <button class="fab-mini new-folder ripple-host" onclick="closeFab();newFolder()" aria-label="New Folder">📁</button>
        </div>
        <div class="fab-mini-row">
          <span class="fab-mini-label">New file</span>
          <button class="fab-mini new-file ripple-host" onclick="closeFab();newFile()" aria-label="New File">📄</button>
        </div>
      </div>
    </div>
    <div id="fab-backdrop" onclick="closeFab()"></div>

  </div><!-- /screen-list -->

  <!-- ── SCREEN 2: EDITOR ── -->
  <div id="screen-editor" class="screen">
    <div id="editor-bar">
      <button class="icon-btn ripple-host" onclick="closeEditor()" aria-label="Back">←</button>
      <span id="editor-filename">untitled</span>
      <button class="btn-save ripple-host" id="save-btn" onclick="saveFile()" disabled>Save</button>
      <button class="icon-btn ripple-host" id="dl-btn" onclick="downloadFile()" title="Download" disabled>⬇</button>
    </div>
    <div class="editor-wrap" id="editor-wrap"></div>
    <div id="kbd-toolbar">
      <button class="kbd-btn" onclick="editorInsert('\t')">⇥ Tab</button>
      <button class="kbd-btn" onclick="editorInsert('()')">( )</button>
      <button class="kbd-btn" onclick="editorInsert('[]')">[  ]</button>
      <button class="kbd-btn" onclick="editorInsert('{}')">{  }</button>
      <button class="kbd-btn" onclick="editorInsert('\"\"')">"  "</button>
      <button class="kbd-btn" onclick="editorInsert('\\'\\'')">'  '</button>
      <button class="kbd-btn" onclick="editorInsert(':')"> : </button>
      <button class="kbd-btn" onclick="editorInsert('=')"> = </button>
      <button class="kbd-btn" onclick="editorInsert('->')">&rarr;</button>
      <button class="kbd-btn" onclick="editorInsert('#')"> # </button>
      <button class="kbd-btn" onclick="editorInsert('import ')">import</button>
      <button class="kbd-btn" onclick="editorInsert('def ')" >def</button>
      <button class="kbd-btn" onclick="editorInsert('self.')">self.</button>
    </div>
  </div><!-- /screen-editor -->

</div><!-- /app -->

<!-- ════════════════════════════════════════
     BOTTOM SHEET — file actions
════════════════════════════════════════ -->
<div id="sheet-overlay" onclick="closeSheet()"></div>
<div id="bottom-sheet">
  <div class="sheet-handle"></div>
  <div class="sheet-header">
    <div class="sheet-file-icon" id="sheet-icon"></div>
    <div class="sheet-file-info">
      <div class="sheet-file-name" id="sheet-name"></div>
      <div class="sheet-file-meta" id="sheet-meta"></div>
    </div>
  </div>
  <div class="sheet-actions">
    <div class="sheet-action ripple-host" onclick="sheetOpen()">
      <span class="sa-icon">📂</span> Open
    </div>
    <div class="sheet-action ripple-host" onclick="sheetRename()">
      <span class="sa-icon">✏️</span> Rename
    </div>
    <div class="sheet-action ripple-host" id="sheet-download-btn" onclick="sheetDownload()">
      <span class="sa-icon">⬇️</span> Download
    </div>
    <div class="sheet-action ripple-host" onclick="sheetCopyPath()">
      <span class="sa-icon">📋</span> Copy path
    </div>
    <div class="sheet-divider"></div>
    <div class="sheet-action danger ripple-host" onclick="sheetDelete()">
      <span class="sa-icon">🗑</span> Delete
    </div>
  </div>
</div>

<!-- ════════════════════════════════════════
     INPUT BOTTOM SHEET
════════════════════════════════════════ -->
<div id="input-sheet-overlay" onclick="closeInputSheet()"></div>
<div id="input-sheet">
  <div class="sheet-handle"></div>
  <h3 id="input-sheet-title">Input</h3>
  <input type="text" id="input-sheet-field" placeholder="">
  <div class="input-sheet-btns">
    <button class="btn-sheet btn-cancel" onclick="closeInputSheet()">Cancel</button>
    <button class="btn-sheet btn-confirm" onclick="confirmInputSheet()">OK</button>
  </div>
</div>

<!-- ════════════════════════════════════════
     DRAG & DROP OVERLAY
════════════════════════════════════════ -->
<div id="drag-overlay">
  <div class="dnd-icon">📂</div>
  <p>Drop files here</p>
  <small>Files will be uploaded to current folder</small>
</div>

<!-- ════════════════════════════════════════
     UPLOAD PROGRESS
════════════════════════════════════════ -->
<div id="upload-progress-wrap"></div>

<!-- Toast -->
<div id="toast"></div>

<!-- Hidden file input -->
<input type="file" id="file-upload-input" multiple onchange="uploadFiles(this.files)">

<!-- ════════════════════════════════════════
     CODEMIRROR
════════════════════════════════════════ -->
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/codemirror.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/python/python.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/javascript/javascript.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/htmlmixed/htmlmixed.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/css/css.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/shell/shell.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/addon/edit/closebrackets.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/addon/edit/matchbrackets.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/keymap/sublime.min.js"></script>

<script>
/* ═══════════════════════════════════════════════════════════
   STATE
═══════════════════════════════════════════════════════════ */
const TOKEN    = "{{ token }}";
const BASE     = `/fm/${TOKEN}`;
let currentDir  = "";
let currentFile = null;
let sheetTarget = null;
let editor      = null;
let modified    = false;
let expiresAt   = {{ expires_at }};
let sessionTotal= 0;
let inputCb     = null;
let viewMode    = "list";
let allItems    = [];   // raw item list for current dir

/* ═══════════════════════════════════════════════════════════
   TIMER
═══════════════════════════════════════════════════════════ */
(function initTimer(){
  const ring = document.getElementById("timer-ring");
  const txt  = document.getElementById("timer-text");
  const CIRC = 100.53; // 2π×16

  function tick(){
    const remaining = Math.max(0, expiresAt - Math.floor(Date.now()/1000));
    const m = String(Math.floor(remaining/60)).padStart(2,"0");
    const s = String(remaining%60).padStart(2,"0");
    txt.textContent = `${m}:${s}`;

    // progress ring
    if(sessionTotal > 0){
      const frac = remaining / sessionTotal;
      ring.style.strokeDashoffset = CIRC * (1-frac);
      ring.style.stroke = remaining < 120 ? "var(--red)" : remaining < 300 ? "var(--yellow)" : "var(--green)";
    }

    if(remaining === 0){
      document.getElementById("expired-screen").classList.add("show");
    }
  }

  // Estimate session total from expires_at (assume 30 min default)
  sessionTotal = 1800;
  tick();
  setInterval(tick, 1000);
})();

/* ═══════════════════════════════════════════════════════════
   TOAST
═══════════════════════════════════════════════════════════ */
let toastTimer;
function toast(msg, type="success"){
  const el = document.getElementById("toast");
  el.textContent = msg;
  el.className = `show ${type}`;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(()=>{ el.className=""; }, 3000);
}

/* ═══════════════════════════════════════════════════════════
   RIPPLE
═══════════════════════════════════════════════════════════ */
document.addEventListener("pointerdown", e=>{
  const host = e.target.closest(".ripple-host");
  if(!host) return;
  const r = document.createElement("span");
  r.className = "ripple";
  const rect = host.getBoundingClientRect();
  const sz = Math.max(rect.width, rect.height) * 2;
  r.style.cssText = `width:${sz}px;height:${sz}px;left:${e.clientX-rect.left-sz/2}px;top:${e.clientY-rect.top-sz/2}px`;
  host.appendChild(r);
  r.addEventListener("animationend", ()=>r.remove());
});

/* ═══════════════════════════════════════════════════════════
   API HELPER
═══════════════════════════════════════════════════════════ */
async function api(endpoint, opts={}){
  try{
    const res = await fetch(`${BASE}/api/${endpoint}`, opts);
    if(res.status===401){ toast("Session expired!","error"); return null; }
    return res;
  }catch(e){
    toast("Network error","error");
    return null;
  }
}

/* ═══════════════════════════════════════════════════════════
   FILE UTILITIES
═══════════════════════════════════════════════════════════ */
function fileIcon(name, type){
  if(type==="dir") return "📁";
  const ext = name.split(".").pop().toLowerCase();
  const m = {
    py:"🐍",js:"🟨",ts:"🔷",html:"🌐",css:"🎨",
    json:"📋",md:"📝",txt:"📄",sh:"⚙️",bash:"⚙️",
    env:"🔐",log:"📜",zip:"📦",tar:"📦",gz:"📦",
    png:"🖼",jpg:"🖼",jpeg:"🖼",gif:"🖼",svg:"🖼",webp:"🖼",
    mp4:"🎬",mov:"🎬",mp3:"🎵",wav:"🎵",
    pdf:"📕",csv:"📊",xml:"📰",yml:"⚙️",yaml:"⚙️",
    toml:"⚙️",cfg:"⚙️",ini:"⚙️",
    sql:"🗄",db:"🗄",
  };
  return m[ext]||"📄";
}

function iconClass(name, type){
  if(type==="dir") return "folder";
  const ext = name.split(".").pop().toLowerCase();
  if(["py"].includes(ext))                return "py";
  if(["js","ts"].includes(ext))           return "js";
  if(["html","htm"].includes(ext))        return "html";
  if(["css","scss","sass"].includes(ext)) return "css";
  if(["json","yaml","yml","toml"].includes(ext)) return "json";
  if(["md","txt","log","csv"].includes(ext))     return "md";
  if(["png","jpg","jpeg","gif","svg","webp"].includes(ext)) return "img";
  if(["mp4","mp3","mov","wav"].includes(ext))    return "media";
  if(["sh","bash","env","cfg","ini"].includes(ext)) return "code";
  return "generic";
}

function humanSize(bytes){
  if(!bytes && bytes!==0) return "";
  if(bytes<1024) return `${bytes} B`;
  if(bytes<1048576) return `${(bytes/1024).toFixed(1)} KB`;
  if(bytes<1073741824) return `${(bytes/1048576).toFixed(1)} MB`;
  return `${(bytes/1073741824).toFixed(2)} GB`;
}

function humanTime(ts){
  if(!ts) return "";
  const d = new Date(ts*1000);
  const now = new Date();
  const diff = (now-d)/1000;
  if(diff<60)   return "Just now";
  if(diff<3600) return `${Math.floor(diff/60)}m ago`;
  if(diff<86400)return `${Math.floor(diff/3600)}h ago`;
  return d.toLocaleDateString();
}

/* ═══════════════════════════════════════════════════════════
   BREADCRUMB
═══════════════════════════════════════════════════════════ */
function renderBreadcrumb(dir){
  const wrap = document.getElementById("breadcrumb-wrap");
  const parts = dir ? dir.split("/").filter(Boolean) : [];
  let html = `<span class="bc-chip" onclick="listDir('')">~ Home</span>`;
  let cum = "";
  parts.forEach(p=>{
    cum = cum ? `${cum}/${p}` : p;
    const cp = cum;
    html += `<span class="bc-sep">›</span>
             <span class="bc-chip" onclick="listDir('${cp}')">${p}</span>`;
  });
  wrap.innerHTML = html;
  // scroll to end
  setTimeout(()=>{ wrap.scrollLeft = wrap.scrollWidth; }, 50);
}

/* ═══════════════════════════════════════════════════════════
   LIST DIR
═══════════════════════════════════════════════════════════ */
async function listDir(dir){
  currentDir = dir;
  renderBreadcrumb(dir);
  const res = await api(`list?dir=${encodeURIComponent(dir)}`);
  if(!res) return;
  const data = await res.json();
  if(!data.success){ toast(data.error,"error"); return; }
  allItems = data.items || [];
  renderFileList(allItems);
}

function renderFileList(items){
  const wrap = document.getElementById("file-list-wrap");
  wrap.className = viewMode==="grid" ? "grid-view" : "";

  // Update count
  document.getElementById("item-count").textContent =
    `${items.length} item${items.length!==1?"s":""}`;

  if(items.length===0){
    wrap.innerHTML = `<div class="empty-state">
      <span class="es-icon">📭</span>
      <p>This folder is empty</p>
    </div>`;
    return;
  }

  wrap.innerHTML = "";

  // Back button (not in grid view root)
  if(currentDir !== ""){
    const back = makeFileItem({name:"..", type:"back", path:"", size:0}, true);
    wrap.appendChild(back);
  }

  items.forEach(item=>{
    wrap.appendChild(makeFileItem(item, false));
  });
}

function makeFileItem(item, isBack){
  const el = document.createElement("div");
  el.className = "file-item ripple-host";
  el.dataset.path = item.path;
  el.dataset.name = item.name;
  el.dataset.type = item.type;

  if(isBack){
    el.innerHTML = `
      <div class="fi-icon-wrap generic">⬆️</div>
      <div class="fi-info">
        <div class="fi-name">..</div>
        <div class="fi-meta">Parent folder</div>
      </div>`;
    el.onclick = ()=>{
      const parts = currentDir.split("/").filter(Boolean);
      parts.pop();
      listDir(parts.join("/"));
    };
    return el;
  }

  const icon  = fileIcon(item.name, item.type);
  const cls   = iconClass(item.name, item.type);
  const size  = item.type==="file" ? humanSize(item.size) : "";
  const meta  = item.type==="dir"  ? "Folder" : size;
  const right = item.type==="file"
    ? `<div class="fi-right">
         <span class="fi-size">${size}</span>
         <button class="fi-more" onclick="openSheet(event,'${esc(item.path)}','${esc(item.name)}','${item.type}',${item.size||0})" aria-label="More">⋮</button>
       </div>`
    : `<div class="fi-right">
         <button class="fi-more" onclick="openSheet(event,'${esc(item.path)}','${esc(item.name)}','${item.type}',0)" aria-label="More">⋮</button>
       </div>`;

  el.innerHTML = `
    <div class="fi-icon-wrap ${cls}">${icon}</div>
    <div class="fi-info">
      <div class="fi-name">${item.name}</div>
      <div class="fi-meta">${meta}</div>
    </div>
    ${right}`;

  el.onclick = (e)=>{
    if(e.target.classList.contains("fi-more")) return;
    if(item.type==="dir") listDir(item.path);
    else openFile(item.path, item.name);
  };

  return el;
}

function esc(s){ return s.replace(/'/g,"\\'").replace(/"/g,"&quot;"); }

/* ═══════════════════════════════════════════════════════════
   FILTER / SEARCH
═══════════════════════════════════════════════════════════ */
function filterFiles(q){
  const filtered = q
    ? allItems.filter(i=>i.name.toLowerCase().includes(q.toLowerCase()))
    : allItems;
  renderFileList(filtered);
}

/* ═══════════════════════════════════════════════════════════
   VIEW TOGGLE
═══════════════════════════════════════════════════════════ */
function setView(mode){
  viewMode = mode;
  document.getElementById("btn-list-view").classList.toggle("active", mode==="list");
  document.getElementById("btn-grid-view").classList.toggle("active", mode==="grid");
  renderFileList(allItems);
}

/* ═══════════════════════════════════════════════════════════
   FAB
═══════════════════════════════════════════════════════════ */
function toggleFab(){
  const fab = document.getElementById("fab-wrap");
  fab.classList.toggle("open");
}
function closeFab(){
  document.getElementById("fab-wrap").classList.remove("open");
}

/* ═══════════════════════════════════════════════════════════
   BOTTOM SHEET
═══════════════════════════════════════════════════════════ */
function openSheet(e, path, name, type, size){
  e.stopPropagation();
  sheetTarget = {path, name, type, size};
  document.getElementById("sheet-icon").textContent = fileIcon(name,type);
  document.getElementById("sheet-icon").className = `sheet-file-icon ${iconClass(name,type)}`;
  document.getElementById("sheet-name").textContent = name;
  document.getElementById("sheet-meta").textContent =
    type==="dir" ? "Folder" : humanSize(size);
  document.getElementById("sheet-download-btn").style.opacity = type==="dir"?"0.3":"1";
  document.getElementById("sheet-download-btn").style.pointerEvents = type==="dir"?"none":"auto";
  document.getElementById("sheet-overlay").style.display = "block";
  setTimeout(()=>document.getElementById("bottom-sheet").classList.add("open"),10);
}

function closeSheet(){
  document.getElementById("bottom-sheet").classList.remove("open");
  setTimeout(()=>{ document.getElementById("sheet-overlay").style.display="none"; },300);
}

function sheetOpen(){
  closeSheet();
  if(!sheetTarget) return;
  if(sheetTarget.type==="dir") listDir(sheetTarget.path);
  else openFile(sheetTarget.path, sheetTarget.name);
}

function sheetRename(){
  closeSheet();
  if(!sheetTarget) return;
  showInputSheet(`✏️ Rename "${sheetTarget.name}"`, sheetTarget.name, async(newName)=>{
    const dir = sheetTarget.path.split("/").slice(0,-1).join("/");
    const newPath = dir ? `${dir}/${newName}` : newName;
    const res = await api("rename",{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({old_path:sheetTarget.path, new_path:newPath}),
    });
    if(!res) return;
    const d = await res.json();
    if(d.success){ toast("✅ Renamed"); listDir(currentDir); }
    else toast(d.error,"error");
  });
}

function sheetDownload(){
  closeSheet();
  if(!sheetTarget || sheetTarget.type==="dir") return;
  downloadFile(sheetTarget.path);
}

function sheetCopyPath(){
  closeSheet();
  if(!sheetTarget) return;
  navigator.clipboard.writeText(sheetTarget.path).then(()=>{
    toast("📋 Path copied","info");
  }).catch(()=>{ toast(`Path: ${sheetTarget.path}`,"info"); });
}

async function sheetDelete(){
  closeSheet();
  if(!sheetTarget) return;
  const target = sheetTarget;
  // Use input sheet as confirm dialog
  showInputSheet(
    `🗑 Type "${target.name}" to confirm delete`,
    "",
    async(val)=>{
      if(val.trim() !== target.name){
        toast("Name didn't match — cancelled","error");
        return;
      }
      const res = await api("delete",{
        method:"POST",
        headers:{"Content-Type":"application/json"},
        body:JSON.stringify({path:target.path}),
      });
      if(!res) return;
      const d = await res.json();
      if(d.success){
        toast("🗑 Deleted");
        if(currentFile===target.path) closeEditor();
        listDir(currentDir);
      } else toast(d.error,"error");
    }
  );
}

/* ═══════════════════════════════════════════════════════════
   INPUT BOTTOM SHEET
═══════════════════════════════════════════════════════════ */
function showInputSheet(title, prefill, cb){
  inputCb = cb;
  document.getElementById("input-sheet-title").textContent = title;
  const inp = document.getElementById("input-sheet-field");
  inp.value = prefill;
  inp.placeholder = prefill || "Enter value…";
  document.getElementById("input-sheet-overlay").style.display = "block";
  setTimeout(()=>{
    document.getElementById("input-sheet").classList.add("open");
    inp.focus();
    inp.select();
  },10);
}

function closeInputSheet(){
  document.getElementById("input-sheet").classList.remove("open");
  setTimeout(()=>{ document.getElementById("input-sheet-overlay").style.display="none"; },300);
  inputCb = null;
}

function confirmInputSheet(){
  const val = document.getElementById("input-sheet-field").value;
  closeInputSheet();
  if(inputCb) inputCb(val);
}

document.getElementById("input-sheet-field").addEventListener("keydown", e=>{
  if(e.key==="Enter") confirmInputSheet();
  if(e.key==="Escape") closeInputSheet();
});

/* ═══════════════════════════════════════════════════════════
   NEW FILE / FOLDER
═══════════════════════════════════════════════════════════ */
function newFile(){
  showInputSheet("📄 New File Name","untitled.py", async(name)=>{
    if(!name.trim()) return;
    const path = currentDir ? `${currentDir}/${name.trim()}` : name.trim();
    const res = await api("write",{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({path, content:""}),
    });
    if(!res) return;
    const d = await res.json();
    if(d.success){ toast("✅ File created"); listDir(currentDir); }
    else toast(d.error,"error");
  });
}

function newFolder(){
  showInputSheet("📁 New Folder Name","new_folder", async(name)=>{
    if(!name.trim()) return;
    const path = currentDir ? `${currentDir}/${name.trim()}` : name.trim();
    const res = await api("mkdir",{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({path}),
    });
    if(!res) return;
    const d = await res.json();
    if(d.success){ toast("✅ Folder created"); listDir(currentDir); }
    else toast(d.error,"error");
  });
}

/* ═══════════════════════════════════════════════════════════
   UPLOAD
═══════════════════════════════════════════════════════════ */
function triggerUpload(){
  document.getElementById("file-upload-input").click();
}

async function uploadFiles(files){
  if(!files || !files.length) return;
  const wrap = document.getElementById("upload-progress-wrap");

  for(const file of files){
    const item = document.createElement("div");
    item.className = "upload-prog-item";
    item.innerHTML = `<div>${file.name}</div>
      <div class="upload-prog-bar-wrap">
        <div class="upload-prog-bar" style="width:30%"></div>
      </div>`;
    wrap.appendChild(item);

    const fd = new FormData();
    fd.append("file", file);
    fd.append("dir", currentDir);
    const bar = item.querySelector(".upload-prog-bar");
    bar.style.width = "60%";

    const res = await api("upload",{method:"POST",body:fd});
    bar.style.width = "100%";
    if(res){
      const d = await res.json();
      if(!d.success){ toast(`❌ ${file.name}: ${d.error}`,"error"); }
    }
    setTimeout(()=>item.remove(), 1500);
  }

  toast(`✅ ${files.length} file(s) uploaded`);
  listDir(currentDir);
  document.getElementById("file-upload-input").value="";
}

/* ═══════════════════════════════════════════════════════════
   DRAG & DROP
═══════════════════════════════════════════════════════════ */
const dragOverlay = document.getElementById("drag-overlay");
let dragDepth = 0;

document.addEventListener("dragenter", e=>{
  e.preventDefault();
  dragDepth++;
  dragOverlay.classList.add("active");
});
document.addEventListener("dragleave", ()=>{
  dragDepth--;
  if(dragDepth<=0){ dragDepth=0; dragOverlay.classList.remove("active"); }
});
document.addEventListener("dragover", e=>e.preventDefault());
document.addEventListener("drop", e=>{
  e.preventDefault();
  dragDepth=0;
  dragOverlay.classList.remove("active");
  if(e.dataTransfer.files.length) uploadFiles(e.dataTransfer.files);
});

/* ═══════════════════════════════════════════════════════════
   OPEN FILE / EDITOR
═══════════════════════════════════════════════════════════ */
async function openFile(path, name){
  const res = await api(`read?path=${encodeURIComponent(path)}`);
  if(!res) return;
  const data = await res.json();
  if(!data.success){ toast(data.error,"error"); return; }

  currentFile = path;
  modified    = false;

  const label = document.getElementById("editor-filename");
  label.textContent = name;
  label.classList.remove("modified");

  document.getElementById("save-btn").disabled = false;
  document.getElementById("dl-btn").disabled   = false;

  // Build editor
  const wrap = document.getElementById("editor-wrap");
  wrap.innerHTML = `<textarea id="cm-editor"></textarea>`;

  editor = CodeMirror.fromTextArea(document.getElementById("cm-editor"),{
    value:             data.content,
    mode:              detectMode(name),
    theme:             "dracula",
    lineNumbers:       true,
    matchBrackets:     true,
    autoCloseBrackets: true,
    keyMap:            "sublime",
    tabSize:           4,
    indentWithTabs:    false,
    extraKeys:{
      "Ctrl-S": saveFile,
      "Cmd-S":  saveFile,
    }
  });
  editor.setValue(data.content);
  editor.clearHistory();
  editor.on("change",()=>{
    if(!modified){
      modified=true;
      document.getElementById("editor-filename").classList.add("modified");
    }
  });

  // Slide to editor
  document.getElementById("app").classList.add("editor-open");
  setTimeout(()=>{ editor.setSize("100%","100%"); editor.refresh(); },350);
}

function closeEditor(){
  document.getElementById("app").classList.remove("editor-open");
  // Refresh file list after edit
  setTimeout(()=>listDir(currentDir), 300);
}

function detectMode(name){
  const ext = name.split(".").pop().toLowerCase();
  const m = {py:"python",js:"javascript",ts:"javascript",
    html:"htmlmixed",htm:"htmlmixed",css:"css",
    sh:"shell",bash:"shell"};
  return m[ext]||"text/plain";
}

/* ═══════════════════════════════════════════════════════════
   SAVE
═══════════════════════════════════════════════════════════ */
async function saveFile(){
  if(!currentFile||!editor) return;
  const btn = document.getElementById("save-btn");
  btn.textContent = "Saving…";
  const res = await api("write",{
    method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({path:currentFile, content:editor.getValue()}),
  });
  btn.textContent = "Save";
  if(!res) return;
  const d = await res.json();
  if(d.success){
    modified=false;
    document.getElementById("editor-filename").classList.remove("modified");
    toast("✅ Saved!","success");
  } else toast(d.error,"error");
}

/* ═══════════════════════════════════════════════════════════
   DOWNLOAD
═══════════════════════════════════════════════════════════ */
function downloadFile(path){
  const p = path||currentFile;
  if(!p) return;
  const a = document.createElement("a");
  a.href = `${BASE}/api/download?path=${encodeURIComponent(p)}`;
  a.download = p.split("/").pop();
  a.click();
}

/* ═══════════════════════════════════════════════════════════
   KEYBOARD INSERT (KBD TOOLBAR)
═══════════════════════════════════════════════════════════ */
function editorInsert(str){
  if(!editor) return;
  editor.replaceSelection(str);
  // For paired chars: move cursor to middle
  const pairs = {"()":1,"[]":1,"{}":1,'""':1,"''":1};
  if(pairs[str]){
    const cur = editor.getCursor();
    editor.setCursor({line:cur.line, ch:cur.ch-1});
  }
  editor.focus();
}

/* ═══════════════════════════════════════════════════════════
   GLOBAL KEYBOARD
═══════════════════════════════════════════════════════════ */
document.addEventListener("keydown", e=>{
  if((e.ctrlKey||e.metaKey) && e.key==="s"){
    e.preventDefault();
    saveFile();
  }
  if(e.key==="Escape"){
    closeFab();
    closeSheet();
    closeInputSheet();
  }
});

/* ═══════════════════════════════════════════════════════════
   INIT
═══════════════════════════════════════════════════════════ */
listDir("");
</script>
</body>
</html>"""

EXPIRED_HTML = """<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>Session Expired</title>
<style>
  body{background:#1a1a2e;color:#fff;display:flex;align-items:center;
       justify-content:center;height:100vh;font-family:'Segoe UI',sans-serif;text-align:center}
  h1{color:#e94560;font-size:2rem} p{color:#a8b4cc;margin-top:1rem;line-height:1.6}
</style></head>
<body><div><h1>⏰ Session Expired</h1>
<p>Your file manager session has expired.<br>Request a new link from the Telegram bot.</p></div></body></html>"""

@app.route("/fm/<token>/")
def file_manager(token):
    data = validate_token(token)
    if not data:
        return render_template_string(EXPIRED_HTML), 401
    remaining = int(data["expires_at"] - time.time())
    expires_at_js = int(data["expires_at"])
    html = FM_HTML.replace("{{ token }}", token).replace("{{ expires_at }}", str(expires_at_js))
    return Response(html, mimetype="text/html")

# ─────────────────────────────────────────────────────────────
# REST API endpoints
# ─────────────────────────────────────────────────────────────

def get_token_data(token):
    data = validate_token(token)
    if not data:
        abort(401, "Session expired")
    return data

@app.route("/fm/<token>/api/list")
def api_list(token):
    td   = get_token_data(token)
    base = td["project_dir"]
    rel  = request.args.get("dir", "")
    path = safe_path(base, rel)
    if not path or not os.path.isdir(path):
        return jsonify({"success": False, "error": "Invalid path"})

    items = []
    try:
        for entry in sorted(os.scandir(path), key=lambda e: (not e.is_dir(), e.name.lower())):
            if is_hidden(entry.name):
                continue
            rel_path = os.path.relpath(os.path.join(path, entry.name), base)
            if entry.is_dir():
                items.append({"name": entry.name, "path": rel_path, "type": "dir", "size": 0})
            else:
                size = entry.stat().st_size
                items.append({"name": entry.name, "path": rel_path, "type": "file", "size": size})
    except PermissionError:
        return jsonify({"success": False, "error": "Permission denied"})

    return jsonify({"success": True, "items": items})

@app.route("/fm/<token>/api/read")
def api_read(token):
    td   = get_token_data(token)
    base = td["project_dir"]
    rel  = request.args.get("path", "")
    path = safe_path(base, rel)
    if not path or not os.path.isfile(path):
        return jsonify({"success": False, "error": "File not found"})
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
        return jsonify({"success": True, "content": content})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/fm/<token>/api/write", methods=["POST"])
def api_write(token):
    td   = get_token_data(token)
    base = td["project_dir"]
    body = request.get_json(force=True)
    rel  = body.get("path", "")
    content = body.get("content", "")
    path = safe_path(base, rel)
    if not path:
        return jsonify({"success": False, "error": "Invalid path"})
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/fm/<token>/api/mkdir", methods=["POST"])
def api_mkdir(token):
    td   = get_token_data(token)
    base = td["project_dir"]
    body = request.get_json(force=True)
    rel  = body.get("path", "")
    path = safe_path(base, rel)
    if not path:
        return jsonify({"success": False, "error": "Invalid path"})
    try:
        os.makedirs(path, exist_ok=True)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/fm/<token>/api/delete", methods=["POST"])
def api_delete(token):
    td   = get_token_data(token)
    base = td["project_dir"]
    body = request.get_json(force=True)
    rel  = body.get("path", "")
    path = safe_path(base, rel)
    if not path:
        return jsonify({"success": False, "error": "Invalid path"})
    if not os.path.exists(path):
        return jsonify({"success": False, "error": "Not found"})
    try:
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/fm/<token>/api/rename", methods=["POST"])
def api_rename(token):
    td   = get_token_data(token)
    base = td["project_dir"]
    body = request.get_json(force=True)
    old  = safe_path(base, body.get("old_path", ""))
    new_ = safe_path(base, body.get("new_path", ""))
    if not old or not new_:
        return jsonify({"success": False, "error": "Invalid path"})
    try:
        os.rename(old, new_)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/fm/<token>/api/upload", methods=["POST"])
def api_upload(token):
    td   = get_token_data(token)
    base = td["project_dir"]
    dir_ = request.form.get("dir", "")
    dest_dir = safe_path(base, dir_)
    if not dest_dir:
        return jsonify({"success": False, "error": "Invalid path"})
    os.makedirs(dest_dir, exist_ok=True)

    saved = []
    for f in request.files.getlist("file"):
        filename = os.path.basename(f.filename or "upload")
        dest = os.path.join(dest_dir, filename)
        try:
            f.save(dest)
            saved.append(filename)
        except Exception as e:
            return jsonify({"success": False, "error": str(e)})
    return jsonify({"success": True, "saved": saved})

@app.route("/fm/<token>/api/download")
def api_download(token):
    td   = get_token_data(token)
    base = td["project_dir"]
    rel  = request.args.get("path", "")
    path = safe_path(base, rel)
    if not path or not os.path.isfile(path):
        abort(404)
    return send_file(path, as_attachment=True, download_name=os.path.basename(path))

# ─────────────────────────────────────────────────────────────
# Start function
# ─────────────────────────────────────────────────────────────

def start_flask(port: int = 8080):
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
