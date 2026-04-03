import asyncio
import logging
import os
import platform
import secrets
import shutil
import signal
import subprocess
import sys
import time
import zipfile
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from threading import Thread

import psutil
from dotenv import load_dotenv
from flask import (
    Flask,
    abort,
    jsonify,
    render_template_string,
    request,
    send_file,
)
from motor.motor_asyncio import AsyncIOMotorClient
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

# ──────────────────────────────────────────
#  Configuration
# ──────────────────────────────────────────
BOT_TOKEN   = os.getenv("BOT_TOKEN", "")
OWNER_ID    = int(os.getenv("OWNER_ID", "0"))
MONGO_URI   = os.getenv("MONGO_URI", "mongodb://localhost:27017")
RENDER_URL  = os.getenv("RENDER_URL", "").rstrip("/")
PORT        = int(os.getenv("PORT", "5000"))
# FIX 1: Make PROJECTS_DIR absolute so Flask thread and asyncio thread agree on the same path
PROJECTS_DIR = Path("./projects").resolve()
PROJECTS_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────
#  MongoDB
# ──────────────────────────────────────────
mongo_client = AsyncIOMotorClient(MONGO_URI)
db           = mongo_client["god_madara_bot"]
users_col    = db["users"]
projects_col = db["projects"]
backups_col  = db["backups"]

# ──────────────────────────────────────────
#  ConversationHandler States
# ──────────────────────────────────────────
(
    NP_NAME,
    NP_FILES,
    EDIT_CMD,
    ADMIN_WAIT_INPUT,
    ADMIN_WAIT_INPUT_2,
) = range(5)

# ──────────────────────────────────────────
#  In-memory state
# ──────────────────────────────────────────
# running_processes: {"user_id:project_name": subprocess.Popen}
running_processes: dict[str, subprocess.Popen] = {}

# Token store: token -> {"user_id", "project_name", "expiry"}
fm_tokens: dict[str, dict] = {}

# ──────────────────────────────────────────
#  FIX 2: Custom safe_filename replaces werkzeug secure_filename
#  secure_filename() aggressively strips dots, breaking .env, main_.py etc.
# ──────────────────────────────────────────
def safe_filename(filename: str) -> str:
    """Keep the filename mostly intact, only strip path separators and null bytes."""
    # Remove directory components
    filename = filename.replace("/", "_").replace("\\", "_").replace("\x00", "")
    # Remove leading ".." sequences (path traversal), but allow single leading dot (hidden files)
    while filename.startswith(".."):
        filename = filename[2:]
    return filename.strip() or "unnamed"


# ──────────────────────────────────────────
#  File Manager HTML — FIXED JavaScript
# ──────────────────────────────────────────
FILE_MANAGER_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>📁 File Manager — {{ project_name }}</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/codemirror.min.css">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/theme/dracula.min.css">
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/codemirror.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/python/python.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/javascript/javascript.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/xml/xml.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/css/css.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/shell/shell.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/addon/edit/matchbrackets.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/addon/edit/closebrackets.min.js"></script>
<style>
  :root {
    --bg: #0d0d0d; --panel: #1a1a2e; --sidebar: #16213e;
    --accent: #e94560; --accent2: #0f3460; --text: #e0e0e0;
    --text-dim: #888; --border: #2a2a4a; --hover: #1f2b4a;
    --success: #00c875; --danger: #e94560; --warning: #f7b731;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', monospace; background: var(--bg); color: var(--text); min-height: 100vh; }
  header { background: var(--panel); border-bottom: 2px solid var(--accent); padding: 12px 20px;
    display: flex; align-items: center; justify-content: space-between; position: sticky; top: 0; z-index: 100; }
  header h1 { font-size: 18px; color: var(--accent); }
  header span { font-size: 13px; color: var(--text-dim); }
  .layout { display: flex; height: calc(100vh - 55px); }
  .sidebar { width: 280px; min-width: 200px; background: var(--sidebar); border-right: 1px solid var(--border);
    overflow-y: auto; display: flex; flex-direction: column; }
  .sidebar-header { padding: 12px 16px; background: var(--accent2); font-size: 13px; font-weight: bold;
    display: flex; justify-content: space-between; align-items: center; }
  .sidebar-actions { padding: 8px; border-bottom: 1px solid var(--border); display: flex; gap: 6px; flex-wrap: wrap; }
  .btn { padding: 6px 12px; border: none; border-radius: 6px; cursor: pointer; font-size: 12px; font-weight: 600;
    transition: opacity .15s, transform .1s; }
  .btn:hover { opacity: .85; transform: translateY(-1px); }
  .btn-primary { background: var(--accent); color: #fff; }
  .btn-success { background: var(--success); color: #000; }
  .btn-danger { background: var(--danger); color: #fff; }
  .btn-warning { background: var(--warning); color: #000; }
  .btn-secondary { background: var(--accent2); color: var(--text); }
  .file-list { list-style: none; padding: 8px 0; flex: 1; overflow-y: auto; }
  .file-item { display: flex; align-items: center; justify-content: space-between;
    padding: 7px 14px; cursor: pointer; border-radius: 4px; margin: 1px 6px; transition: background .15s; }
  .file-item:hover { background: var(--hover); }
  .file-item.active { background: var(--accent2); border-left: 3px solid var(--accent); }
  .file-item .file-name { font-size: 13px; flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .file-item .file-icon { margin-right: 8px; font-size: 14px; }
  .file-actions { display: flex; gap: 4px; opacity: 0; transition: opacity .15s; }
  .file-item:hover .file-actions { opacity: 1; }
  .file-actions button { background: none; border: none; cursor: pointer; font-size: 14px; padding: 2px 4px; border-radius: 3px; }
  .file-actions button:hover { background: rgba(255,255,255,.1); }
  .editor-panel { flex: 1; display: flex; flex-direction: column; background: var(--bg); }
  .editor-toolbar { padding: 8px 14px; background: var(--panel); border-bottom: 1px solid var(--border);
    display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
  .editor-toolbar .current-file { font-size: 13px; color: var(--accent); flex: 1; }
  .CodeMirror { height: 100%; font-size: 14px; font-family: 'Fira Code', 'Courier New', monospace; }
  .editor-wrap { flex: 1; overflow: hidden; }
  .editor-wrap .CodeMirror { height: 100%; }
  .welcome { flex: 1; display: flex; flex-direction: column; align-items: center; justify-content: center;
    color: var(--text-dim); font-size: 16px; gap: 12px; }
  .welcome span { font-size: 56px; }
  .upload-area { padding: 8px; border-bottom: 1px solid var(--border); }
  .upload-label { display: block; padding: 8px; border: 2px dashed var(--border);
    text-align: center; border-radius: 6px; cursor: pointer; font-size: 12px; color: var(--text-dim);
    transition: border-color .15s; }
  .upload-label:hover { border-color: var(--accent); color: var(--text); }
  #upload-input { display: none; }
  .toast-container { position: fixed; bottom: 20px; right: 20px; z-index: 9999; display: flex; flex-direction: column; gap: 8px; }
  .toast { padding: 12px 18px; border-radius: 8px; font-size: 13px; min-width: 220px; animation: slideIn .3s ease; }
  .toast-success { background: var(--success); color: #000; }
  .toast-error { background: var(--danger); color: #fff; }
  .toast-info { background: var(--accent2); color: var(--text); }
  @keyframes slideIn { from { transform: translateX(120%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }
  .modal-overlay { display:none; position:fixed; inset:0; background:rgba(0,0,0,.7); z-index:200; align-items:center; justify-content:center; }
  .modal-overlay.open { display: flex; }
  .modal { background: var(--panel); border: 1px solid var(--border); border-radius: 10px; padding: 24px; min-width: 320px; }
  .modal h3 { margin-bottom: 14px; color: var(--accent); }
  .modal input { width: 100%; padding: 8px 12px; background: var(--bg); border: 1px solid var(--border); border-radius: 6px;
    color: var(--text); font-size: 14px; margin-bottom: 12px; }
  .modal-buttons { display: flex; gap: 8px; justify-content: flex-end; }
</style>
</head>
<body>
<header>
  <h1>📁 {{ project_name }}</h1>
  <span>God Madara File Manager &nbsp;|&nbsp; User: {{ user_id }}</span>
</header>
<div class="layout">
  <div class="sidebar">
    <div class="sidebar-header">
      📂 Files
      <button class="btn btn-primary" style="padding:4px 10px;font-size:11px" id="btn-new-file">+ New</button>
    </div>
    <div class="sidebar-actions">
      <button class="btn btn-secondary" id="btn-new-folder">📁 Folder</button>
      <button class="btn btn-secondary" id="btn-refresh">🔄</button>
    </div>
    <div class="upload-area">
      <label class="upload-label" for="upload-input">⬆️ Upload File</label>
      <input type="file" id="upload-input" multiple>
    </div>
    <ul class="file-list" id="file-list">
      <li style="padding:12px;color:var(--text-dim);font-size:13px">Loading…</li>
    </ul>
  </div>
  <div class="editor-panel" id="editor-panel">
    <div class="welcome" id="welcome-msg">
      <span>📂</span>
      <div>Select a file to edit</div>
      <div style="font-size:13px">Click any file in the sidebar</div>
    </div>
    <div class="editor-toolbar" id="editor-toolbar" style="display:none">
      <span class="current-file" id="current-file-label">—</span>
      <button class="btn btn-success" id="btn-save">💾 Save</button>
      <button class="btn btn-warning" id="btn-download-cur">⬇️ Download</button>
      <button class="btn btn-secondary" id="btn-close-editor">✖ Close</button>
    </div>
    <div class="editor-wrap" id="editor-wrap" style="display:none">
      <textarea id="code-editor"></textarea>
    </div>
  </div>
</div>

<!-- Modals -->
<div class="modal-overlay" id="new-file-modal">
  <div class="modal">
    <h3>📄 New File</h3>
    <input type="text" id="new-file-name" placeholder="filename.py">
    <div class="modal-buttons">
      <button class="btn btn-secondary" data-close="new-file-modal">Cancel</button>
      <button class="btn btn-primary" id="btn-create-file">Create</button>
    </div>
  </div>
</div>
<div class="modal-overlay" id="new-folder-modal">
  <div class="modal">
    <h3>📁 New Folder</h3>
    <input type="text" id="new-folder-name" placeholder="folder_name">
    <div class="modal-buttons">
      <button class="btn btn-secondary" data-close="new-folder-modal">Cancel</button>
      <button class="btn btn-primary" id="btn-create-folder">Create</button>
    </div>
  </div>
</div>
<div class="modal-overlay" id="rename-modal">
  <div class="modal">
    <h3>✏️ Rename</h3>
    <input type="text" id="rename-new-name" placeholder="new_name.py">
    <input type="hidden" id="rename-old-path">
    <div class="modal-buttons">
      <button class="btn btn-secondary" data-close="rename-modal">Cancel</button>
      <button class="btn btn-warning" id="btn-do-rename">Rename</button>
    </div>
  </div>
</div>

<div class="toast-container" id="toasts"></div>

<script>
// ── Config (injected by server, NO inline onclick with string concatenation) ──
const BASE_API = "/files/{{ user_id }}/{{ project_name }}/api";
const TOKEN    = "{{ token }}";

// ── Helpers ──
function apiUrl(endpoint, extraParams) {
  let url = BASE_API + "/" + endpoint + "?token=" + encodeURIComponent(TOKEN);
  if (extraParams) {
    for (const [k, v] of Object.entries(extraParams)) {
      url += "&" + k + "=" + encodeURIComponent(v);
    }
  }
  return url;
}

let editor = null;
let currentFile = null;
let currentPath = "";

function toast(msg, type) {
  type = type || "info";
  const c = document.getElementById("toasts");
  const el = document.createElement("div");
  el.className = "toast toast-" + type;
  el.textContent = msg;
  c.appendChild(el);
  setTimeout(function() { el.remove(); }, 3500);
}

function getIcon(name, isDir) {
  if (isDir) return "📁";
  const ext = (name.split(".").pop() || "").toLowerCase();
  const map = {py:"🐍",js:"📜",json:"📋",txt:"📄",md:"📝",html:"🌐",
    css:"🎨",sh:"⚙️",zip:"📦",log:"📋",env:"🔑",ini:"⚙️",cfg:"⚙️",
    yaml:"📋",yml:"📋"};
  return map[ext] || "📄";
}

function getMode(name) {
  const ext = (name.split(".").pop() || "").toLowerCase();
  const map = {py:"python",js:"javascript",json:"javascript",
    html:"xml",css:"css",sh:"shell",bash:"shell"};
  return map[ext] || "python";
}

// ── File listing — uses event delegation (NO inline onclick) ──
async function loadFiles(path) {
  if (path === undefined) path = "";
  currentPath = path;
  const res = await fetch(apiUrl("list", { path: path }));
  const data = await res.json();
  const ul = document.getElementById("file-list");
  ul.innerHTML = "";

  if (path) {
    const li = document.createElement("li");
    li.className = "file-item";
    li.dataset.type = "up";
    li.dataset.path = path.split("/").slice(0, -1).join("/");
    li.innerHTML = '<span class="file-icon">⬆️</span><span class="file-name">..</span>';
    ul.appendChild(li);
  }

  for (const item of (data.items || [])) {
    const li = document.createElement("li");
    li.className = "file-item";
    li.dataset.type = item.is_dir ? "dir" : "file";
    li.dataset.path = item.path;
    li.dataset.name = item.name;

    const icon = getIcon(item.name, item.is_dir);
    const actionsHtml = [
      !item.is_dir ? '<button class="action-btn" data-action="edit" title="Edit">✏️</button>' : "",
      '<button class="action-btn" data-action="rename" title="Rename">🏷️</button>',
      !item.is_dir ? '<button class="action-btn" data-action="download" title="Download">⬇️</button>' : "",
      '<button class="action-btn" data-action="delete" title="Delete">🗑️</button>',
    ].join("");

    li.innerHTML =
      '<span class="file-icon">' + icon + '</span>' +
      '<span class="file-name" title="' + escHtml(item.path) + '">' + escHtml(item.name) + '</span>' +
      '<div class="file-actions">' + actionsHtml + '</div>';

    ul.appendChild(li);
  }

  if (!data.items || data.items.length === 0) {
    ul.innerHTML += '<li style="padding:12px;color:var(--text-dim);font-size:12px">Empty folder</li>';
  }
}

function escHtml(s) {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;")
           .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

// ── Event delegation on file list (fixes special-char filename bugs) ──
document.getElementById("file-list").addEventListener("click", function(e) {
  const li = e.target.closest(".file-item");
  if (!li) return;

  const actionBtn = e.target.closest(".action-btn");
  const itemType  = li.dataset.type;
  const itemPath  = li.dataset.path;
  const itemName  = li.dataset.name || "";

  if (actionBtn) {
    e.stopPropagation();
    const action = actionBtn.dataset.action;
    if (action === "edit")     { openFile(itemPath); }
    if (action === "rename")   { showRenameModal(itemPath, itemName); }
    if (action === "download") { downloadFile(itemPath); }
    if (action === "delete")   { deleteItem(itemPath); }
    return;
  }

  // Click on row itself
  if (itemType === "up" || itemType === "dir") {
    loadFiles(itemPath);
  } else if (itemType === "file") {
    openFile(itemPath);
  }
});

function refreshFiles() { loadFiles(currentPath); }

async function openFile(path) {
  const res = await fetch(apiUrl("read", { path: path }));
  const data = await res.json();
  if (data.error) { toast("❌ " + data.error, "error"); return; }
  currentFile = path;
  document.getElementById("welcome-msg").style.display    = "none";
  document.getElementById("editor-toolbar").style.display = "flex";
  document.getElementById("editor-wrap").style.display    = "flex";
  document.getElementById("current-file-label").textContent = "📄 " + path;

  // Highlight active item — use data attribute selector instead of CSS.escape for compatibility
  document.querySelectorAll(".file-item").forEach(function(el) { el.classList.remove("active"); });
  document.querySelectorAll(".file-item").forEach(function(el) {
    if (el.dataset.path === path) { el.classList.add("active"); }
  });

  if (!editor) {
    editor = CodeMirror.fromTextArea(document.getElementById("code-editor"), {
      theme: "dracula", lineNumbers: true, matchBrackets: true,
      autoCloseBrackets: true, indentUnit: 4, tabSize: 4,
      indentWithTabs: false, lineWrapping: false, scrollbarStyle: "native",
    });
    editor.setSize("100%", "100%");
  }
  editor.setOption("mode", getMode(path));
  editor.setValue(data.content || "");
  editor.clearHistory();
  editor.focus();
}

async function saveFile() {
  if (!currentFile || !editor) return;
  const content = editor.getValue();
  const res = await fetch(apiUrl("write"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ path: currentFile, content: content })
  });
  const data = await res.json();
  if (data.ok) toast("✅ Saved: " + currentFile, "success");
  else         toast("❌ Save failed: " + data.error, "error");
}

function closeEditor() {
  currentFile = null;
  document.getElementById("welcome-msg").style.display    = "flex";
  document.getElementById("editor-toolbar").style.display = "none";
  document.getElementById("editor-wrap").style.display    = "none";
  document.querySelectorAll(".file-item").forEach(function(el) { el.classList.remove("active"); });
}

function downloadFile(path) {
  window.open(apiUrl("download", { path: path }));
}

async function deleteItem(path) {
  if (!confirm("Delete " + path + "?\nThis cannot be undone.")) return;
  const res = await fetch(apiUrl("delete"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ path: path })
  });
  const data = await res.json();
  if (data.ok) {
    toast("🗑️ Deleted: " + path, "success");
    if (currentFile === path) closeEditor();
    refreshFiles();
  } else {
    toast("❌ " + data.error, "error");
  }
}

function showModal(id) {
  document.getElementById(id).classList.add("open");
}
function closeModal(id) {
  document.getElementById(id).classList.remove("open");
}

function showRenameModal(path, name) {
  document.getElementById("rename-old-path").value  = path;
  document.getElementById("rename-new-name").value  = name;
  showModal("rename-modal");
  document.getElementById("rename-new-name").focus();
}

async function createNewFile() {
  const name = document.getElementById("new-file-name").value.trim();
  if (!name) return;
  const path = currentPath ? currentPath + "/" + name : name;
  const res = await fetch(apiUrl("write"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ path: path, content: "" })
  });
  const data = await res.json();
  closeModal("new-file-modal");
  if (data.ok) { toast("✅ Created: " + name, "success"); refreshFiles(); openFile(path); }
  else         { toast("❌ " + data.error, "error"); }
}

async function createNewFolder() {
  const name = document.getElementById("new-folder-name").value.trim();
  if (!name) return;
  const path = currentPath ? currentPath + "/" + name : name;
  const res = await fetch(apiUrl("mkdir"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ path: path })
  });
  const data = await res.json();
  closeModal("new-folder-modal");
  if (data.ok) { toast("✅ Folder created: " + name, "success"); refreshFiles(); }
  else         { toast("❌ " + data.error, "error"); }
}

async function renameItem() {
  const oldPath = document.getElementById("rename-old-path").value;
  const newName = document.getElementById("rename-new-name").value.trim();
  if (!newName) return;
  const dir     = oldPath.includes("/") ? oldPath.split("/").slice(0, -1).join("/") : "";
  const newPath = dir ? dir + "/" + newName : newName;
  const res = await fetch(apiUrl("rename"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ old_path: oldPath, new_path: newPath })
  });
  const data = await res.json();
  closeModal("rename-modal");
  if (data.ok) {
    toast("✏️ Renamed to " + newName, "success");
    if (currentFile === oldPath) {
      currentFile = newPath;
      document.getElementById("current-file-label").textContent = "📄 " + newPath;
    }
    refreshFiles();
  } else {
    toast("❌ " + data.error, "error");
  }
}

async function uploadFiles(input) {
  const files = input.files;
  if (!files.length) return;
  for (const file of files) {
    const formData = new FormData();
    formData.append("file", file);
    formData.append("path", currentPath);
    const res  = await fetch(apiUrl("upload"), { method: "POST", body: formData });
    const data = await res.json();
    if (data.ok) toast("⬆️ Uploaded: " + file.name, "success");
    else         toast("❌ Upload failed: " + file.name + " — " + (data.error || ""), "error");
  }
  input.value = "";
  refreshFiles();
}

// ── Button wiring (no inline onclick) ──
document.getElementById("btn-new-file").onclick     = function() { document.getElementById("new-file-name").value=""; showModal("new-file-modal"); document.getElementById("new-file-name").focus(); };
document.getElementById("btn-new-folder").onclick   = function() { document.getElementById("new-folder-name").value=""; showModal("new-folder-modal"); document.getElementById("new-folder-name").focus(); };
document.getElementById("btn-refresh").onclick      = refreshFiles;
document.getElementById("btn-save").onclick         = saveFile;
document.getElementById("btn-download-cur").onclick = function() { if (currentFile) downloadFile(currentFile); };
document.getElementById("btn-close-editor").onclick = closeEditor;
document.getElementById("btn-create-file").onclick  = createNewFile;
document.getElementById("btn-create-folder").onclick= createNewFolder;
document.getElementById("btn-do-rename").onclick    = renameItem;
document.getElementById("upload-input").onchange    = function() { uploadFiles(this); };

// ── Close modal buttons ──
document.querySelectorAll("[data-close]").forEach(function(btn) {
  btn.onclick = function() { closeModal(btn.dataset.close); };
});

// ── Keyboard shortcuts ──
document.addEventListener("keydown", function(e) {
  if ((e.ctrlKey || e.metaKey) && e.key === "s") { e.preventDefault(); saveFile(); }
  if (e.key === "Escape") {
    document.querySelectorAll(".modal-overlay.open").forEach(function(m) { m.classList.remove("open"); });
  }
});

// ── Enter key in modal inputs ──
document.getElementById("new-file-name").addEventListener("keydown",  function(e) { if (e.key === "Enter") createNewFile(); });
document.getElementById("new-folder-name").addEventListener("keydown",function(e) { if (e.key === "Enter") createNewFolder(); });
document.getElementById("rename-new-name").addEventListener("keydown",function(e) { if (e.key === "Enter") renameItem(); });

// Initial load
loadFiles("");
</script>
</body>
</html>
"""

# ──────────────────────────────────────────
#  Single Flask App (keep-alive + file manager)
# ──────────────────────────────────────────
app = Flask("god_madara_bot")


@app.route("/")
def home():
    return "Bot is alive", 200


def _resolve_token(token: str):
    """Validate token and return (user_id, project_name) or abort 401."""
    info = fm_tokens.get(token)
    if not info:
        abort(401, "Invalid or expired token.")
    if time.time() > info["expiry"]:
        fm_tokens.pop(token, None)
        abort(401, "Token expired. Please generate a new link from the bot.")
    return info["user_id"], info["project_name"]


# FIX 3: _safe_path now explicitly handles empty rel and always uses resolved base
def _safe_path(base: Path, rel: str) -> Path:
    """Resolve a relative path within base, preventing directory traversal."""
    rel = rel.strip().lstrip("/").lstrip("\\")
    if not rel:
        return base.resolve()
    target = (base / rel).resolve()
    if not str(target).startswith(str(base.resolve())):
        abort(403, "Path traversal detected.")
    return target


@app.route("/files/<int:user_id>/<project_name>")
def file_manager_ui(user_id: int, project_name: str):
    token = request.args.get("token", "")
    uid, pname = _resolve_token(token)
    if uid != user_id or pname != project_name:
        abort(403, "Token does not match this project.")
    return render_template_string(
        FILE_MANAGER_HTML,
        user_id=user_id,
        project_name=project_name,
        token=token,
    )


# FIX 4: All Flask routes now use resolved base path consistently
@app.route("/files/<int:user_id>/<project_name>/api/list")
def api_list(user_id: int, project_name: str):
    token = request.args.get("token", "")
    uid, pname = _resolve_token(token)
    if uid != user_id or pname != project_name:
        abort(403)
    rel_path = request.args.get("path", "")
    base     = (PROJECTS_DIR / str(user_id) / project_name).resolve()  # FIX: resolved base
    target   = _safe_path(base, rel_path)
    if not target.exists() or not target.is_dir():
        return jsonify({"error": "Directory not found", "items": []})
    items = []
    for entry in sorted(target.iterdir(), key=lambda e: (not e.is_dir(), e.name.lower())):
        rel  = str(entry.relative_to(base)).replace("\\", "/")  # relative to RESOLVED base
        size = entry.stat().st_size if entry.is_file() else None
        items.append({"name": entry.name, "path": rel, "is_dir": entry.is_dir(), "size": size})
    return jsonify({"items": items, "path": rel_path})


@app.route("/files/<int:user_id>/<project_name>/api/read")
def api_read(user_id: int, project_name: str):
    token = request.args.get("token", "")
    uid, pname = _resolve_token(token)
    if uid != user_id or pname != project_name:
        abort(403)
    rel_path = request.args.get("path", "")
    base     = (PROJECTS_DIR / str(user_id) / project_name).resolve()  # FIX: resolved base
    target   = _safe_path(base, rel_path)
    if not target.exists() or not target.is_file():
        return jsonify({"error": "File not found"})
    try:
        content = target.read_text(errors="replace")
    except Exception as exc:
        return jsonify({"error": str(exc)})
    return jsonify({"content": content, "path": rel_path})


@app.route("/files/<int:user_id>/<project_name>/api/write", methods=["POST"])
def api_write(user_id: int, project_name: str):
    token = request.args.get("token", "")
    uid, pname = _resolve_token(token)
    if uid != user_id or pname != project_name:
        abort(403)
    data     = request.get_json(force=True)
    rel_path = data.get("path", "")
    content  = data.get("content", "")
    base     = (PROJECTS_DIR / str(user_id) / project_name).resolve()  # FIX: resolved base
    target   = _safe_path(base, rel_path)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)})
    return jsonify({"ok": True})


@app.route("/files/<int:user_id>/<project_name>/api/delete", methods=["POST"])
def api_delete(user_id: int, project_name: str):
    token = request.args.get("token", "")
    uid, pname = _resolve_token(token)
    if uid != user_id or pname != project_name:
        abort(403)
    data     = request.get_json(force=True)
    rel_path = data.get("path", "")
    base     = (PROJECTS_DIR / str(user_id) / project_name).resolve()  # FIX: resolved base
    target   = _safe_path(base, rel_path)
    if not target.exists():
        return jsonify({"ok": False, "error": "Not found"})
    try:
        if target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink()
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)})
    return jsonify({"ok": True})


@app.route("/files/<int:user_id>/<project_name>/api/rename", methods=["POST"])
def api_rename(user_id: int, project_name: str):
    token = request.args.get("token", "")
    uid, pname = _resolve_token(token)
    if uid != user_id or pname != project_name:
        abort(403)
    data    = request.get_json(force=True)
    old_rel = data.get("old_path", "")
    new_rel = data.get("new_path", "")
    base    = (PROJECTS_DIR / str(user_id) / project_name).resolve()  # FIX: resolved base
    old_t   = _safe_path(base, old_rel)
    new_t   = _safe_path(base, new_rel)
    if not old_t.exists():
        return jsonify({"ok": False, "error": "Source not found"})
    if new_t.exists():
        return jsonify({"ok": False, "error": "Destination already exists"})
    try:
        old_t.rename(new_t)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)})
    return jsonify({"ok": True})


@app.route("/files/<int:user_id>/<project_name>/api/mkdir", methods=["POST"])
def api_mkdir(user_id: int, project_name: str):
    token = request.args.get("token", "")
    uid, pname = _resolve_token(token)
    if uid != user_id or pname != project_name:
        abort(403)
    data     = request.get_json(force=True)
    rel_path = data.get("path", "")
    base     = (PROJECTS_DIR / str(user_id) / project_name).resolve()  # FIX: resolved base
    target   = _safe_path(base, rel_path)
    try:
        target.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)})
    return jsonify({"ok": True})


@app.route("/files/<int:user_id>/<project_name>/api/upload", methods=["POST"])
def api_upload(user_id: int, project_name: str):
    token = request.args.get("token", "")
    uid, pname = _resolve_token(token)
    if uid != user_id or pname != project_name:
        abort(403)
    rel_dir    = request.form.get("path", "")
    base       = (PROJECTS_DIR / str(user_id) / project_name).resolve()  # FIX: resolved base
    upload_dir = _safe_path(base, rel_dir)
    upload_dir.mkdir(parents=True, exist_ok=True)
    file = request.files.get("file")
    if not file or not file.filename:
        return jsonify({"ok": False, "error": "No file provided"})
    # FIX 5: Use safe_filename instead of secure_filename to preserve dots and underscores
    filename = safe_filename(file.filename)
    dest = upload_dir / filename
    try:
        file.save(str(dest))
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)})
    return jsonify({"ok": True, "name": filename})


@app.route("/files/<int:user_id>/<project_name>/api/download")
def api_download(user_id: int, project_name: str):
    token = request.args.get("token", "")
    uid, pname = _resolve_token(token)
    if uid != user_id or pname != project_name:
        abort(403)
    rel_path = request.args.get("path", "")
    base     = (PROJECTS_DIR / str(user_id) / project_name).resolve()  # FIX: resolved base
    target   = _safe_path(base, rel_path)
    if not target.exists() or not target.is_file():
        abort(404)
    return send_file(str(target), as_attachment=True, download_name=target.name)


# ──────────────────────────────────────────
#  Utility helpers
# ──────────────────────────────────────────

def fmt_uptime(start_ts: float) -> str:
    delta = timedelta(seconds=int(time.time() - start_ts))
    h, rem = divmod(int(delta.total_seconds()), 3600)
    m, s   = divmod(rem, 60)
    return f"{h}h {m}m {s}s"


def get_project_dir(user_id: int, project_name: str) -> Path:
    p = PROJECTS_DIR / str(user_id) / project_name
    p.mkdir(parents=True, exist_ok=True)
    return p


async def get_or_create_user(user) -> dict:
    doc = await users_col.find_one({"user_id": user.id})
    if not doc:
        doc = {
            "user_id":        user.id,
            "username":       user.username or user.first_name,
            "plan":           "free",
            "banned":         False,
            "premium_expiry": None,
            "joined_date":    datetime.utcnow(),
        }
        await users_col.insert_one(doc)
    else:
        await users_col.update_one(
            {"user_id": user.id},
            {"$set": {"username": user.username or user.first_name}},
        )
    return doc


async def check_premium_expiry(user_id: int) -> dict:
    doc = await users_col.find_one({"user_id": user_id})
    if not doc:
        return {}
    if doc.get("premium_expiry") and datetime.utcnow() > doc["premium_expiry"]:
        await users_col.update_one(
            {"user_id": user_id},
            {"$set": {"plan": "free", "premium_expiry": None}},
        )
        doc["plan"]           = "free"
        doc["premium_expiry"] = None
    return doc


async def is_banned(user_id: int) -> bool:
    doc = await users_col.find_one({"user_id": user_id})
    return bool(doc and doc.get("banned"))


async def user_project_count(user_id: int) -> int:
    return await projects_col.count_documents({"user_id": user_id})


async def user_project_limit(user_id: int) -> int:
    doc = await check_premium_expiry(user_id)
    return 10 if doc.get("plan") == "premium" else 1


def process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError, TypeError, OSError):
        return False


async def sync_project_status(project: dict) -> dict:
    if project.get("status") == "running":
        pid = project.get("pid")
        key = f"{project['user_id']}:{project['project_name']}"
        proc = running_processes.get(key)
        if proc:
            ret = proc.poll()
            if ret is not None:
                running_processes.pop(key, None)
                await projects_col.update_one(
                    {"_id": project["_id"]},
                    {"$set": {"status": "stopped" if ret == 0 else "error",
                              "exit_code": ret, "uptime_start": None}},
                )
                project["status"] = "stopped" if ret == 0 else "error"
                return project
        if not pid or not process_alive(pid):
            await projects_col.update_one(
                {"_id": project["_id"]},
                {"$set": {"status": "stopped", "uptime_start": None}},
            )
            project["status"] = "stopped"
    return project


def get_status_emoji(status: str) -> str:
    return {"running": "🟢", "stopped": "🔴", "error": "🟡"}.get(status, "⚫")


async def kill_project_process(user_id: int, project_name: str, pid=None):
    key  = f"{user_id}:{project_name}"
    proc = running_processes.pop(key, None)
    if proc:
        try:
            proc.kill()
        except Exception:
            pass
    if pid:
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            pass


def generate_fm_token(user_id: int, project_name: str) -> str:
    token = secrets.token_urlsafe(32)
    fm_tokens[token] = {
        "user_id":      user_id,
        "project_name": project_name,
        "expiry":       time.time() + 3600,
    }
    return token


def _start_process(project_dir: Path, run_cmd: str, log_file: Path) -> subprocess.Popen:
    """
    BUG 2 FIX: Use shell=True so commands with spaces/args work correctly.
    Also ensure proper env PATH is passed so python3 and installed packages are found.
    Log file is opened with append mode and passed to the process.
    """
    env = os.environ.copy()
    # Ensure PATH includes common python binary locations
    env.setdefault("PATH", "/usr/local/bin:/usr/bin:/bin")

    log_handle = open(str(log_file), "a")
    log_handle.write(f"\n{'='*40}\nStarted at {datetime.utcnow()}\nCommand: {run_cmd}\n{'='*40}\n")
    log_handle.flush()

    proc = subprocess.Popen(
        run_cmd,
        shell=True,               # FIX: shell=True handles complex commands with spaces/args
        cwd=str(project_dir),
        stdout=log_handle,
        stderr=log_handle,
        env=env,
        close_fds=True,
        preexec_fn=os.setsid if hasattr(os, "setsid") else None,
    )
    # We deliberately keep log_handle open — it will be closed when the process ends
    # (the OS will close it when parent forgets about it, but process inherited the fd)
    return proc


# ──────────────────────────────────────────
#  BUG 5 FIX: Backup / Restore system
# ──────────────────────────────────────────

async def backup_state():
    """Save all currently running project states to the backups collection."""
    try:
        running = await projects_col.find({"status": "running"}).to_list(length=1000)
        if not running:
            return
        backup_doc = {
            "timestamp": datetime.utcnow(),
            "projects": [
                {
                    "user_id":      p["user_id"],
                    "project_name": p["project_name"],
                    "run_command":  p.get("run_command", "python3 main.py"),
                    "status":       "running",
                    "uptime_start": p.get("uptime_start"),
                }
                for p in running
            ],
        }
        # Delete all previous backups, keep only the latest
        await backups_col.delete_many({})
        await backups_col.insert_one(backup_doc)
        logger.info(f"[Backup] Saved {len(running)} running project(s).")
    except Exception as exc:
        logger.error(f"[Backup] Error: {exc}")


async def restore_state():
    """On startup, restore previously running projects from the latest backup."""
    try:
        backup = await backups_col.find_one(sort=[("timestamp", -1)])
        if not backup:
            logger.info("[Restore] No backup found. Starting fresh.")
            return
        restored = 0
        for entry in backup.get("projects", []):
            uid          = entry["user_id"]
            pname        = entry["project_name"]
            run_cmd      = entry.get("run_command", "python3 main.py")
            project_dir  = PROJECTS_DIR / str(uid) / pname

            if not project_dir.exists():
                logger.info(f"[Restore] Skipping {uid}:{pname} — directory missing.")
                await projects_col.update_one(
                    {"user_id": uid, "project_name": pname},
                    {"$set": {"status": "stopped", "pid": None, "uptime_start": None}},
                )
                continue

            project = await projects_col.find_one({"user_id": uid, "project_name": pname})
            if not project:
                logger.info(f"[Restore] Skipping {uid}:{pname} — not in DB.")
                continue

            try:
                log_file = project_dir / "output.log"
                proc     = _start_process(project_dir, run_cmd, log_file)
                key      = f"{uid}:{pname}"
                running_processes[key] = proc
                await projects_col.update_one(
                    {"user_id": uid, "project_name": pname},
                    {"$set": {
                        "status":       "running",
                        "pid":          proc.pid,
                        "last_run":     datetime.utcnow(),
                        "uptime_start": time.time(),
                        "exit_code":    None,
                    }},
                )
                restored += 1
                logger.info(f"[Restore] Restarted {uid}:{pname} (PID {proc.pid})")
            except Exception as exc:
                logger.error(f"[Restore] Failed to restart {uid}:{pname}: {exc}")
                await projects_col.update_one(
                    {"user_id": uid, "project_name": pname},
                    {"$set": {"status": "stopped", "pid": None, "uptime_start": None}},
                )

        logger.info(f"[Restore] Restored {restored} project(s).")
    except Exception as exc:
        logger.error(f"[Restore] Error: {exc}")


async def backup_loop():
    """Run backup_state() every 5 minutes indefinitely."""
    while True:
        await asyncio.sleep(300)  # 5 minutes
        await backup_state()


# ──────────────────────────────────────────
#  Keyboards
# ──────────────────────────────────────────

def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🆕 New Project",  callback_data="new_project"),
            InlineKeyboardButton("📂 My Projects",  callback_data="my_projects"),
        ],
        [
            InlineKeyboardButton("💎 Premium",      callback_data="premium"),
            InlineKeyboardButton("📊 Bot Status",   callback_data="bot_status"),
        ],
    ])


def project_dashboard_keyboard(project_name: str, user_id: int) -> InlineKeyboardMarkup:
    pdata = f"{user_id}:{project_name}"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("▶️ Run",          callback_data=f"run:{pdata}"),
            InlineKeyboardButton("🔄 Restart",      callback_data=f"restart:{pdata}"),
            InlineKeyboardButton("📋 Logs",         callback_data=f"logs:{pdata}"),
            InlineKeyboardButton("🔃 Refresh",      callback_data=f"refresh:{pdata}"),
        ],
        [
            InlineKeyboardButton("✏️ Edit Run CMD", callback_data=f"edit_cmd:{pdata}"),
            InlineKeyboardButton("📁 File Manager", callback_data=f"filemanager:{pdata}"),
            InlineKeyboardButton("🗑️ Delete",       callback_data=f"delete:{pdata}"),
        ],
        [
            InlineKeyboardButton("🔙 Back",         callback_data="my_projects"),
        ],
    ])


def back_to_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back", callback_data="main_menu")]
    ])


def admin_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👥 All Users",       callback_data="admin:all_users:0"),
            InlineKeyboardButton("🟢 Running Scripts", callback_data="admin:running"),
        ],
        [
            InlineKeyboardButton("💎 Give Premium",    callback_data="admin:give_premium"),
            InlineKeyboardButton("❌ Remove Premium",  callback_data="admin:remove_premium"),
        ],
        [
            InlineKeyboardButton("⏰ Temp Premium",    callback_data="admin:temp_premium"),
            InlineKeyboardButton("🚫 Ban User",        callback_data="admin:ban"),
        ],
        [
            InlineKeyboardButton("✅ Unban User",      callback_data="admin:unban"),
            InlineKeyboardButton("📢 Broadcast",       callback_data="admin:broadcast_menu"),
        ],
        [
            InlineKeyboardButton("📨 Message User",    callback_data="admin:msg_user"),
        ],
    ])


# ──────────────────────────────────────────
#  Common guard
# ──────────────────────────────────────────

async def banned_guard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id
    if await is_banned(user_id):
        msg = "🚫 You are banned from using this bot."
        if update.message:
            await update.message.reply_text(msg)
        elif update.callback_query:
            await update.callback_query.answer(msg, show_alert=True)
        return True
    return False


# ──────────────────────────────────────────
#  /start
# ──────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await banned_guard(update, context):
        return
    user  = update.effective_user
    await get_or_create_user(user)
    doc   = await check_premium_expiry(user.id)
    plan  = doc.get("plan", "free")
    limit = 10 if plan == "premium" else 1
    count = await user_project_count(user.id)
    plan_label = "Premium ✨" if plan == "premium" else "Free"

    text = (
        f"🌟 *Welcome to God Madara Hosting Bot!*\n\n"
        f"👋 Hello {user.first_name}!\n\n"
        f"🚀 *What I can do:*\n"
        f"• Host Python projects 24/7\n"
        f"• Web File Manager — Edit files in browser\n"
        f"• Auto-install requirements.txt\n"
        f"• Real-time logs & monitoring\n"
        f"• Free: 1 project | Premium: 10 projects\n\n"
        f"📊 *Your Status:*\n"
        f"👤 ID: `{user.id}`\n"
        f"💎 Plan: {plan_label}\n"
        f"📁 Projects: {count}/{limit}\n\n"
        f"Choose an option below:"
    )
    if update.message:
        await update.message.reply_text(
            text, parse_mode=ParseMode.MARKDOWN, reply_markup=main_menu_keyboard()
        )
    elif update.callback_query:
        await update.callback_query.edit_message_text(
            text, parse_mode=ParseMode.MARKDOWN, reply_markup=main_menu_keyboard()
        )


async def main_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if await banned_guard(update, context):
        return
    await start(update, context)


# ──────────────────────────────────────────
#  NEW PROJECT ConversationHandler
# ──────────────────────────────────────────

# FIX 6 & 7: new_project_start now clears stale user_data and wraps edit_message_text in try-except
async def new_project_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    if await banned_guard(update, context):
        return ConversationHandler.END

    # FIX 8: Clear any stale conversation state from previous conversations
    context.user_data.clear()

    user_id = update.effective_user.id
    count   = await user_project_count(user_id)
    limit   = await user_project_limit(user_id)
    if count >= limit:
        plan_label = "Premium" if limit == 10 else "Free"
        limit_text = (
            f"❌ You have reached your project limit ({limit} for {plan_label} plan).\n"
            "Upgrade to Premium for 10 projects."
        )
        limit_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💎 Upgrade Premium", callback_data="premium")],
            [InlineKeyboardButton("🔙 Back",             callback_data="main_menu")],
        ])
        try:
            await query.edit_message_text(limit_text, reply_markup=limit_kb)
        except Exception:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=limit_text,
                reply_markup=limit_kb,
            )
        return ConversationHandler.END

    prompt_text = (
        "📝 *Enter a name for your new project:*\n\n"
        "_(Use letters, numbers, underscores only)_"
    )
    try:
        await query.edit_message_text(prompt_text, parse_mode=ParseMode.MARKDOWN)
    except Exception:
        # Fallback: send a fresh message if edit fails (e.g. message unchanged or API error)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=prompt_text,
            parse_mode=ParseMode.MARKDOWN,
        )
    return NP_NAME


async def new_project_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if await banned_guard(update, context):
        return ConversationHandler.END
    name      = update.message.text.strip().replace(" ", "_")
    safe_name = "".join(c for c in name if c.isalnum() or c in "_-")
    if not safe_name:
        await update.message.reply_text("❌ Invalid name. Use letters, numbers, underscores only.")
        return NP_NAME
    user_id  = update.effective_user.id
    existing = await projects_col.find_one({"user_id": user_id, "project_name": safe_name})
    if existing:
        await update.message.reply_text(
            f"❌ You already have a project named *{safe_name}*. Choose another name.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return NP_NAME
    context.user_data["new_project_name"]  = safe_name
    context.user_data["new_project_files"] = []
    get_project_dir(user_id, safe_name)
    await update.message.reply_text(
        f"📦 Project *{safe_name}* created!\n\n"
        "📎 *Upload your project files* one by one (`.py`, `.txt`, `.zip`, etc.)\n"
        "When done, press the button below.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Done Uploading", callback_data="np_done_uploading")]
        ]),
    )
    return NP_FILES


async def new_project_receive_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if await banned_guard(update, context):
        return ConversationHandler.END
    user_id      = update.effective_user.id
    project_name = context.user_data.get("new_project_name")
    if not project_name:
        await update.message.reply_text("❌ Session expired. Start over with /start.")
        return ConversationHandler.END
    project_dir = get_project_dir(user_id, project_name)
    doc         = update.message.document
    if not doc:
        await update.message.reply_text("⚠️ Please send a file, or press ✅ Done when finished.")
        return NP_FILES
    file_name = doc.file_name
    file_path = project_dir / file_name
    tg_file   = await doc.get_file()
    await tg_file.download_to_drive(str(file_path))
    files_list: list = context.user_data.setdefault("new_project_files", [])
    done_btn = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Done Uploading", callback_data="np_done_uploading")]
    ])
    if file_name.lower().endswith(".zip"):
        try:
            with zipfile.ZipFile(file_path, "r") as zf:
                zf.extractall(project_dir)
                extracted = zf.namelist()
            os.remove(file_path)
            files_list.extend(extracted)
            await update.message.reply_text(
                f"📦 ZIP extracted: {len(extracted)} file(s)\n"
                + "\n".join(f"  • `{f}`" for f in extracted[:20])
                + ("\n  …and more" if len(extracted) > 20 else ""),
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=done_btn,
            )
        except zipfile.BadZipFile:
            await update.message.reply_text("❌ Bad ZIP file. Please re-upload.")
    else:
        files_list.append(file_name)
        await update.message.reply_text(
            f"✅ Saved: `{file_name}`\nTotal files: {len(files_list)}\n\nUpload more or press ✅ Done.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=done_btn,
        )
    return NP_FILES


async def new_project_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    BUG 1 FIX: pip install runs ASYNCHRONOUSLY via asyncio.create_subprocess_exec.
    Bot stays responsive during installation.
    """
    query = update.callback_query
    await query.answer()
    if await banned_guard(update, context):
        return ConversationHandler.END
    user_id      = update.effective_user.id
    project_name = context.user_data.get("new_project_name")
    if not project_name:
        await query.edit_message_text("❌ Session expired. Use /start.")
        return ConversationHandler.END
    project_dir = get_project_dir(user_id, project_name)
    disk_files  = [f.name for f in project_dir.iterdir() if f.is_file()]
    await query.edit_message_text(
        f"⏳ Finalizing project *{project_name}*…\n📂 Files found: {len(disk_files)}",
        parse_mode=ParseMode.MARKDOWN,
    )
    req_file      = project_dir / "requirements.txt"
    install_result = ""
    if req_file.exists():
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⏳ *Found `requirements.txt` — installing packages asynchronously…*\nBot remains responsive.",
            parse_mode=ParseMode.MARKDOWN,
        )
        try:
            # BUG 1 FIX: asyncio.create_subprocess_exec — non-blocking, event loop stays free
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "pip", "install", "-r", str(req_file),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=180)
            output = (stdout + stderr).decode(errors="replace")
            lines  = [l for l in output.splitlines() if l.strip()]
            success_lines = [l for l in lines if "successfully installed" in l.lower() or "already satisfied" in l.lower()]
            fail_lines    = [l for l in lines if "error" in l.lower() and "error:" in l.lower()]
            install_result = "✅ *Package Installation:*\n"
            if success_lines:
                install_result += "\n".join(f"  ✅ {l}" for l in success_lines[:8]) + "\n"
            if fail_lines:
                install_result += "\n".join(f"  ❌ {l}" for l in fail_lines[:5]) + "\n"
            if not success_lines and not fail_lines and lines:
                install_result += "  ℹ️ " + "\n  ℹ️ ".join(lines[:5]) + "\n"
            install_result += f"  Exit code: `{proc.returncode}`"
        except asyncio.TimeoutError:
            install_result = "⚠️ pip install timed out after 180s"
        except Exception as exc:
            install_result = f"❌ pip error: {exc}"
    main_py = project_dir / "main.py"
    if main_py.exists():
        run_cmd = "python3 main.py"
    elif disk_files:
        py_files = [f for f in disk_files if f.endswith(".py")]
        run_cmd  = f"python3 {py_files[0]}" if py_files else f"python3 {disk_files[0]}"
    else:
        run_cmd = "python3 main.py"
    await projects_col.insert_one({
        "user_id":       user_id,
        "project_name":  project_name,
        "files":         disk_files,
        "status":        "stopped",
        "pid":           None,
        "created_date":  datetime.utcnow(),
        "run_command":   run_cmd,
        "last_run":      None,
        "exit_code":     None,
        "uptime_start":  None,
        "owner_stopped": False,
    })
    context.user_data.clear()
    msg = (
        f"🎉 *Project '{project_name}' is ready!*\n\n"
        f"📂 Files: {', '.join(f'`{f}`' for f in disk_files[:10])}"
        + (" …and more" if len(disk_files) > 10 else "") + "\n"
        + (f"\n{install_result}\n" if install_result else "")
        + f"\n⚙️ Default run command: `{run_cmd}`\n\n"
        "Use the dashboard below to run your project."
    )
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=msg,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=project_dashboard_keyboard(project_name, user_id),
    )
    return ConversationHandler.END


async def np_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    if update.message:
        await update.message.reply_text("❌ New project cancelled.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END


# ──────────────────────────────────────────
#  MY PROJECTS
# ──────────────────────────────────────────

async def my_projects_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if await banned_guard(update, context):
        return
    user_id  = update.effective_user.id
    projects = await projects_col.find({"user_id": user_id}).to_list(length=50)
    if not projects:
        await query.edit_message_text(
            "📂 *My Projects*\n\nYou have no projects yet.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🆕 New Project", callback_data="new_project")],
                [InlineKeyboardButton("🔙 Back",         callback_data="main_menu")],
            ]),
        )
        return
    buttons = []
    for p in projects:
        # Sync status without DB write for display only
        status_emoji = get_status_emoji(p.get("status", "stopped"))
        buttons.append([InlineKeyboardButton(
            f"{status_emoji} {p['project_name']}",
            callback_data=f"project_dash:{user_id}:{p['project_name']}",
        )])
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
    await query.edit_message_text(
        "📂 *My Projects*\n\nSelect a project to manage:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def project_dashboard_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if await banned_guard(update, context):
        return
    # callback_data = "project_dash:{user_id}:{project_name}"
    parts        = query.data.split(":", 2)
    user_id      = int(parts[1])
    project_name = parts[2]
    if query.from_user.id != user_id and query.from_user.id != OWNER_ID:
        await query.answer("❌ Access denied.", show_alert=True)
        return
    project = await projects_col.find_one({"user_id": user_id, "project_name": project_name})
    if not project:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main_keyboard())
        return
    project = await sync_project_status(project)
    await _send_project_dashboard(query, project)


async def _send_project_dashboard(query_or_msg, project: dict):
    status      = project.get("status", "stopped")
    pid         = project.get("pid") or "N/A"
    uptime      = fmt_uptime(project["uptime_start"]) if project.get("uptime_start") and status == "running" else "N/A"
    last_run    = project["last_run"].strftime("%Y-%m-%d %H:%M UTC") if project.get("last_run") else "Never"
    exit_code   = project.get("exit_code")
    run_cmd     = project.get("run_command", "python3 main.py")
    created     = project["created_date"].strftime("%Y-%m-%d") if project.get("created_date") else "N/A"
    user_id     = project["user_id"]
    project_name = project["project_name"]
    text = (
        f"📊 *Project: {project_name}*\n\n"
        f"🔹 Status: {get_status_emoji(status)} {status.capitalize()}\n"
        f"🔹 PID: `{pid}`\n"
        f"🔹 Uptime: {uptime}\n"
        f"🔹 Last Run: {last_run}\n"
        f"🔹 Exit Code: `{exit_code if exit_code is not None else 'None'}`\n"
        f"🔹 Run Command: `{run_cmd}`\n"
        f"📅 Created: {created}"
    )
    kb = project_dashboard_keyboard(project_name, user_id)
    if hasattr(query_or_msg, "edit_message_text"):
        await query_or_msg.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    else:
        await query_or_msg.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)


# ──────────────────────────────────────────
#  Project Actions
# ──────────────────────────────────────────

async def run_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if await banned_guard(update, context):
        return
    _, user_id_str, project_name = query.data.split(":", 2)
    user_id = int(user_id_str)
    if query.from_user.id != user_id and query.from_user.id != OWNER_ID:
        await query.answer("❌ Access denied.", show_alert=True)
        return
    project = await projects_col.find_one({"user_id": user_id, "project_name": project_name})
    if not project:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main_keyboard())
        return
    if project.get("owner_stopped") and query.from_user.id != OWNER_ID:
        await query.edit_message_text(
            "⚠️ *Your project was stopped by admin.*\nContact the owner for support.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📩 Contact Owner", url=f"tg://user?id={OWNER_ID}")],
                [InlineKeyboardButton("🔙 Back", callback_data=f"project_dash:{user_id}:{project_name}")],
            ]),
        )
        return
    if project.get("status") == "running":
        await query.answer("⚠️ Project is already running!", show_alert=True)
        return
    project_dir = get_project_dir(user_id, project_name)
    run_cmd     = project.get("run_command", "python3 main.py")
    log_file    = project_dir / "output.log"
    try:
        # BUG 2 FIX: uses _start_process with shell=True
        proc = _start_process(project_dir, run_cmd, log_file)
        key  = f"{user_id}:{project_name}"
        running_processes[key] = proc
        await projects_col.update_one(
            {"user_id": user_id, "project_name": project_name},
            {"$set": {
                "status":        "running",
                "pid":           proc.pid,
                "last_run":      datetime.utcnow(),
                "uptime_start":  time.time(),
                "exit_code":     None,
                "owner_stopped": False,
            }},
        )
        await query.edit_message_text(
            f"▶️ *Project '{project_name}' started!*\n🆔 PID: `{proc.pid}`\n⚙️ Command: `{run_cmd}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=project_dashboard_keyboard(project_name, user_id),
        )
    except Exception as exc:
        await projects_col.update_one(
            {"user_id": user_id, "project_name": project_name},
            {"$set": {"status": "error", "exit_code": -1}},
        )
        await query.edit_message_text(
            f"❌ Failed to start project:\n`{exc}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=project_dashboard_keyboard(project_name, user_id),
        )


async def restart_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if await banned_guard(update, context):
        return
    _, user_id_str, project_name = query.data.split(":", 2)
    user_id = int(user_id_str)
    if query.from_user.id != user_id and query.from_user.id != OWNER_ID:
        await query.answer("❌ Access denied.", show_alert=True)
        return
    project = await projects_col.find_one({"user_id": user_id, "project_name": project_name})
    if not project:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main_keyboard())
        return
    await kill_project_process(user_id, project_name, project.get("pid"))
    await projects_col.update_one(
        {"user_id": user_id, "project_name": project_name},
        {"$set": {"status": "stopped", "pid": None}},
    )
    project_dir = get_project_dir(user_id, project_name)
    run_cmd     = project.get("run_command", "python3 main.py")
    log_file    = project_dir / "output.log"
    try:
        with open(str(log_file), "a") as lf:
            lf.write(f"\n{'='*40}\nRestarted at {datetime.utcnow()}\n{'='*40}\n")
        proc = _start_process(project_dir, run_cmd, log_file)
        key  = f"{user_id}:{project_name}"
        running_processes[key] = proc
        await projects_col.update_one(
            {"user_id": user_id, "project_name": project_name},
            {"$set": {
                "status":        "running",
                "pid":           proc.pid,
                "last_run":      datetime.utcnow(),
                "uptime_start":  time.time(),
                "exit_code":     None,
                "owner_stopped": False,
            }},
        )
        await query.edit_message_text(
            f"🔄 *Project '{project_name}' restarted!*\n🆔 PID: `{proc.pid}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=project_dashboard_keyboard(project_name, user_id),
        )
    except Exception as exc:
        await query.edit_message_text(
            f"❌ Restart failed:\n`{exc}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=project_dashboard_keyboard(project_name, user_id),
        )


async def logs_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if await banned_guard(update, context):
        return
    _, user_id_str, project_name = query.data.split(":", 2)
    user_id     = int(user_id_str)
    if query.from_user.id != user_id and query.from_user.id != OWNER_ID:
        await query.answer("❌ Access denied.", show_alert=True)
        return
    project_dir = get_project_dir(user_id, project_name)
    log_file    = project_dir / "output.log"
    if not log_file.exists():
        await query.edit_message_text(
            "📋 No logs yet. Run the project first.",
            reply_markup=project_dashboard_keyboard(project_name, user_id),
        )
        return
    with open(str(log_file), "r", errors="replace") as f:
        lines = f.readlines()
    last_50 = "".join(lines[-50:]).strip() or "(empty log)"
    if len(last_50) > 3800:
        last_50 = "…(truncated)\n" + last_50[-3800:]
    await query.edit_message_text(
        f"📋 *Logs for '{project_name}'* (last 50 lines):\n\n```\n{last_50}\n```",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=project_dashboard_keyboard(project_name, user_id),
    )


async def refresh_project(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("🔃 Refreshing…")
    if await banned_guard(update, context):
        return
    _, user_id_str, project_name = query.data.split(":", 2)
    user_id  = int(user_id_str)
    project  = await projects_col.find_one({"user_id": user_id, "project_name": project_name})
    if not project:
        await query.edit_message_text("❌ Project not found.", reply_markup=back_to_main_keyboard())
        return
    project = await sync_project_status(project)
    await _send_project_dashboard(query, project)


async def delete_project_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if await banned_guard(update, context):
        return
    _, user_id_str, project_name = query.data.split(":", 2)
    user_id = int(user_id_str)
    if query.from_user.id != user_id and query.from_user.id != OWNER_ID:
        await query.answer("❌ Access denied.", show_alert=True)
        return
    await query.edit_message_text(
        f"🗑️ *Delete Project '{project_name}'?*\n\n⚠️ This will permanently delete all files and logs.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yes, Delete", callback_data=f"delete_yes:{user_id}:{project_name}"),
                InlineKeyboardButton("❌ No",          callback_data=f"project_dash:{user_id}:{project_name}"),
            ]
        ]),
    )


async def delete_project_yes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if await banned_guard(update, context):
        return
    _, user_id_str, project_name = query.data.split(":", 2)
    user_id = int(user_id_str)
    if query.from_user.id != user_id and query.from_user.id != OWNER_ID:
        await query.answer("❌ Access denied.", show_alert=True)
        return
    project = await projects_col.find_one({"user_id": user_id, "project_name": project_name})
    if project:
        await kill_project_process(user_id, project_name, project.get("pid"))
    await projects_col.delete_one({"user_id": user_id, "project_name": project_name})
    project_dir = PROJECTS_DIR / str(user_id) / project_name
    if project_dir.exists():
        shutil.rmtree(project_dir, ignore_errors=True)
    await query.edit_message_text(
        f"✅ Project *{project_name}* deleted successfully.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📂 My Projects", callback_data="my_projects")],
            [InlineKeyboardButton("🔙 Main Menu",   callback_data="main_menu")],
        ]),
    )


# ──────────────────────────────────────────
#  Edit Run Command ConversationHandler
# ──────────────────────────────────────────

async def edit_cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    if await banned_guard(update, context):
        return ConversationHandler.END
    _, user_id_str, project_name = query.data.split(":", 2)
    user_id = int(user_id_str)
    context.user_data["edit_cmd_project"] = project_name
    context.user_data["edit_cmd_user_id"] = user_id
    project = await projects_col.find_one({"user_id": user_id, "project_name": project_name})
    current = project.get("run_command", "python3 main.py") if project else "python3 main.py"
    await query.edit_message_text(
        f"✏️ *Edit Run Command for '{project_name}'*\n\n"
        f"Current: `{current}`\n\n"
        "Send the new run command (e.g. `python3 bot.py --flag value`):",
        parse_mode=ParseMode.MARKDOWN,
    )
    return EDIT_CMD


async def edit_cmd_receive(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if await banned_guard(update, context):
        return ConversationHandler.END
    new_cmd      = update.message.text.strip()
    project_name = context.user_data.get("edit_cmd_project")
    user_id      = context.user_data.get("edit_cmd_user_id")
    if not project_name or not user_id:
        await update.message.reply_text("❌ Session expired.")
        return ConversationHandler.END
    await projects_col.update_one(
        {"user_id": user_id, "project_name": project_name},
        {"$set": {"run_command": new_cmd}},
    )
    context.user_data.pop("edit_cmd_project", None)
    context.user_data.pop("edit_cmd_user_id", None)
    await update.message.reply_text(
        f"✅ Run command updated to:\n`{new_cmd}`",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=project_dashboard_keyboard(project_name, user_id),
    )
    return ConversationHandler.END


async def edit_cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    await update.message.reply_text("❌ Cancelled.", reply_markup=main_menu_keyboard())
    return ConversationHandler.END


# ──────────────────────────────────────────
#  File Manager Telegram Callback
# ──────────────────────────────────────────

async def file_manager_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if await banned_guard(update, context):
        return
    _, user_id_str, project_name = query.data.split(":", 2)
    user_id = int(user_id_str)
    if query.from_user.id != user_id and query.from_user.id != OWNER_ID:
        await query.answer("❌ Access denied.", show_alert=True)
        return
    token    = generate_fm_token(user_id, project_name)
    base_url = RENDER_URL or f"http://localhost:{PORT}"
    # BUG 3 FIX: token is a query param, URL is clean — no double "?"
    fm_url   = f"{base_url}/files/{user_id}/{project_name}?token={token}"
    await query.edit_message_text(
        f"📁 *File Manager — {project_name}*\n\n"
        f"🔗 Your secure link (expires in 1 hour):\n`{fm_url}`\n\n"
        "⚠️ Do not share this link — it grants full file access.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🌐 Open File Manager", url=fm_url)],
            [InlineKeyboardButton("🔙 Back", callback_data=f"project_dash:{user_id}:{project_name}")],
        ]),
    )


# ──────────────────────────────────────────
#  Premium Screen
# ──────────────────────────────────────────

async def premium_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if await banned_guard(update, context):
        return
    user_id    = update.effective_user.id
    doc        = await check_premium_expiry(user_id)
    plan       = doc.get("plan", "free")
    is_premium = plan == "premium"
    expiry     = doc.get("premium_expiry")
    status_line  = "✨ *You are Premium!*" if is_premium else "🔒 You are on *Free Plan*"
    expiry_line  = f"\n⏳ Expires: {expiry.strftime('%Y-%m-%d %H:%M UTC')}" if expiry else ""
    upgrade_section = "🌟 *Premium is active!*" if is_premium else "💰 *Upgrade to Premium:*\nContact the owner below."
    text = (
        f"💎 *Premium Membership*\n\n"
        f"{status_line}{expiry_line}\n\n"
        f"*Free Plan:*\n"
        f"• 1 Project only\n"
        f"• Basic file manager\n\n"
        f"*Premium Plan:*\n"
        f"• ✅ 10 projects\n"
        f"• ✅ Priority support\n"
        f"• ✅ Extended file manager\n"
        f"• ✅ Advanced monitoring\n\n"
        f"{upgrade_section}"
    )
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📩 Contact Owner", url=f"tg://user?id={OWNER_ID}")],
            [InlineKeyboardButton("🔙 Back",           callback_data="main_menu")],
        ]),
    )


# ──────────────────────────────────────────
#  Bot Status Screen
# ──────────────────────────────────────────

async def bot_status_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if await banned_guard(update, context):
        return
    t0              = time.time()
    total_users     = await users_col.count_documents({})
    premium_users   = await users_col.count_documents({"plan": "premium"})
    total_projects  = await projects_col.count_documents({})
    running_cnt     = await projects_col.count_documents({"status": "running"})
    ping_ms         = int((time.time() - t0) * 1000)
    cpu             = psutil.cpu_percent(interval=0.5)
    ram             = psutil.virtual_memory()
    disk            = psutil.disk_usage("/")
    py_version      = platform.python_version()

    def hr(b):
        for unit in ("B", "KB", "MB", "GB"):
            if b < 1024:
                return f"{b:.1f}{unit}"
            b /= 1024
        return f"{b:.1f}TB"

    text = (
        f"📊 *Bot Dashboard*\n\n"
        f"👥 Total Users: `{total_users}`\n"
        f"💎 Premium Users: `{premium_users}`\n"
        f"📁 Total Projects: `{total_projects}`\n"
        f"🟢 Running Projects: `{running_cnt}`\n"
        f"🗄️ Database: Connected ✅\n"
        f"🐍 Python: `{py_version}`\n\n"
        f"💻 *System:*\n"
        f"├ CPU: `{cpu}%`\n"
        f"├ RAM: `{hr(ram.used)}/{hr(ram.total)}` (`{ram.percent}%`)\n"
        f"└ Disk: `{hr(disk.used)}/{hr(disk.total)}` (`{disk.percent}%`)\n\n"
        f"🏓 Response Ping: `{ping_ms}ms`"
    )
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=back_to_main_keyboard(),
    )


# ──────────────────────────────────────────
#  ADMIN — /admin command (OWNER ONLY)
# ──────────────────────────────────────────

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # BUG 6 FIX: strictly OWNER_ID only
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("❌ You are not the owner.")
        return
    await update.message.reply_text(
        "🛡️ *Admin Panel*\n\nSelect an action:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=admin_panel_keyboard(),
    )


# ──────────────────────────────────────────
#  ADMIN ConversationHandler (BUG 4 FIX)
# ──────────────────────────────────────────
# Entry: admin:give_premium / remove_premium / temp_premium / ban / unban /
#         broadcast_all / broadcast_specific / msg_user
# ADMIN_WAIT_INPUT  → receives first text (user_id or message)
# ADMIN_WAIT_INPUT_2 → receives second text (duration, message) for multi-step flows

async def admin_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """CallbackQueryHandler entry for admin conversation flows."""
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        await query.answer("❌ Admin only.", show_alert=True)
        return ConversationHandler.END

    data   = query.data  # "admin:give_premium", etc.
    action = data.split(":")[1]
    context.user_data["admin_action"] = action

    prompts = {
        "give_premium":       "💎 *Give Premium*\n\nSend the *user_id* to give premium:",
        "remove_premium":     "❌ *Remove Premium*\n\nSend the *user_id* to remove premium:",
        "temp_premium":       "⏰ *Temp Premium*\n\nSend the *user_id*:",
        "ban":                "🚫 *Ban User*\n\nSend the *user_id* to ban:",
        "unban":              "✅ *Unban User*\n\nSend the *user_id* to unban:",
        "broadcast_all":      "📢 *Broadcast to ALL users*\n\nSend the message to broadcast:",
        "broadcast_specific": "📨 *Broadcast to Specific User*\n\nSend the *user_id*:",
        "msg_user":           "📨 *Message User*\n\nSend the *user_id*:",
    }
    await query.edit_message_text(
        prompts.get(action, "Send input:"),
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Cancel", callback_data="admin:back")]
        ]),
    )
    return ADMIN_WAIT_INPUT


async def admin_process_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """First text input for admin conversations."""
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    action  = context.user_data.get("admin_action", "")
    text_in = update.message.text.strip()

    # ── Actions that need only one input (user_id) ──
    if action == "give_premium":
        try:
            uid = int(text_in)
        except ValueError:
            await update.message.reply_text("❌ Invalid user_id. Send a number.")
            return ADMIN_WAIT_INPUT
        await users_col.update_one(
            {"user_id": uid},
            {"$set": {"plan": "premium", "premium_expiry": None}},
            upsert=True,
        )
        await update.message.reply_text(
            f"✅ Premium given to user `{uid}`.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=admin_panel_keyboard(),
        )
        context.user_data.clear()
        return ConversationHandler.END

    if action == "remove_premium":
        try:
            uid = int(text_in)
        except ValueError:
            await update.message.reply_text("❌ Invalid user_id. Send a number.")
            return ADMIN_WAIT_INPUT
        await users_col.update_one({"user_id": uid}, {"$set": {"plan": "free", "premium_expiry": None}})
        # Delete extra projects beyond limit=1
        extra = await projects_col.find({"user_id": uid}).skip(1).to_list(length=100)
        for p in extra:
            await kill_project_process(uid, p["project_name"], p.get("pid"))
            proj_dir = PROJECTS_DIR / str(uid) / p["project_name"]
            if proj_dir.exists():
                shutil.rmtree(proj_dir, ignore_errors=True)
            await projects_col.delete_one({"_id": p["_id"]})
        await update.message.reply_text(
            f"✅ Premium removed from user `{uid}`. Extra projects deleted.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=admin_panel_keyboard(),
        )
        context.user_data.clear()
        return ConversationHandler.END

    if action == "ban":
        try:
            uid = int(text_in)
        except ValueError:
            await update.message.reply_text("❌ Invalid user_id. Send a number.")
            return ADMIN_WAIT_INPUT
        await users_col.update_one({"user_id": uid}, {"$set": {"banned": True}}, upsert=True)
        # Stop all their projects
        running = await projects_col.find({"user_id": uid, "status": "running"}).to_list(length=100)
        for p in running:
            await kill_project_process(uid, p["project_name"], p.get("pid"))
            await projects_col.update_one({"_id": p["_id"]}, {"$set": {"status": "stopped"}})
        await update.message.reply_text(
            f"🚫 User `{uid}` banned and all running projects stopped.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=admin_panel_keyboard(),
        )
        context.user_data.clear()
        return ConversationHandler.END

    if action == "unban":
        try:
            uid = int(text_in)
        except ValueError:
            await update.message.reply_text("❌ Invalid user_id. Send a number.")
            return ADMIN_WAIT_INPUT
        await users_col.update_one({"user_id": uid}, {"$set": {"banned": False}})
        await update.message.reply_text(
            f"✅ User `{uid}` unbanned.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=admin_panel_keyboard(),
        )
        context.user_data.clear()
        return ConversationHandler.END

    if action == "broadcast_all":
        # text_in IS the message
        all_users = await users_col.find({}).to_list(length=10000)
        success = fail = 0
        for u in all_users:
            try:
                await context.bot.send_message(
                    chat_id=u["user_id"],
                    text=f"📢 *Broadcast:*\n\n{text_in}",
                    parse_mode=ParseMode.MARKDOWN,
                )
                success += 1
            except Exception:
                fail += 1
        await update.message.reply_text(
            f"📢 Broadcast done.\n✅ Sent: {success}\n❌ Failed: {fail}",
            reply_markup=admin_panel_keyboard(),
        )
        context.user_data.clear()
        return ConversationHandler.END

    # ── Actions needing a second input ──
    if action == "temp_premium":
        try:
            uid = int(text_in)
        except ValueError:
            await update.message.reply_text("❌ Invalid user_id. Send a number.")
            return ADMIN_WAIT_INPUT
        context.user_data["admin_uid_2"] = uid
        await update.message.reply_text(
            "⏰ Send duration: e.g. `24h` or `7d` (hours or days):",
            parse_mode=ParseMode.MARKDOWN,
        )
        return ADMIN_WAIT_INPUT_2

    if action in ("broadcast_specific", "msg_user"):
        try:
            uid = int(text_in)
        except ValueError:
            await update.message.reply_text("❌ Invalid user_id. Send a number.")
            return ADMIN_WAIT_INPUT
        context.user_data["admin_uid_2"] = uid
        await update.message.reply_text("📨 Now send the message:")
        return ADMIN_WAIT_INPUT_2

    return ConversationHandler.END


async def admin_process_input_2(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Second text input for multi-step admin flows."""
    if update.effective_user.id != OWNER_ID:
        return ConversationHandler.END
    action  = context.user_data.get("admin_action", "")
    uid     = context.user_data.get("admin_uid_2")
    text_in = update.message.text.strip()

    if action == "temp_premium":
        try:
            if text_in.endswith("h"):
                delta = timedelta(hours=int(text_in[:-1]))
            elif text_in.endswith("d"):
                delta = timedelta(days=int(text_in[:-1]))
            else:
                delta = timedelta(hours=int(text_in))
        except ValueError:
            await update.message.reply_text("❌ Invalid format. Use `24h` or `7d`.", parse_mode=ParseMode.MARKDOWN)
            return ADMIN_WAIT_INPUT_2
        expiry = datetime.utcnow() + delta
        await users_col.update_one(
            {"user_id": uid},
            {"$set": {"plan": "premium", "premium_expiry": expiry}},
            upsert=True,
        )
        await update.message.reply_text(
            f"✅ Temp premium given to `{uid}` until `{expiry.strftime('%Y-%m-%d %H:%M UTC')}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=admin_panel_keyboard(),
        )

    elif action in ("broadcast_specific", "msg_user"):
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=f"📨 *Message from admin:*\n\n{text_in}",
                parse_mode=ParseMode.MARKDOWN,
            )
            await update.message.reply_text(
                f"✅ Message sent to `{uid}`.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=admin_panel_keyboard(),
            )
        except Exception as exc:
            await update.message.reply_text(
                f"❌ Failed to send: {exc}",
                reply_markup=admin_panel_keyboard(),
            )

    context.user_data.clear()
    return ConversationHandler.END


async def admin_conv_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(
            "🛡️ *Admin Panel*\n\nSelect an action:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=admin_panel_keyboard(),
        )
    elif update.message:
        await update.message.reply_text("❌ Admin action cancelled.", reply_markup=admin_panel_keyboard())
    return ConversationHandler.END


# FIX 9: new_project fallback handler — exits any active conversation and starts new project flow
async def new_project_fallback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Fallback handler: user clicks New Project while inside edit_cmd or admin conversation."""
    context.user_data.clear()
    # Delegate to new_project_start which handles try-except on edit_message_text
    return await new_project_start(update, context)


# ──────────────────────────────────────────
#  Admin non-conversation callbacks (all_users, running, etc.)
# ──────────────────────────────────────────

async def admin_generic_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles admin callbacks that do NOT enter a conversation."""
    query = update.callback_query
    await query.answer()
    if update.effective_user.id != OWNER_ID:
        await query.answer("❌ Admin only.", show_alert=True)
        return

    data   = query.data
    parts  = data.split(":", 3)
    action = parts[1] if len(parts) > 1 else ""

    if action == "all_users":
        page     = int(parts[2]) if len(parts) > 2 else 0
        per_page = 10
        total    = await users_col.count_documents({})
        users    = await users_col.find({}).skip(page * per_page).limit(per_page).to_list(length=per_page)
        lines    = []
        for u in users:
            plan_icon = "💎" if u.get("plan") == "premium" else "🆓"
            ban_icon  = " 🚫" if u.get("banned") else ""
            lines.append(f"{plan_icon}{ban_icon} @{u.get('username', 'N/A')} | `{u['user_id']}`")
        total_pages = max(1, (total + per_page - 1) // per_page)
        text = f"👥 *All Users* (Page {page+1}/{total_pages})\n\n" + "\n".join(lines)
        nav  = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"admin:all_users:{page-1}"))
        if (page + 1) * per_page < total:
            nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin:all_users:{page+1}"))
        kb = [nav] if nav else []
        kb.append([InlineKeyboardButton("🔙 Back", callback_data="admin:back")])
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(kb))

    elif action == "running":
        projects = await projects_col.find({"status": "running"}).to_list(length=100)
        if not projects:
            await query.edit_message_text(
                "🟢 *Running Scripts*\n\nNo projects currently running.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:back")]]),
            )
            return
        buttons = []
        for p in projects:
            u     = await users_col.find_one({"user_id": p["user_id"]})
            uname = u.get("username", "N/A") if u else "N/A"
            buttons.append([InlineKeyboardButton(
                f"@{uname} — {p['project_name']}",
                callback_data=f"admin:view_running:{p['user_id']}:{p['project_name']}",
            )])
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="admin:back")])
        await query.edit_message_text(
            "🟢 *Running Scripts:*",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    elif action == "view_running":
        # parts[2] = "user_id", parts[3] = "project_name" (split was limited to 3)
        uid   = int(parts[2])
        pname = parts[3] if len(parts) > 3 else ""
        project = await projects_col.find_one({"user_id": uid, "project_name": pname})
        u       = await users_col.find_one({"user_id": uid})
        uname   = u.get("username", "N/A") if u else "N/A"
        pid     = project.get("pid") if project else "N/A"
        uptime  = fmt_uptime(project["uptime_start"]) if project and project.get("uptime_start") else "N/A"
        files   = project.get("files", []) if project else []
        text = (
            f"👤 Username: @{uname}\n"
            f"🆔 User ID: `{uid}`\n"
            f"📊 PID: `{pid}`\n"
            f"⏱️ Running Time: {uptime}\n"
            f"📜 Files: {', '.join(f'`{f}`' for f in files[:10])}"
        )
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("⏹️ Stop",         callback_data=f"admin:stop_proj:{uid}:{pname}"),
                    InlineKeyboardButton("📥 Download ZIP", callback_data=f"admin:dl_proj:{uid}:{pname}"),
                ],
                [InlineKeyboardButton("🔙 Back", callback_data="admin:running")],
            ]),
        )

    elif action == "stop_proj":
        uid   = int(parts[2])
        pname = parts[3] if len(parts) > 3 else ""
        project = await projects_col.find_one({"user_id": uid, "project_name": pname})
        if project:
            await kill_project_process(uid, pname, project.get("pid"))
        await projects_col.update_one(
            {"user_id": uid, "project_name": pname},
            {"$set": {"status": "stopped", "pid": None, "owner_stopped": True, "uptime_start": None}},
        )
        await query.edit_message_text(
            f"⏹️ Project *{pname}* stopped by admin.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin:running")]]),
        )

    elif action == "dl_proj":
        uid   = int(parts[2])
        pname = parts[3] if len(parts) > 3 else ""
        project_dir = PROJECTS_DIR / str(uid) / pname
        if not project_dir.exists():
            await query.answer("❌ Project directory not found.", show_alert=True)
            return
        zip_buf = BytesIO()
        with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in project_dir.rglob("*"):
                if f.is_file():
                    zf.write(f, f.relative_to(project_dir))
        zip_buf.seek(0)
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=zip_buf,
            filename=f"{pname}.zip",
            caption=f"📥 Project files for *{pname}* (user `{uid}`)",
            parse_mode=ParseMode.MARKDOWN,
        )
        await query.answer("📥 Sending ZIP…")

    elif action == "back":
        await query.edit_message_text(
            "🛡️ *Admin Panel*\n\nSelect an action:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=admin_panel_keyboard(),
        )

    elif action == "broadcast_menu":
        await query.edit_message_text(
            "📢 *Broadcast*\n\nChoose broadcast target:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("📢 All Users",       callback_data="admin:broadcast_all"),
                    InlineKeyboardButton("📨 Specific User",   callback_data="admin:broadcast_specific"),
                ],
                [InlineKeyboardButton("🔙 Back", callback_data="admin:back")],
            ]),
        )


# ──────────────────────────────────────────
#  Generic callback dispatcher
# ──────────────────────────────────────────

async def generic_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data  = query.data
    if await banned_guard(update, context):
        return
    if data.startswith("project_dash:"):
        await project_dashboard_callback(update, context)
    elif data.startswith("run:"):
        await run_project(update, context)
    elif data.startswith("restart:"):
        await restart_project(update, context)
    elif data.startswith("logs:"):
        await logs_project(update, context)
    elif data.startswith("refresh:"):
        await refresh_project(update, context)
    elif data.startswith("delete:") and not data.startswith("delete_yes:"):
        await delete_project_confirm(update, context)
    elif data.startswith("delete_yes:"):
        await delete_project_yes(update, context)
    elif data.startswith("filemanager:"):
        await file_manager_callback(update, context)
    elif data == "my_projects":
        await my_projects_callback(update, context)
    elif data == "premium":
        await premium_callback(update, context)
    elif data == "bot_status":
        await bot_status_callback(update, context)
    elif data == "main_menu":
        await main_menu_callback(update, context)
    elif data.startswith("admin:"):
        # Route to non-conversation admin callbacks
        action = data.split(":")[1] if ":" in data else ""
        if action in ("all_users", "running", "view_running", "stop_proj", "dl_proj", "back", "broadcast_menu"):
            await admin_generic_callback(update, context)
        # Conversation-entry actions are handled by admin_conv ConversationHandler
        # If they fall through here (e.g. after conversation ends), show admin panel
        else:
            await query.answer()
    else:
        await query.answer()


# ──────────────────────────────────────────
#  Startup / shutdown hooks
# ──────────────────────────────────────────

async def on_startup(application: Application):
    """Restore previously running projects, then start backup loop."""
    logger.info("Running startup restore…")
    await restore_state()
    # Schedule backup loop as a background task
    asyncio.create_task(backup_loop())
    logger.info("God Madara Bot started. Backup loop scheduled.")


# ──────────────────────────────────────────
#  Flask runner (daemon thread)
# ──────────────────────────────────────────

def run_flask():
    logger.info(f"Starting Flask on port {PORT} (keep-alive + file manager)…")
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)


# ──────────────────────────────────────────
#  Main
# ──────────────────────────────────────────

def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN not set in environment!")

    # Start Flask (keep-alive + file manager) in a daemon thread
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()

    # Build Telegram Application
    tg_app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .build()
    )

    # ── New Project ConversationHandler ──
    # FIX 6: allow_reentry=True so user can re-enter even if already in this conversation
    new_project_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(new_project_start, pattern="^new_project$")],
        states={
            NP_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, new_project_name)],
            NP_FILES: [
                MessageHandler(filters.Document.ALL, new_project_receive_file),
                CallbackQueryHandler(new_project_done, pattern="^np_done_uploading$"),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", np_cancel),
            CommandHandler("start",  np_cancel),
        ],
        per_message=False,
        allow_reentry=True,  # FIX 6: allow re-entry into this conversation
    )

    # ── Edit Command ConversationHandler ──
    # FIX 6: allow_reentry=True + FIX 9: new_project fallback so clicking New Project exits cleanly
    edit_cmd_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(edit_cmd_start, pattern=r"^edit_cmd:")],
        states={
            EDIT_CMD: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_cmd_receive)],
        },
        fallbacks=[
            CommandHandler("cancel", edit_cmd_cancel),
            # FIX 9: if user clicks "New Project" while in edit_cmd conversation, exit and start new project
            CallbackQueryHandler(new_project_fallback, pattern="^new_project$"),
        ],
        per_message=False,
        allow_reentry=True,  # FIX 6: allow re-entry
    )

    # ── Admin ConversationHandler (BUG 4 FIX) ──
    # Only entry points for actions that NEED text input
    # FIX 6: allow_reentry=True + FIX 9: new_project fallback
    admin_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(
                admin_entry,
                pattern=r"^admin:(give_premium|remove_premium|temp_premium|ban|unban|broadcast_all|broadcast_specific|msg_user)$",
            )
        ],
        states={
            ADMIN_WAIT_INPUT:   [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_process_input)],
            ADMIN_WAIT_INPUT_2: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_process_input_2)],
        },
        fallbacks=[
            CommandHandler("cancel", admin_conv_cancel),
            CallbackQueryHandler(admin_conv_cancel, pattern=r"^admin:back$"),
            # FIX 9: if user clicks "New Project" while in admin conversation, exit and start new project
            CallbackQueryHandler(new_project_fallback, pattern="^new_project$"),
        ],
        per_message=False,
        allow_reentry=True,  # FIX 6: allow re-entry
    )

    # Register handlers — ORDER matters (ConversationHandlers first)
    tg_app.add_handler(CommandHandler("start", start))
    tg_app.add_handler(CommandHandler("admin", admin_command))
    tg_app.add_handler(new_project_conv)
    tg_app.add_handler(edit_cmd_conv)
    tg_app.add_handler(admin_conv)
    # Generic callback dispatcher (handles everything else)
    tg_app.add_handler(CallbackQueryHandler(generic_callback))

    logger.info("God Madara Hosting Bot starting…")
    tg_app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
