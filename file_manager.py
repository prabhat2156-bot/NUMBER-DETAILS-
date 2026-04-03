import os
import secrets
import datetime
import mimetypes
import shutil
from pathlib import Path
from flask import (
    Flask, request, render_template_string, redirect,
    url_for, send_file, abort, jsonify, session
)
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)

# In-memory token store (also backed by MongoDB from bot.py)
# Tokens: {token: {"user_id": ..., "project_name": ..., "project_path": ..., "expires_at": ...}}
_active_tokens = {}


def register_token(token: str, user_id: int, project_name: str, project_path: str, expires_at: datetime.datetime):
    """Register a file manager token."""
    _active_tokens[token] = {
        "user_id": user_id,
        "project_name": project_name,
        "project_path": project_path,
        "expires_at": expires_at,
    }


def cleanup_expired_tokens():
    """Remove expired tokens from memory."""
    now = datetime.datetime.utcnow()
    expired = [t for t, v in _active_tokens.items() if v["expires_at"] < now]
    for t in expired:
        del _active_tokens[t]


def validate_token(token: str):
    """Return token data if valid, else None."""
    cleanup_expired_tokens()
    data = _active_tokens.get(token)
    if not data:
        return None
    if datetime.datetime.utcnow() > data["expires_at"]:
        del _active_tokens[token]
        return None
    return data


def safe_path(base: str, rel: str) -> str:
    """Ensure the path is within base directory (prevent directory traversal)."""
    base = os.path.realpath(base)
    target = os.path.realpath(os.path.join(base, rel))
    if not target.startswith(base):
        raise ValueError("Path traversal detected")
    return target


FILE_MANAGER_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>🌟 God Madara File Manager - {{ project_name }}</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/codemirror.min.css">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/theme/dracula.min.css">
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/codemirror.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/python/python.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/javascript/javascript.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/xml/xml.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/css/css.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/shell/shell.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/addon/edit/matchbrackets.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/addon/edit/closebrackets.js"></script>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #0d1117; color: #c9d1d9; min-height: 100vh; }
  .header { background: linear-gradient(135deg, #1a1a2e, #16213e); padding: 15px 20px; border-bottom: 1px solid #30363d; display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 10px; }
  .header h1 { font-size: 1.2em; color: #f0c040; }
  .header .project-badge { background: #21262d; border: 1px solid #30363d; padding: 5px 12px; border-radius: 20px; font-size: 0.85em; color: #58a6ff; }
  .header .expiry { font-size: 0.75em; color: #8b949e; }
  .container { display: flex; height: calc(100vh - 70px); }
  .sidebar { width: 280px; min-width: 200px; background: #161b22; border-right: 1px solid #30363d; overflow-y: auto; flex-shrink: 0; }
  .sidebar-header { padding: 12px 15px; background: #21262d; border-bottom: 1px solid #30363d; font-size: 0.85em; color: #8b949e; display: flex; align-items: center; justify-content: space-between; }
  .file-list { list-style: none; }
  .file-item { display: flex; align-items: center; padding: 8px 15px; cursor: pointer; border-bottom: 1px solid #21262d; transition: background 0.15s; font-size: 0.9em; gap: 8px; }
  .file-item:hover { background: #21262d; }
  .file-item.active { background: #1f4068; border-left: 3px solid #58a6ff; }
  .file-item .icon { font-size: 1em; flex-shrink: 0; }
  .file-item .name { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .file-item .actions { display: flex; gap: 5px; opacity: 0; transition: opacity 0.15s; }
  .file-item:hover .actions { opacity: 1; }
  .file-item .actions button { background: none; border: 1px solid #30363d; color: #8b949e; cursor: pointer; padding: 2px 5px; border-radius: 4px; font-size: 0.75em; }
  .file-item .actions button:hover { background: #30363d; color: #c9d1d9; }
  .main { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
  .toolbar { padding: 10px 15px; background: #21262d; border-bottom: 1px solid #30363d; display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
  .btn { padding: 6px 14px; border: none; border-radius: 6px; cursor: pointer; font-size: 0.85em; font-weight: 500; transition: all 0.15s; }
  .btn-primary { background: #238636; color: white; }
  .btn-primary:hover { background: #2ea043; }
  .btn-secondary { background: #21262d; color: #c9d1d9; border: 1px solid #30363d; }
  .btn-secondary:hover { background: #30363d; }
  .btn-danger { background: #da3633; color: white; }
  .btn-danger:hover { background: #f85149; }
  .btn-info { background: #1f6feb; color: white; }
  .btn-info:hover { background: #388bfd; }
  .editor-area { flex: 1; overflow: hidden; position: relative; }
  .CodeMirror { height: 100% !important; font-size: 14px; font-family: 'Fira Code', 'Courier New', monospace; }
  .welcome-screen { display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100%; color: #8b949e; gap: 15px; }
  .welcome-screen h2 { color: #f0c040; font-size: 2em; }
  .welcome-screen p { font-size: 1em; }
  .status-bar { padding: 5px 15px; background: #161b22; border-top: 1px solid #30363d; font-size: 0.75em; color: #8b949e; display: flex; gap: 15px; }
  .modal { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.7); z-index: 100; align-items: center; justify-content: center; }
  .modal.show { display: flex; }
  .modal-box { background: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 25px; min-width: 320px; max-width: 90vw; }
  .modal-box h3 { color: #f0c040; margin-bottom: 15px; }
  .modal-box input { width: 100%; background: #0d1117; border: 1px solid #30363d; color: #c9d1d9; padding: 8px 12px; border-radius: 6px; font-size: 0.9em; margin-bottom: 12px; }
  .modal-box input:focus { outline: none; border-color: #58a6ff; }
  .modal-actions { display: flex; gap: 8px; justify-content: flex-end; }
  .path-bar { padding: 6px 15px; background: #0d1117; border-bottom: 1px solid #30363d; font-size: 0.8em; color: #8b949e; font-family: monospace; }
  @media (max-width: 600px) { .sidebar { width: 200px; } .header h1 { font-size: 1em; } }
</style>
</head>
<body>
<div class="header">
  <h1>🌟 God Madara File Manager</h1>
  <span class="project-badge">📁 {{ project_name }}</span>
  <span class="expiry" id="expiry-timer">⏱ Session: Loading...</span>
</div>
<div class="container">
  <div class="sidebar">
    <div class="sidebar-header">
      <span>📂 Files</span>
      <button class="btn btn-secondary" style="padding:2px 8px;font-size:0.75em;" onclick="refreshFiles()">↻</button>
    </div>
    <ul class="file-list" id="file-list">
      <li style="padding:15px;color:#8b949e;font-size:0.85em;">Loading files...</li>
    </ul>
  </div>
  <div class="main">
    <div class="toolbar">
      <button class="btn btn-primary" onclick="saveFile()" id="save-btn" style="display:none">💾 Save</button>
      <button class="btn btn-secondary" onclick="showNewFileModal()">📄 New File</button>
      <button class="btn btn-secondary" onclick="showUploadModal()">📤 Upload</button>
      <button class="btn btn-secondary" onclick="showNewFolderModal()">📁 New Folder</button>
      <span id="current-file-name" style="font-size:0.85em;color:#8b949e;margin-left:8px;"></span>
    </div>
    <div class="path-bar" id="path-bar">/ (root)</div>
    <div class="editor-area" id="editor-area">
      <div class="welcome-screen" id="welcome-screen">
        <h2>🌟</h2>
        <p>Select a file to edit</p>
        <p style="font-size:0.85em;">or create a new one</p>
      </div>
      <textarea id="code-editor" style="display:none"></textarea>
    </div>
    <div class="status-bar">
      <span id="status-msg">Ready</span>
      <span id="cursor-pos"></span>
    </div>
  </div>
</div>

<!-- New File Modal -->
<div class="modal" id="new-file-modal">
  <div class="modal-box">
    <h3>📄 New File</h3>
    <input type="text" id="new-file-name" placeholder="filename.py" />
    <div class="modal-actions">
      <button class="btn btn-secondary" onclick="closeModal('new-file-modal')">Cancel</button>
      <button class="btn btn-primary" onclick="createFile()">Create</button>
    </div>
  </div>
</div>

<!-- New Folder Modal -->
<div class="modal" id="new-folder-modal">
  <div class="modal-box">
    <h3>📁 New Folder</h3>
    <input type="text" id="new-folder-name" placeholder="folder_name" />
    <div class="modal-actions">
      <button class="btn btn-secondary" onclick="closeModal('new-folder-modal')">Cancel</button>
      <button class="btn btn-primary" onclick="createFolder()">Create</button>
    </div>
  </div>
</div>

<!-- Upload Modal -->
<div class="modal" id="upload-modal">
  <div class="modal-box">
    <h3>📤 Upload File</h3>
    <input type="file" id="upload-input" multiple style="background:none;border:none;padding:0;margin-bottom:12px;" />
    <div class="modal-actions">
      <button class="btn btn-secondary" onclick="closeModal('upload-modal')">Cancel</button>
      <button class="btn btn-primary" onclick="uploadFile()">Upload</button>
    </div>
  </div>
</div>

<!-- Rename Modal -->
<div class="modal" id="rename-modal">
  <div class="modal-box">
    <h3>✏️ Rename</h3>
    <input type="hidden" id="rename-old-name" />
    <input type="text" id="rename-new-name" placeholder="new_name.py" />
    <div class="modal-actions">
      <button class="btn btn-secondary" onclick="closeModal('rename-modal')">Cancel</button>
      <button class="btn btn-primary" onclick="doRename()">Rename</button>
    </div>
  </div>
</div>

<script>
const TOKEN = "{{ token }}";
const BASE = "";
let editor = null;
let currentFile = null;
let currentDir = "";
let expiresAt = new Date("{{ expires_at }}");

function updateTimer() {
  const now = new Date();
  const diff = Math.max(0, Math.floor((expiresAt - now) / 1000));
  const m = Math.floor(diff / 60).toString().padStart(2, '0');
  const s = (diff % 60).toString().padStart(2, '0');
  document.getElementById('expiry-timer').textContent = `⏱ Session: ${m}:${s}`;
  if (diff <= 0) {
    document.getElementById('expiry-timer').textContent = '⏱ Session expired!';
    document.getElementById('expiry-timer').style.color = '#f85149';
  }
}
setInterval(updateTimer, 1000);
updateTimer();

function setStatus(msg) {
  document.getElementById('status-msg').textContent = msg;
}

async function apiFetch(url, options = {}) {
  try {
    const res = await fetch(url, options);
    return res;
  } catch (e) {
    setStatus('Network error: ' + e.message);
    return null;
  }
}

async function refreshFiles() {
  const res = await apiFetch(`/fm/${TOKEN}/api/list?dir=${encodeURIComponent(currentDir)}`);
  if (!res) return;
  const data = await res.json();
  if (data.error) { setStatus('Error: ' + data.error); return; }
  renderFileList(data.files);
  document.getElementById('path-bar').textContent = '/' + (currentDir || '');
}

function getIcon(item) {
  if (item.is_dir) return '📁';
  const ext = item.name.split('.').pop().toLowerCase();
  const icons = { py: '🐍', txt: '📝', md: '📖', json: '🔧', env: '⚙️', sh: '🖥️', js: '📜', html: '🌐', css: '🎨', yml: '⚙️', yaml: '⚙️', log: '📋' };
  return icons[ext] || '📄';
}

function renderFileList(files) {
  const ul = document.getElementById('file-list');
  ul.innerHTML = '';
  if (currentDir) {
    const li = document.createElement('li');
    li.className = 'file-item';
    li.innerHTML = `<span class="icon">⬆️</span><span class="name">.. (go up)</span>`;
    li.onclick = () => { currentDir = currentDir.split('/').slice(0, -1).join('/'); refreshFiles(); };
    ul.appendChild(li);
  }
  if (!files || files.length === 0) {
    ul.innerHTML += '<li style="padding:12px 15px;color:#8b949e;font-size:0.85em;">Empty directory</li>';
    return;
  }
  files.forEach(item => {
    const li = document.createElement('li');
    li.className = 'file-item' + (currentFile === (currentDir ? currentDir + '/' + item.name : item.name) ? ' active' : '');
    const relPath = currentDir ? currentDir + '/' + item.name : item.name;
    li.innerHTML = `
      <span class="icon">${getIcon(item)}</span>
      <span class="name" title="${item.name}">${item.name}</span>
      <div class="actions">
        ${!item.is_dir ? `<button onclick="event.stopPropagation();downloadFile('${relPath}')" title="Download">⬇</button>` : ''}
        <button onclick="event.stopPropagation();showRenameModal('${relPath}')" title="Rename">✏</button>
        <button onclick="event.stopPropagation();deleteFile('${relPath}','${item.is_dir}')" title="Delete" style="color:#f85149;">🗑</button>
      </div>
    `;
    if (item.is_dir) {
      li.onclick = () => { currentDir = relPath; refreshFiles(); };
    } else {
      li.onclick = () => openFile(relPath, item.name);
    }
    ul.appendChild(li);
  });
}

function getModeForFile(name) {
  const ext = name.split('.').pop().toLowerCase();
  const modes = { py: 'python', js: 'javascript', html: 'xml', css: 'css', sh: 'shell', bash: 'shell' };
  return modes[ext] || 'python';
}

async function openFile(relPath, name) {
  const res = await apiFetch(`/fm/${TOKEN}/api/read?path=${encodeURIComponent(relPath)}`);
  if (!res) return;
  const data = await res.json();
  if (data.error) { setStatus('Error: ' + data.error); return; }
  
  document.getElementById('welcome-screen').style.display = 'none';
  document.getElementById('save-btn').style.display = '';
  document.getElementById('current-file-name').textContent = relPath;
  
  if (!editor) {
    const ta = document.getElementById('code-editor');
    ta.style.display = 'block';
    editor = CodeMirror.fromTextArea(ta, {
      theme: 'dracula', lineNumbers: true, matchBrackets: true,
      autoCloseBrackets: true, indentUnit: 4, tabSize: 4,
      indentWithTabs: false, lineWrapping: false,
    });
    editor.on('cursorActivity', () => {
      const c = editor.getCursor();
      document.getElementById('cursor-pos').textContent = `Ln ${c.line+1}, Col ${c.ch+1}`;
    });
  }
  
  editor.setValue(data.content);
  editor.setOption('mode', getModeForFile(name));
  currentFile = relPath;
  setStatus(`Opened: ${relPath}`);
  document.querySelectorAll('.file-item').forEach(el => el.classList.remove('active'));
  event.currentTarget && event.currentTarget.classList.add('active');
  refreshFiles();
}

async function saveFile() {
  if (!currentFile || !editor) { setStatus('No file open'); return; }
  const content = editor.getValue();
  const res = await apiFetch(`/fm/${TOKEN}/api/write`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path: currentFile, content })
  });
  if (!res) return;
  const data = await res.json();
  if (data.error) { setStatus('Save failed: ' + data.error); return; }
  setStatus(`Saved: ${currentFile}`);
}

function showNewFileModal() {
  document.getElementById('new-file-name').value = '';
  document.getElementById('new-file-modal').classList.add('show');
  setTimeout(() => document.getElementById('new-file-name').focus(), 100);
}
function showNewFolderModal() {
  document.getElementById('new-folder-name').value = '';
  document.getElementById('new-folder-modal').classList.add('show');
  setTimeout(() => document.getElementById('new-folder-name').focus(), 100);
}
function showUploadModal() {
  document.getElementById('upload-modal').classList.add('show');
}
function showRenameModal(path) {
  document.getElementById('rename-old-name').value = path;
  document.getElementById('rename-new-name').value = path.split('/').pop();
  document.getElementById('rename-modal').classList.add('show');
  setTimeout(() => document.getElementById('rename-new-name').focus(), 100);
}
function closeModal(id) {
  document.getElementById(id).classList.remove('show');
}

async function createFile() {
  let name = document.getElementById('new-file-name').value.trim();
  if (!name) return;
  const path = currentDir ? currentDir + '/' + name : name;
  const res = await apiFetch(`/fm/${TOKEN}/api/write`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path, content: '' })
  });
  if (!res) return;
  const data = await res.json();
  if (data.error) { setStatus('Error: ' + data.error); return; }
  closeModal('new-file-modal');
  await refreshFiles();
  openFile(path, name);
}

async function createFolder() {
  let name = document.getElementById('new-folder-name').value.trim();
  if (!name) return;
  const path = currentDir ? currentDir + '/' + name : name;
  const res = await apiFetch(`/fm/${TOKEN}/api/mkdir`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path })
  });
  if (!res) return;
  const data = await res.json();
  if (data.error) { setStatus('Error: ' + data.error); return; }
  closeModal('new-folder-modal');
  setStatus('Folder created: ' + name);
  refreshFiles();
}

async function uploadFile() {
  const files = document.getElementById('upload-input').files;
  if (!files.length) return;
  for (const file of files) {
    const fd = new FormData();
    fd.append('file', file);
    fd.append('dir', currentDir);
    const res = await apiFetch(`/fm/${TOKEN}/api/upload`, { method: 'POST', body: fd });
    if (!res) continue;
    const data = await res.json();
    if (data.error) { setStatus('Upload error: ' + data.error); continue; }
  }
  closeModal('upload-modal');
  setStatus(`Uploaded ${files.length} file(s)`);
  refreshFiles();
}

function downloadFile(path) {
  window.open(`/fm/${TOKEN}/api/download?path=${encodeURIComponent(path)}`, '_blank');
}

async function deleteFile(path, isDir) {
  if (!confirm(`Delete ${isDir === 'True' || isDir === true ? 'folder' : 'file'}: ${path}?`)) return;
  const res = await apiFetch(`/fm/${TOKEN}/api/delete`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path })
  });
  if (!res) return;
  const data = await res.json();
  if (data.error) { setStatus('Delete error: ' + data.error); return; }
  if (currentFile === path) { currentFile = null; document.getElementById('welcome-screen').style.display = ''; document.getElementById('save-btn').style.display = 'none'; document.getElementById('current-file-name').textContent = ''; }
  setStatus('Deleted: ' + path);
  refreshFiles();
}

async function doRename() {
  const oldPath = document.getElementById('rename-old-name').value;
  const newName = document.getElementById('rename-new-name').value.trim();
  if (!newName) return;
  const dirPart = oldPath.includes('/') ? oldPath.split('/').slice(0, -1).join('/') + '/' : '';
  const newPath = dirPart + newName;
  const res = await apiFetch(`/fm/${TOKEN}/api/rename`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ old_path: oldPath, new_path: newPath })
  });
  if (!res) return;
  
  const data = await res.json();
  if (data.error) { setStatus('Rename error: ' + data.error); return; }
  closeModal('rename-modal');
  if (currentFile === oldPath) { currentFile = newPath; document.getElementById('current-file-name').textContent = newPath; }
  setStatus('Renamed to: ' + newName);
  refreshFiles();
}

document.addEventListener('keydown', e => {
  if ((e.ctrlKey || e.metaKey) && e.key === 's') { e.preventDefault(); saveFile(); }
});

window.onclick = e => {
  document.querySelectorAll('.modal').forEach(m => { if (e.target === m) m.classList.remove('show'); });
};

refreshFiles();
</script>
</body>
</html>
"""

EXPIRED_HTML = """
<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>Session Expired</title>
<style>
body{background:#0d1117;color:#c9d1d9;font-family:'Segoe UI',sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;flex-direction:column;gap:15px;}
h1{color:#f85149;}p{color:#8b949e;}
</style></head>
<body><h1>⏱ Session Expired</h1><p>Your file manager session has expired.</p><p>Generate a new link from the Telegram bot.</p></body></html>
"""


@app.route("/fm/<token>/")
def file_manager_index(token):
    data = validate_token(token)
    if not data:
        return EXPIRED_HTML, 401
    expires_str = data["expires_at"].strftime("%Y-%m-%dT%H:%M:%SZ")
    return render_template_string(
        FILE_MANAGER_HTML,
        token=token,
        project_name=data["project_name"],
        expires_at=expires_str,
    )


@app.route("/fm/<token>/api/list")
def api_list(token):
    data = validate_token(token)
    if not data:
        return jsonify({"error": "Session expired"}), 401
    rel_dir = request.args.get("dir", "")
    try:
        target = safe_path(data["project_path"], rel_dir)
    except ValueError:
        return jsonify({"error": "Invalid path"}), 400
    if not os.path.isdir(target):
        return jsonify({"error": "Not a directory"}), 400
    files = []
    try:
        for entry in sorted(os.scandir(target), key=lambda e: (not e.is_dir(), e.name.lower())):
            if entry.name.startswith(".") and entry.name != ".env":
                continue
            files.append({
                "name": entry.name,
                "is_dir": entry.is_dir(),
                "size": entry.stat().st_size if not entry.is_dir() else 0,
            })
    except PermissionError:
        return jsonify({"error": "Permission denied"}), 403
    return jsonify({"files": files})


@app.route("/fm/<token>/api/read")
def api_read(token):
    data = validate_token(token)
    if not data:
        return jsonify({"error": "Session expired"}), 401
    rel_path = request.args.get("path", "")
    try:
        target = safe_path(data["project_path"], rel_path)
    except ValueError:
        return jsonify({"error": "Invalid path"}), 400
    if not os.path.isfile(target):
        return jsonify({"error": "Not a file"}), 400
    if os.path.getsize(target) > 2 * 1024 * 1024:  # 2MB limit
        return jsonify({"error": "File too large to edit (>2MB)"}), 400
    try:
        with open(target, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"content": content})


@app.route("/fm/<token>/api/write", methods=["POST"])
def api_write(token):
    data = validate_token(token)
    if not data:
        return jsonify({"error": "Session expired"}), 401
    payload = request.get_json()
    if not payload:
        return jsonify({"error": "Invalid request"}), 400
    rel_path = payload.get("path", "")
    content = payload.get("content", "")
    try:
        target = safe_path(data["project_path"], rel_path)
    except ValueError:
        return jsonify({"error": "Invalid path"}), 400
    os.makedirs(os.path.dirname(target), exist_ok=True)
    try:
        with open(target, "w", encoding="utf-8") as f:
            f.write(content)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"ok": True})


@app.route("/fm/<token>/api/mkdir", methods=["POST"])
def api_mkdir(token):
    data = validate_token(token)
    if not data:
        return jsonify({"error": "Session expired"}), 401
    payload = request.get_json()
    rel_path = payload.get("path", "")
    try:
        target = safe_path(data["project_path"], rel_path)
    except ValueError:
        return jsonify({"error": "Invalid path"}), 400
    os.makedirs(target, exist_ok=True)
    return jsonify({"ok": True})


@app.route("/fm/<token>/api/delete", methods=["POST"])
def api_delete(token):
    data = validate_token(token)
    if not data:
        return jsonify({"error": "Session expired"}), 401
    payload = request.get_json()
    rel_path = payload.get("path", "")
    try:
        target = safe_path(data["project_path"], rel_path)
    except ValueError:
        return jsonify({"error": "Invalid path"}), 400
    if not os.path.exists(target):
        return jsonify({"error": "Path not found"}), 404
    try:
        if os.path.isdir(target):
            shutil.rmtree(target)
        else:
            os.remove(target)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"ok": True})


@app.route("/fm/<token>/api/rename", methods=["POST"])
def api_rename(token):
    data = validate_token(token)
    if not data:
        return jsonify({"error": "Session expired"}), 401
    payload = request.get_json()
    old_rel = payload.get("old_path", "")
    new_rel = payload.get("new_path", "")
    try:
        old_target = safe_path(data["project_path"], old_rel)
        new_target = safe_path(data["project_path"], new_rel)
    except ValueError:
        return jsonify({"error": "Invalid path"}), 400
    if not os.path.exists(old_target):
        return jsonify({"error": "Source not found"}), 404
    if os.path.exists(new_target):
        return jsonify({"error": "Destination already exists"}), 400
    try:
        os.rename(old_target, new_target)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"ok": True})


@app.route("/fm/<token>/api/upload", methods=["POST"])
def api_upload(token):
    data = validate_token(token)
    if not data:
        return jsonify({"error": "Session expired"}), 401
    rel_dir = request.form.get("dir", "")
    try:
        target_dir = safe_path(data["project_path"], rel_dir)
    except ValueError:
        return jsonify({"error": "Invalid path"}), 400
    os.makedirs(target_dir, exist_ok=True)
    file = request.files.get("file")
    if not file:
        return jsonify({"error": "No file"}), 400
    filename = secure_filename(file.filename)
    file.save(os.path.join(target_dir, filename))
    return jsonify({"ok": True, "name": filename})


@app.route("/fm/<token>/api/download")
def api_download(token):
    data = validate_token(token)
    if not data:
        return EXPIRED_HTML, 401
    rel_path = request.args.get("path", "")
    try:
        target = safe_path(data["project_path"], rel_path)
    except ValueError:
        abort(400)
    if not os.path.isfile(target):
        abort(404)
    return send_file(target, as_attachment=True)


@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "God Madara File Manager", "base_url": os.environ.get("BASE_URL", "not set")})


def run_flask(port: int = 8080):
    """Run Flask in a background thread."""
    import logging
    log = logging.getLogger("werkzeug")
    log.setLevel(logging.ERROR)
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

# ==========================================
 
