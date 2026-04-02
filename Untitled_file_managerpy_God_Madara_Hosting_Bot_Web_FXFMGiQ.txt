# file_manager.py — God Madara Hosting Bot | Web File Manager (FINAL)
# Flask-based browser file manager with token auth, dark UI, syntax highlighting
# Run standalone: python file_manager.py
# Deployed as a separate Render Web Service on port 8080

import os
import re
import sys
import mimetypes
import shutil
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from flask import (
    Flask, render_template_string, request, redirect,
    url_for, jsonify, send_from_directory, abort, flash
)
from dotenv import load_dotenv
from werkzeug.utils import secure_filename

load_dotenv()

FM_PORT      = int(os.environ.get("FILE_MANAGER_PORT", 8080))
FM_SECRET    = os.environ.get("FM_SECRET", "changeme_secret")
PROJECTS_DIR = Path(os.environ.get("PROJECTS_DIR", "projects"))
TOKENS_DIR   = Path("fm_tokens")

app = Flask(__name__)
app.secret_key = FM_SECRET

# ─── Token helpers ────────────────────────────────────────────────────────────

def validate_token(token: str) -> dict | None:
    """Returns {project_id, user_id} or None if invalid/expired."""
    token = re.sub(r'[^a-f0-9]', '', token)
    token_file = TOKENS_DIR / f"{token}.txt"
    if not token_file.exists():
        return None
    try:
        lines = token_file.read_text().strip().splitlines()
        project_id = lines[0]
        user_id    = int(lines[1])
        expiry     = datetime.fromisoformat(lines[2])
        if datetime.utcnow() > expiry:
            token_file.unlink(missing_ok=True)
            return None
        return {"project_id": project_id, "user_id": user_id}
    except Exception:
        return None


def safe_path(base: Path, rel: str) -> Path | None:
    """Resolve a relative path safely, preventing directory traversal."""
    try:
        resolved = (base / rel).resolve()
        if not str(resolved).startswith(str(base.resolve())):
            return None
        return resolved
    except Exception:
        return None


def human_size(size: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


SUPPORTED_EDITABLE = {
    ".py", ".txt", ".md", ".json", ".yaml", ".yml", ".toml",
    ".cfg", ".ini", ".env", ".sh", ".js", ".ts", ".html",
    ".css", ".xml", ".csv", ".log", ".conf", ".dockerfile",
}

def is_editable(filename: str) -> bool:
    return Path(filename).suffix.lower() in SUPPORTED_EDITABLE


# ─── Dark-themed HTML template ────────────────────────────────────────────────

BASE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>📂 God Madara File Manager</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github-dark.min.css">
<script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js"></script>
<style>
  :root {
    --bg: #0d1117; --surface: #161b22; --border: #30363d;
    --text: #c9d1d9; --muted: #8b949e; --accent: #58a6ff;
    --danger: #f85149; --success: #3fb950; --warning: #d29922;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', system-ui, sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; }
  header { background: var(--surface); border-bottom: 1px solid var(--border); padding: 14px 24px; display: flex; align-items: center; gap: 12px; }
  header h1 { font-size: 1.2rem; color: var(--accent); }
  header span { color: var(--muted); font-size: .85rem; }
  .container { max-width: 1100px; margin: 0 auto; padding: 20px; }
  .breadcrumb { display: flex; flex-wrap: wrap; gap: 4px; align-items: center; margin-bottom: 16px; font-size: .9rem; }
  .breadcrumb a { color: var(--accent); text-decoration: none; } .breadcrumb a:hover { text-decoration: underline; }
  .breadcrumb span { color: var(--muted); }
  .toolbar { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 16px; }
  .btn { padding: 7px 14px; border: none; border-radius: 6px; cursor: pointer; font-size: .85rem; font-weight: 600; text-decoration: none; display: inline-flex; align-items: center; gap: 6px; transition: opacity .15s; }
  .btn:hover { opacity: .85; }
  .btn-primary { background: var(--accent); color: #0d1117; }
  .btn-danger  { background: var(--danger); color: #fff; }
  .btn-success { background: var(--success); color: #0d1117; }
  .btn-muted   { background: var(--border); color: var(--text); }
  .file-table { width: 100%; border-collapse: collapse; background: var(--surface); border-radius: 8px; overflow: hidden; }
  .file-table th { background: #1c2128; padding: 10px 14px; text-align: left; font-size: .8rem; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; border-bottom: 1px solid var(--border); }
  .file-table td { padding: 10px 14px; border-bottom: 1px solid var(--border); font-size: .9rem; }
  .file-table tr:last-child td { border-bottom: none; }
  .file-table tr:hover td { background: #1c2128; }
  .file-icon { margin-right: 6px; }
  .file-link { color: var(--text); text-decoration: none; } .file-link:hover { color: var(--accent); }
  .actions { display: flex; gap: 6px; }
  .actions a, .actions button { font-size: .78rem; padding: 3px 9px; }
  .modal-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.7); z-index: 1000; align-items: center; justify-content: center; }
  .modal-overlay.active { display: flex; }
  .modal { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 24px; width: min(500px, 95vw); }
  .modal h3 { margin-bottom: 16px; color: var(--accent); }
  .form-group { margin-bottom: 14px; }
  label { display: block; font-size: .85rem; margin-bottom: 5px; color: var(--muted); }
  input[type=text], textarea, select { width: 100%; padding: 8px 10px; background: var(--bg); border: 1px solid var(--border); color: var(--text); border-radius: 6px; font-size: .9rem; }
  textarea { min-height: 120px; resize: vertical; }
  .editor-area { background: var(--bg); border: 1px solid var(--border); border-radius: 8px; padding: 0; overflow: hidden; }
  .editor-header { background: var(--surface); padding: 10px 16px; display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid var(--border); font-size: .85rem; }
  #code-editor { width: 100%; min-height: 60vh; padding: 16px; background: var(--bg); color: var(--text); border: none; outline: none; font-family: 'JetBrains Mono', 'Fira Code', monospace; font-size: .88rem; resize: none; tab-size: 4; }
  .flash { padding: 10px 14px; border-radius: 6px; margin-bottom: 12px; font-size: .9rem; }
  .flash.error   { background: #2d1216; border: 1px solid var(--danger); color: var(--danger); }
  .flash.success { background: #0d2818; border: 1px solid var(--success); color: var(--success); }
  .empty-dir { text-align: center; padding: 40px; color: var(--muted); }
  @media (max-width: 600px) { .file-table th:nth-child(3), .file-table td:nth-child(3) { display: none; } }
</style>
</head>
<body>
<header>
  <div>
    <h1>📂 God Madara File Manager</h1>
    <span>Project: {{ project_name }} &nbsp;|&nbsp; Session expires: {{ expiry }}</span>
  </div>
</header>
<div class="container">
{% with messages = get_flashed_messages(with_categories=true) %}
  {% for category, message in messages %}
    <div class="flash {{ category }}">{{ message }}</div>
  {% endfor %}
{% endwith %}
{% block content %}{% endblock %}
</div>
{% block modals %}{% endblock %}
{% block scripts %}{% endblock %}
</body>
</html>"""

BROWSER_TEMPLATE = BASE_TEMPLATE.replace("{% block content %}{% endblock %}", """
{% block content %}
<div class="breadcrumb">
  <a href="{{ url_for('browse', token=token, rel='') }}">🏠 Root</a>
  {% for crumb in breadcrumbs %}
    <span>/</span>
    <a href="{{ url_for('browse', token=token, rel=crumb.rel) }}">{{ crumb.name }}</a>
  {% endfor %}
</div>
<div class="toolbar">
  <button class="btn btn-primary" onclick="showModal('modal-newfile')">➕ New File</button>
  <button class="btn btn-primary" onclick="showModal('modal-newfolder')">📁 New Folder</button>
  <label class="btn btn-muted" style="cursor:pointer">
    📤 Upload <input type="file" multiple hidden onchange="uploadFiles(this)">
  </label>
</div>
{% if files %}
<table class="file-table">
  <thead><tr><th>Name</th><th>Size</th><th>Modified</th><th>Actions</th></tr></thead>
  <tbody>
  {% for f in files %}
  <tr>
    <td>
      <span class="file-icon">{{ '📁' if f.is_dir else '📄' }}</span>
      {% if f.is_dir %}
        <a class="file-link" href="{{ url_for('browse', token=token, rel=f.rel) }}">{{ f.name }}</a>
      {% elif f.editable %}
        <a class="file-link" href="{{ url_for('edit_file', token=token, rel=f.rel) }}">{{ f.name }}</a>
      {% else %}
        <a class="file-link" href="{{ url_for('download_file', token=token, rel=f.rel) }}" download>{{ f.name }}</a>
      {% endif %}
    </td>
    <td>{{ f.size }}</td>
    <td>{{ f.mtime }}</td>
    <td>
      <div class="actions">
        {% if not f.is_dir and f.editable %}
          <a class="btn btn-muted" href="{{ url_for('edit_file', token=token, rel=f.rel) }}">✏️</a>
        {% endif %}
        {% if not f.is_dir %}
          <a class="btn btn-muted" href="{{ url_for('download_file', token=token, rel=f.rel) }}" download>⬇️</a>
        {% endif %}
        <button class="btn btn-muted" onclick="renameItem('{{ f.rel }}', '{{ f.name }}')">🔤</button>
        <button class="btn btn-danger" onclick="deleteItem('{{ f.rel }}', {{ 'true' if f.is_dir else 'false' }})">🗑️</button>
      </div>
    </td>
  </tr>
  {% endfor %}
  </tbody>
</table>
{% else %}
<div class="empty-dir">📭 This directory is empty.</div>
{% endif %}
{% endblock %}""").replace("{% block modals %}{% endblock %}", """
{% block modals %}
<!-- New File Modal -->
<div class="modal-overlay" id="modal-newfile">
  <div class="modal">
    <h3>➕ Create New File</h3>
    <form method="POST" action="{{ url_for('create_file', token=token) }}">
      <input type="hidden" name="dir" value="{{ current_rel }}">
      <div class="form-group">
        <label>Filename</label>
        <input type="text" name="filename" placeholder="main.py" required autofocus>
      </div>
      <div style="display:flex;gap:8px;justify-content:flex-end">
        <button type="button" class="btn btn-muted" onclick="hideModal('modal-newfile')">Cancel</button>
        <button type="submit" class="btn btn-primary">Create</button>
      </div>
    </form>
  </div>
</div>
<!-- New Folder Modal -->
<div class="modal-overlay" id="modal-newfolder">
  <div class="modal">
    <h3>📁 Create New Folder</h3>
    <form method="POST" action="{{ url_for('create_folder', token=token) }}">
      <input type="hidden" name="dir" value="{{ current_rel }}">
      <div class="form-group">
        <label>Folder Name</label>
        <input type="text" name="foldername" placeholder="my_folder" required autofocus>
      </div>
      <div style="display:flex;gap:8px;justify-content:flex-end">
        <button type="button" class="btn btn-muted" onclick="hideModal('modal-newfolder')">Cancel</button>
        <button type="submit" class="btn btn-primary">Create</button>
      </div>
    </form>
  </div>
</div>
<!-- Rename Modal -->
<div class="modal-overlay" id="modal-rename">
  <div class="modal">
    <h3>🔤 Rename</h3>
    <form method="POST" action="{{ url_for('rename_item', token=token) }}">
      <input type="hidden" name="rel" id="rename-rel">
      <div class="form-group">
        <label>New Name</label>
        <input type="text" name="newname" id="rename-newname" required autofocus>
      </div>
      <div style="display:flex;gap:8px;justify-content:flex-end">
        <button type="button" class="btn btn-muted" onclick="hideModal('modal-rename')">Cancel</button>
        <button type="submit" class="btn btn-primary">Rename</button>
      </div>
    </form>
  </div>
</div>
<!-- Delete confirm Modal -->
<div class="modal-overlay" id="modal-delete">
  <div class="modal">
    <h3>🗑️ Confirm Delete</h3>
    <p style="margin-bottom:16px;color:var(--muted)">Are you sure you want to delete <strong id="delete-name"></strong>?</p>
    <form method="POST" action="{{ url_for('delete_item', token=token) }}">
      <input type="hidden" name="rel" id="delete-rel">
      <div style="display:flex;gap:8px;justify-content:flex-end">
        <button type="button" class="btn btn-muted" onclick="hideModal('modal-delete')">Cancel</button>
        <button type="submit" class="btn btn-danger">Delete</button>
      </div>
    </form>
  </div>
</div>
{% endblock %}""").replace("{% block scripts %}{% endblock %}", """
{% block scripts %}
<script>
function showModal(id) { document.getElementById(id).classList.add('active'); }
function hideModal(id) { document.getElementById(id).classList.remove('active'); }
function renameItem(rel, name) {
  document.getElementById('rename-rel').value = rel;
  document.getElementById('rename-newname').value = name;
  showModal('modal-rename');
}
function deleteItem(rel, isDir) {
  document.getElementById('delete-rel').value = rel;
  document.getElementById('delete-name').textContent = rel.split('/').pop();
  showModal('modal-delete');
}
function uploadFiles(input) {
  const files = Array.from(input.files);
  if (!files.length) return;
  const fd = new FormData();
  files.forEach(f => fd.append('files', f));
  fd.append('dir', '{{ current_rel }}');
  fetch('{{ url_for("upload_files", token=token) }}', { method: 'POST', body: fd })
    .then(r => r.json())
    .then(d => { if (d.ok) location.reload(); else alert('Upload failed: ' + d.error); });
}
document.querySelectorAll('.modal-overlay').forEach(el => {
  el.addEventListener('click', e => { if (e.target === el) el.classList.remove('active'); });
});
</script>
{% endblock %}""")

EDITOR_TEMPLATE = BASE_TEMPLATE.replace("{% block content %}{% endblock %}", """
{% block content %}
<div class="breadcrumb">
  <a href="{{ url_for('browse', token=token, rel='') }}">🏠 Root</a>
  <span>/</span>
  <span>{{ rel }}</span>
</div>
<div class="editor-area">
  <div class="editor-header">
    <span>✏️ Editing: <strong>{{ filename }}</strong></span>
    <div style="display:flex;gap:8px">
      <a class="btn btn-muted" href="{{ url_for('browse', token=token, rel=parent_rel) }}">⬅️ Back</a>
      <button class="btn btn-success" onclick="saveFile()">💾 Save</button>
    </div>
  </div>
  <textarea id="code-editor" spellcheck="false">{{ content }}</textarea>
</div>
{% endblock %}
{% block scripts %}
<script>
function saveFile() {
  const content = document.getElementById('code-editor').value;
  fetch('{{ url_for("save_file", token=token) }}', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ rel: '{{ rel }}', content: content })
  }).then(r => r.json()).then(d => {
    if (d.ok) {
      const btn = document.querySelector('.btn-success');
      btn.textContent = '✅ Saved!';
      setTimeout(() => { btn.textContent = '💾 Save'; }, 2000);
    } else { alert('Save failed: ' + d.error); }
  });
}
// Tab support in textarea
document.getElementById('code-editor').addEventListener('keydown', function(e) {
  if (e.key === 'Tab') {
    e.preventDefault();
    const s = this.selectionStart, end = this.selectionEnd;
    this.value = this.value.substring(0, s) + '    ' + this.value.substring(end);
    this.selectionStart = this.selectionEnd = s + 4;
  }
});
</script>
{% endblock %}""")


# ─── Route helpers ────────────────────────────────────────────────────────────

def get_token_and_base(token: str):
    """Returns (token_data, base_path) or aborts 403/404."""
    data = validate_token(token)
    if not data:
        abort(403)
    # Resolve base path via meta file written by bot.py
    meta_file = PROJECTS_DIR / f"meta_{data['project_id']}.txt"
    if meta_file.exists():
        base_path = Path(meta_file.read_text().strip())
    else:
        # Fallback: scan user dir
        user_dir = PROJECTS_DIR / str(data["user_id"])
        if not user_dir.exists():
            abort(404)
        candidates = [d for d in user_dir.iterdir() if d.is_dir()]
        if not candidates:
            abort(404)
        base_path = candidates[0]
    if not base_path.exists():
        abort(404)
    return data, base_path


def build_breadcrumbs(rel: str) -> list[dict]:
    if not rel:
        return []
    parts = Path(rel).parts
    crumbs = []
    for i, part in enumerate(parts):
        crumbs.append({"name": part, "rel": "/".join(parts[: i + 1])})
    return crumbs


def list_dir(base: Path, rel: str) -> list[dict]:
    target = safe_path(base, rel) if rel else base
    if not target or not target.is_dir():
        return []
    items = []
    for entry in sorted(target.iterdir(), key=lambda e: (not e.is_dir(), e.name.lower())):
        entry_rel = str(entry.relative_to(base))
        stat = entry.stat()
        items.append({
            "name": entry.name,
            "rel": entry_rel,
            "is_dir": entry.is_dir(),
            "size": human_size(stat.st_size) if not entry.is_dir() else "—",
            "mtime": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
            "editable": is_editable(entry.name) if not entry.is_dir() else False,
        })
    return items


def token_expiry_str(token: str) -> str:
    token_file = TOKENS_DIR / f"{token}.txt"
    if not token_file.exists():
        return "unknown"
    try:
        lines = token_file.read_text().strip().splitlines()
        expiry = datetime.fromisoformat(lines[2])
        remaining = expiry - datetime.utcnow()
        mins = int(remaining.total_seconds() / 60)
        return f"in {mins} min" if mins > 0 else "expired"
    except Exception:
        return "unknown"


def get_project_name(base_path: Path) -> str:
    return base_path.name


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route("/fm/<token>")
@app.route("/fm/<token>/browse")
def browse(token):
    rel = request.args.get("rel", "")
    _, base = get_token_and_base(token)
    files = list_dir(base, rel)
    return render_template_string(
        BROWSER_TEMPLATE,
        token=token,
        files=files,
        breadcrumbs=build_breadcrumbs(rel),
        current_rel=rel,
        project_name=get_project_name(base),
        expiry=token_expiry_str(token),
    )


@app.route("/fm/<token>/edit")
def edit_file(token):
    rel = request.args.get("rel", "")
    _, base = get_token_and_base(token)
    target = safe_path(base, rel)
    if not target or not target.is_file():
        flash("File not found.", "error")
        return redirect(url_for("browse", token=token, rel=""))
    try:
        content = target.read_text(errors="replace")
    except Exception:
        flash("Cannot read file.", "error")
        return redirect(url_for("browse", token=token, rel=""))
    parent_rel = str(Path(rel).parent) if rel else ""
    if parent_rel == ".":
        parent_rel = ""
    return render_template_string(
        EDITOR_TEMPLATE,
        token=token,
        rel=rel,
        filename=target.name,
        content=content,
        parent_rel=parent_rel,
        project_name=get_project_name(base),
        expiry=token_expiry_str(token),
    )


@app.route("/fm/<token>/save", methods=["POST"])
def save_file(token):
    _, base = get_token_and_base(token)
    data = request.get_json()
    if not data:
        return jsonify({"ok": False, "error": "No data"})
    rel = data.get("rel", "")
    content = data.get("content", "")
    target = safe_path(base, rel)
    if not target:
        return jsonify({"ok": False, "error": "Invalid path"})
    try:
        target.write_text(content, encoding="utf-8")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/fm/<token>/download")
def download_file(token):
    rel = request.args.get("rel", "")
    _, base = get_token_and_base(token)
    target = safe_path(base, rel)
    if not target or not target.is_file():
        abort(404)
    return send_from_directory(str(target.parent), target.name, as_attachment=True)


@app.route("/fm/<token>/create_file", methods=["POST"])
def create_file(token):
    _, base = get_token_and_base(token)
    dir_rel  = request.form.get("dir", "")
    filename = secure_filename(request.form.get("filename", "").strip())
    if not filename:
        flash("Invalid filename.", "error")
        return redirect(url_for("browse", token=token, rel=dir_rel))
    target_dir = safe_path(base, dir_rel) if dir_rel else base
    if not target_dir:
        flash("Invalid path.", "error")
        return redirect(url_for("browse", token=token, rel=dir_rel))
    new_file = target_dir / filename
    if new_file.exists():
        flash(f"File '{filename}' already exists.", "error")
        return redirect(url_for("browse", token=token, rel=dir_rel))
    new_file.touch()
    flash(f"✅ Created '{filename}'", "success")
    new_rel = str(new_file.relative_to(base))
    return redirect(url_for("edit_file", token=token, rel=new_rel))


@app.route("/fm/<token>/create_folder", methods=["POST"])
def create_folder(token):
    _, base = get_token_and_base(token)
    dir_rel    = request.form.get("dir", "")
    foldername = secure_filename(request.form.get("foldername", "").strip())
    if not foldername:
        flash("Invalid folder name.", "error")
        return redirect(url_for("browse", token=token, rel=dir_rel))
    target_dir = safe_path(base, dir_rel) if dir_rel else base
    if not target_dir:
        flash("Invalid path.", "error")
        return redirect(url_for("browse", token=token, rel=dir_rel))
    new_dir = target_dir / foldername
    if new_dir.exists():
        flash(f"Folder '{foldername}' already exists.", "error")
        return redirect(url_for("browse", token=token, rel=dir_rel))
    new_dir.mkdir(parents=True)
    flash(f"✅ Folder '{foldername}' created", "success")
    return redirect(url_for("browse", token=token, rel=dir_rel))


@app.route("/fm/<token>/rename", methods=["POST"])
def rename_item(token):
    _, base = get_token_and_base(token)
    rel     = request.form.get("rel", "")
    newname = secure_filename(request.form.get("newname", "").strip())
    if not newname or not rel:
        flash("Invalid rename request.", "error")
        return redirect(url_for("browse", token=token, rel=""))
    target = safe_path(base, rel)
    if not target or not target.exists():
        flash("Item not found.", "error")
        return redirect(url_for("browse", token=token, rel=""))
    new_target = target.parent / newname
    if new_target.exists():
        flash(f"'{newname}' already exists.", "error")
        parent_rel = str(target.parent.relative_to(base)) if target.parent != base else ""
        return redirect(url_for("browse", token=token, rel=parent_rel))
    target.rename(new_target)
    flash(f"✅ Renamed to '{newname}'", "success")
    parent_rel = str(new_target.parent.relative_to(base)) if new_target.parent != base else ""
    return redirect(url_for("browse", token=token, rel=parent_rel))


@app.route("/fm/<token>/delete", methods=["POST"])
def delete_item(token):
    _, base = get_token_and_base(token)
    rel = request.form.get("rel", "")
    if not rel:
        flash("Cannot delete root.", "error")
        return redirect(url_for("browse", token=token, rel=""))
    target = safe_path(base, rel)
    if not target or not target.exists():
        flash("Item not found.", "error")
        return redirect(url_for("browse", token=token, rel=""))
    parent_rel = str(target.parent.relative_to(base)) if target.parent != base else ""
    try:
        if target.is_dir():
            shutil.rmtree(str(target))
        else:
            target.unlink()
        flash(f"✅ Deleted '{target.name}'", "success")
    except Exception as e:
        flash(f"Delete failed: {e}", "error")
    return redirect(url_for("browse", token=token, rel=parent_rel))


@app.route("/fm/<token>/upload", methods=["POST"])
def upload_files(token):
    _, base = get_token_and_base(token)
    dir_rel  = request.form.get("dir", "")
    files    = request.files.getlist("files")
    if not files:
        return jsonify({"ok": False, "error": "No files"})
    target_dir = safe_path(base, dir_rel) if dir_rel else base
    if not target_dir:
        return jsonify({"ok": False, "error": "Invalid directory"})
    for f in files:
        fname = secure_filename(f.filename)
        if fname:
            f.save(str(target_dir / fname))
    return jsonify({"ok": True, "count": len(files)})


@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "God Madara File Manager"})


if __name__ == "__main__":
    TOKENS_DIR.mkdir(exist_ok=True)
    app.run(host="0.0.0.0", port=FM_PORT, debug=False)
