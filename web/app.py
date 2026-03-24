import os
import shutil
from flask import Flask, render_template, request, redirect, url_for, send_file, jsonify, abort
from werkzeug.utils import secure_filename
import asyncio
from database import verify_fm_token, get_project

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "godmadara_secret")


def run_async(coro):
    loop = asyncio.new_event_loop()
    result = loop.run_until_complete(coro)
    loop.close()
    return result


@app.route("/fm/<token>")
def file_manager(token):
    doc = run_async(verify_fm_token(token))
    if not doc:
        abort(403)
    project = run_async(get_project(doc["project_id"]))
    if not project:
        abort(404)

    project_path = project["project_path"]
    rel_path = request.args.get("path", "")
    current_path = os.path.normpath(os.path.join(project_path, rel_path))

    # Security: prevent path traversal
    if not current_path.startswith(project_path):
        current_path = project_path
        rel_path = ""

    items = []
    if os.path.isdir(current_path):
        for entry in sorted(os.scandir(current_path), key=lambda e: (not e.is_dir(), e.name)):
            item_rel = os.path.relpath(entry.path, project_path)
            items.append({
                "name": entry.name,
                "is_dir": entry.is_dir(),
                "rel_path": item_rel,
                "size": entry.stat().st_size if entry.is_file() else "-",
            })

    parent_rel = os.path.relpath(os.path.dirname(current_path), project_path)
    if parent_rel == ".":
        parent_rel = ""

    return render_template("filemanager.html",
                           token=token,
                           items=items,
                           current_rel=rel_path,
                           parent_rel=parent_rel,
                           project_name=project["project_name"])


@app.route("/fm/<token>/upload", methods=["POST"])
def upload_file(token):
    doc = run_async(verify_fm_token(token))
    if not doc:
        abort(403)
    project = run_async(get_project(doc["project_id"]))
    project_path = project["project_path"]
    rel_path = request.form.get("path", "")
    current_path = os.path.normpath(os.path.join(project_path, rel_path))
    if not current_path.startswith(project_path):
        current_path = project_path

    files = request.files.getlist("files")
    for f in files:
        if f.filename:
            fname = secure_filename(f.filename)
            f.save(os.path.join(current_path, fname))

    return redirect(url_for("file_manager", token=token, path=rel_path))


@app.route("/fm/<token>/delete", methods=["POST"])
def delete_file(token):
    doc = run_async(verify_fm_token(token))
    if not doc:
        abort(403)
    project = run_async(get_project(doc["project_id"]))
    project_path = project["project_path"]
    rel_path = request.form.get("item_path", "")
    full_path = os.path.normpath(os.path.join(project_path, rel_path))
    if not full_path.startswith(project_path):
        abort(403)

    if os.path.isfile(full_path):
        os.remove(full_path)
    elif os.path.isdir(full_path):
        shutil.rmtree(full_path)

    parent = os.path.relpath(os.path.dirname(full_path), project_path)
    if parent == ".":
        parent = ""
    return redirect(url_for("file_manager", token=token, path=parent))


@app.route("/fm/<token>/rename", methods=["POST"])
def rename_file(token):
    doc = run_async(verify_fm_token(token))
    if not doc:
        abort(403)
    project = run_async(get_project(doc["project_id"]))
    project_path = project["project_path"]
    old_rel = request.form.get("old_path", "")
    new_name = secure_filename(request.form.get("new_name", ""))
    old_full = os.path.normpath(os.path.join(project_path, old_rel))
    if not old_full.startswith(project_path):
        abort(403)

    new_full = os.path.join(os.path.dirname(old_full), new_name)
    os.rename(old_full, new_full)

    parent = os.path.relpath(os.path.dirname(old_full), project_path)
    if parent == ".":
        parent = ""
    return redirect(url_for("file_manager", token=token, path=parent))


@app.route("/fm/<token>/create", methods=["POST"])
def create_item(token):
    doc = run_async(verify_fm_token(token))
    if not doc:
        abort(403)
    project = run_async(get_project(doc["project_id"]))
    project_path = project["project_path"]
    rel_path = request.form.get("path", "")
    name = secure_filename(request.form.get("name", ""))
    item_type = request.form.get("type", "file")
    current_path = os.path.normpath(os.path.join(project_path, rel_path))
    if not current_path.startswith(project_path):
        abort(403)

    full = os.path.join(current_path, name)
    if item_type == "folder":
        os.makedirs(full, exist_ok=True)
    else:
        with open(full, "w") as f:
            f.write("")

    return redirect(url_for("file_manager", token=token, path=rel_path))


@app.route("/fm/<token>/edit", methods=["GET", "POST"])
def edit_file(token):
    doc = run_async(verify_fm_token(token))
    if not doc:
        abort(403)
    project = run_async(get_project(doc["project_id"]))
    project_path = project["project_path"]
    rel_path = request.args.get("path") or request.form.get("path", "")
    full_path = os.path.normpath(os.path.join(project_path, rel_path))
    if not full_path.startswith(project_path):
        abort(403)

    if request.method == "POST":
        content = request.form.get("content", "")
        with open(full_path, "w") as f:
            f.write(content)
        parent = os.path.relpath(os.path.dirname(full_path), project_path)
        if parent == ".":
            parent = ""
        return redirect(url_for("file_manager", token=token, path=parent))

    with open(full_path, "r", errors="replace") as f:
        content = f.read()

    return render_template("filemanager.html",
                           token=token,
                           edit_mode=True,
                           edit_path=rel_path,
                           edit_content=content,
                           project_name=project["project_name"],
                           items=[], current_rel="", parent_rel="")


@app.route("/fm/<token>/download")
def download_file_route(token):
    doc = run_async(verify_fm_token(token))
    if not doc:
        abort(403)
    project = run_async(get_project(doc["project_id"]))
    project_path = project["project_path"]
    rel_path = request.args.get("path", "")
    full_path = os.path.normpath(os.path.join(project_path, rel_path))
    if not full_path.startswith(project_path):
        abort(403)
    return send_file(full_path, as_attachment=True)
  
