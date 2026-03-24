import asyncio
import subprocess
import datetime
import os
import sys
from database import get_project, update_project, get_running_projects

# pid -> process object
running_processes: dict[str, subprocess.Popen] = {}


async def run_project(project_id: str) -> dict:
    project = await get_project(project_id)
    if not project:
        return {"success": False, "error": "Project not found"}

    project_path = project["project_path"]
    run_cmd = project.get("run_command", "python3 main.py")

    # Auto install requirements first
    from core.installer import auto_install
    install_result = await auto_install(project_path)

    try:
        log_file = open(os.path.join(project_path, "bot.log"), "a")
        process = subprocess.Popen(
            run_cmd.split(),
            cwd=project_path,
            stdout=log_file,
            stderr=log_file,
            preexec_fn=os.setsid
        )
        pid = process.pid
        running_processes[project_id] = process

        await update_project(project_id, {
            "status": "running",
            "pid": pid,
            "uptime_start": datetime.datetime.utcnow(),
            "last_run": datetime.datetime.utcnow(),
        })

        # Monitor in background
        asyncio.create_task(_monitor_process(project_id, process, log_file))

        return {"success": True, "pid": pid, "install_info": install_result}
    except Exception as e:
        await update_project(project_id, {"status": "crashed", "pid": None})
        return {"success": False, "error": str(e)}


async def _monitor_process(project_id: str, process: subprocess.Popen, log_file):
    loop = asyncio.get_event_loop()
    exit_code = await loop.run_in_executor(None, process.wait)
    log_file.close()

    status = "crashed" if exit_code != 0 else "stopped"
    await update_project(project_id, {
        "status": status,
        "pid": None,
        "last_exit_code": exit_code,
        "uptime_start": None,
    })
    if project_id in running_processes:
        del running_processes[project_id]


async def stop_project(project_id: str) -> dict:
    project = await get_project(project_id)
    if not project:
        return {"success": False, "error": "Project not found"}

    process = running_processes.get(project_id)
    if process:
        try:
            import signal
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        except Exception:
            process.kill()
        del running_processes[project_id]

    await update_project(project_id, {
        "status": "stopped",
        "pid": None,
        "uptime_start": None,
    })
    return {"success": True}


async def restart_project(project_id: str) -> dict:
    await stop_project(project_id)
    await asyncio.sleep(1)
    return await run_project(project_id)


async def get_logs(project_id: str, lines: int = 50) -> str:
    project = await get_project(project_id)
    if not project:
        return "Project not found"
    log_path = os.path.join(project["project_path"], "bot.log")
    if not os.path.exists(log_path):
        return "No logs yet."
    with open(log_path, "r", errors="replace") as f:
        all_lines = f.readlines()
    return "".join(all_lines[-lines:]) or "Log is empty."


def get_uptime_str(uptime_start) -> str:
    if not uptime_start:
        return "N/A"
    delta = datetime.datetime.utcnow() - uptime_start
    total = int(delta.total_seconds())
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h {m}m {s}s"


async def restore_running_projects():
    """Called on bot startup — restores previously running projects from DB"""
    projects = await get_running_projects()
    for proj in projects:
        pid = proj.get("pid")
        # Check if process is truly alive
        alive = False
        if pid:
            try:
                os.kill(pid, 0)
                alive = True
            except OSError:
                alive = False

        if not alive:
            # Restart it
            project_id = str(proj["_id"])
            await run_project(project_id)
