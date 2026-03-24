import asyncio
import os
import subprocess


async def auto_install(project_path: str) -> dict:
    req_file = os.path.join(project_path, "requirements.txt")
    result = {"installed": [], "failed": [], "skipped": False}

    if not os.path.exists(req_file):
        result["skipped"] = True
        return result

    with open(req_file, "r") as f:
        packages = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    for pkg in packages:
        try:
            proc = await asyncio.create_subprocess_exec(
                "pip", "install", pkg, "--quiet",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode == 0:
                result["installed"].append(pkg)
            else:
                result["failed"].append(pkg)
        except Exception as e:
            result["failed"].append(f"{pkg} (error: {e})")

    return result
  
