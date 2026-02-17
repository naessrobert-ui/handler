from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _streamlit_cmd(app_file: Path) -> list[str]:
    """Bygg streamlit-kommando med forutsigbare defaults for server-drift."""
    port = os.getenv("HANDLER_STREAMLIT_PORT", os.getenv("PORT", "8501"))
    address = os.getenv("HANDLER_STREAMLIT_ADDRESS", "0.0.0.0")

    return [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_file),
        "--server.headless=true",
        f"--server.address={address}",
        f"--server.port={port}",
    ]


def main() -> int:
    """Starter Streamlit-appen fra prosjektroten til handler.

    Scriptet kan kalles fra et annet repo (f.eks. prisanalyse) uten at man må
    stå i riktig arbeidskatalog først.
    """

    app_dir = Path(__file__).resolve().parent
    app_file = app_dir / "main.py"

    if not app_file.exists():
        print(f"Fant ikke appfil: {app_file}", file=sys.stderr)
        return 1

    cmd = _streamlit_cmd(app_file)

    # Bevar eksisterende miljøvariabler slik at HANDLER_* kan settes fra caller.
    env = os.environ.copy()

    print("Starter handler-app ...")
    print("Kommando:", " ".join(cmd))
    if env.get("HANDLER_OSLO_BORS_URL"):
        print("HANDLER_OSLO_BORS_URL er satt til:", env["HANDLER_OSLO_BORS_URL"])
    else:
        print("Tips: sett HANDLER_OSLO_BORS_URL i prisanalyse for menylenken.")

    return subprocess.call(cmd, cwd=str(app_dir), env=env)


if __name__ == "__main__":
    raise SystemExit(main())
