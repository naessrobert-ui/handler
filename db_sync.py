from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Callable, Optional


def _copy_with_progress(src: Path, dst: Path, on_progress: Optional[Callable[[float], None]] = None, chunk_size: int = 8 * 1024 * 1024) -> None:
    """
    Kopierer fil i chunks og rapporterer progresjon 0.0–1.0.
    """
    total = src.stat().st_size
    copied = 0

    dst.parent.mkdir(parents=True, exist_ok=True)

    # copy2 = metadata, men vi streamer selv og legger metadata etterpå
    with src.open("rb") as fsrc, dst.open("wb") as fdst:
        while True:
            buf = fsrc.read(chunk_size)
            if not buf:
                break
            fdst.write(buf)
            copied += len(buf)
            if on_progress and total > 0:
                on_progress(min(copied / total, 1.0))

    shutil.copystat(src, dst)  # bevar timestamps osv.
    if on_progress:
        on_progress(1.0)


def ensure_local_db(
    remote_db_path: str,
    local_db_path: str,
    on_progress: Optional[Callable[[float, str], None]] = None,
    force: bool = False,
    copy_wal_shm: bool = True,
) -> dict:
    """
    Sørger for at DB finnes lokalt. Kopierer fra remote hvis:
      - lokal ikke finnes, eller
      - remote er nyere (mtime) / annen størrelse, eller
      - force=True

    on_progress: callback(progress_float_0_1, message)
    Returnerer info-dict med status.
    """
    remote = Path(remote_db_path)
    local = Path(local_db_path)

    if not remote.exists():
        raise FileNotFoundError(f"Fant ikke remote DB: {remote}")

    local.parent.mkdir(parents=True, exist_ok=True)

    # Avgør om vi skal kopiere
    do_copy = force or (not local.exists())
    reason = "force" if force else ("mangler lokalt" if not local.exists() else "")

    if not do_copy and local.exists():
        r_stat = remote.stat()
        l_stat = local.stat()
        if (r_stat.st_mtime > l_stat.st_mtime + 1) or (r_stat.st_size != l_stat.st_size):
            do_copy = True
            reason = "remote er nyere/ulik"

    if not do_copy:
        return {
            "copied": False,
            "reason": "lokal er oppdatert",
            "local_path": str(local),
            "remote_path": str(remote),
        }

    if on_progress:
        on_progress(0.0, f"Kopierer DB fra nettverk til lokal … ({reason})")

    # Kopier hoved-db med progress
    def _p(p: float) -> None:
        if on_progress:
            on_progress(p, f"Kopierer database … {int(p * 100)}%")

    _copy_with_progress(remote, local, on_progress=_p)

    # Valgfritt: kopier -wal/-shm hvis de finnes
    copied_sidecars = []
    if copy_wal_shm:
        for ext in ("-wal", "-shm"):
            side_remote = Path(str(remote) + ext)
            side_local = Path(str(local) + ext)
            if side_remote.exists():
                shutil.copy2(side_remote, side_local)
                copied_sidecars.append(side_remote.name)

    if on_progress:
        on_progress(1.0, "Lokal database er klar ✅")

    return {
        "copied": True,
        "reason": reason,
        "local_path": str(local),
        "remote_path": str(remote),
        "sidecars": copied_sidecars,
    }
