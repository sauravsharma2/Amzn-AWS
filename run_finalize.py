# run_finalize.py
from __future__ import annotations
import json
import os
import sys
import time
from pathlib import Path
from typing import Iterable, Mapping, Optional

OUTDIR = Path("output")
PRIMARY_TXT = OUTDIR / "output.txt"   # fichier principal attendu

PROJECT_NAME = "MSOF_direct_semi_direct_entries"

def _now_utc_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _write_manifest(
    project: str,
    run_id: str,
    started_at: str,
    status: str,
    exit_code: int,
    message: str,
    artifacts: list[dict],
    metrics: Optional[Mapping] = None,
) -> None:
    manifest = {
        "schema_version": "1.0",
        "project": project,
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": _now_utc_iso(),
        "status": status,
        "exit_code": exit_code,
        "message": message,
        "artifacts": artifacts,
        "metrics": dict(metrics or {}),
    }
    OUTDIR.mkdir(parents=True, exist_ok=True)
    with (OUTDIR / "manifest.json").open("w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(f"[MANIFEST] {OUTDIR/'manifest.json'}")

def finalize_run(
    *,
    project_name: str = PROJECT_NAME,
    started_at: Optional[str] = None,
    exit_code: int = 0,
    status: Optional[str] = None,
    message: Optional[str] = None,
    # artefacts additionnels optionnels (ex: Excel), chemins RELATIFS à la racine du projet
    extra_artifacts: Optional[Iterable[tuple[str, str, str, bool]]] = None,
    # métriques optionnelles
    metrics: Optional[Mapping] = None,
    # si True, on considère l'absence du TXT comme une erreur (exit_code=1 + status=error)
    require_primary_txt: bool = False,
) -> int:
    """
    Écrit manifest.json dans output/, en déclarant:
      - output.txt (s'il existe) comme artefact primaire table/tsv
      - + (optionnel) d'autres artefacts (path, type, format, primary)

    Paramètres:
      extra_artifacts: iterable de tuples (path, type, format, primary)
        ex: [("output/MyResults.xlsx", "file", "xlsx", False)]
    Retourne le code de sortie effectif (utile si tu veux sys.exit(...) après).
    """
    started_at = started_at or _now_utc_iso()
    run_id = time.strftime("%Y%m%d-%H%M%S")
    artifacts: list[dict] = []

    # Artefact principal (toujours supposé: output/output.txt)
    if PRIMARY_TXT.exists() and PRIMARY_TXT.is_file():
        artifacts.append({
            "path": PRIMARY_TXT.name,  # chemin RELATIF dans output/
            "type": "table",
            "format": "tsv",
            "primary": True,
        })
        print(f"[ARTIFACT] found {PRIMARY_TXT}")
    else:
        msg = f"[ARTIFACT] missing {PRIMARY_TXT} (will proceed)"
        print(msg)
        if require_primary_txt:
            status = "error"
            exit_code = 1
            message = (message or "run completed") + " | primary txt missing"

    # Artefacts additionnels
    for (p, t, fmt, prim) in (extra_artifacts or []):
        rel = Path(p)
        # On référence dans le manifest par NOM DE FICHIER pour rester local à output/
        # Si ton fichier est déjà en output/, rel.name suffit.
        artifacts.append({
            "path": rel.name,
            "type": t,
            "format": fmt,
            "primary": bool(prim),
        })
        print(f"[ARTIFACT] declare {rel} ({t}/{fmt}, primary={prim})")

    if status is None:
        status = "ok" if exit_code == 0 else "error"
    if message is None:
        message = "run completed" if exit_code == 0 else f"run completed with exit code {exit_code}"

    _write_manifest(
        project=project_name,
        run_id=run_id,
        started_at=started_at,
        status=status,
        exit_code=exit_code,
        message=message,
        artifacts=artifacts,
        metrics=metrics,
    )
    return exit_code
