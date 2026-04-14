import sys
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


def resolve_dotenv_path(project_root: Optional[Path] = None) -> Optional[Path]:
    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        candidates = [
            exe_dir / "_internal" / ".env",
            exe_dir / ".env",
        ]
    else:
        root = Path(project_root).resolve() if project_root else Path(__file__).resolve().parents[1]
        candidates = [root / ".env"]

    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def load_app_env(project_root: Optional[Path] = None, *, override: bool = True) -> Optional[Path]:
    env_path = resolve_dotenv_path(project_root=project_root)
    if env_path is not None:
        load_dotenv(dotenv_path=env_path, override=override)
    return env_path
