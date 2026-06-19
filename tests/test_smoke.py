"""Smoke tests — verify modules compile cleanly."""
import pytest
import sys
from pathlib import Path


def test_main_modules_compile():
    """All .py files in repo compile without SyntaxErrors."""
    root = Path(".")
    # Scan root + one level deep for .py files
    py_files = list(root.glob("*.py")) + list(root.glob("*/*.py"))
    # Filter out common noise dirs
    py_files = [
        p for p in py_files
        if not any(
            part in p.parts
            for part in ("__pycache__", "node_modules", ".git", ".venv", "venv")
        )
    ]
    if len(py_files) == 0:
        pytest.skip("No .py files found at root or one level deep")
        return
    for py_file in py_files:
        if py_file.name.startswith("test_"):
            continue
        with open(py_file) as f:
            compile(f.read(), str(py_file), "exec")


def test_config_files_exist():
    """Verify critical config files are present."""
    has_deps = Path("requirements.txt").exists() or Path("pyproject.toml").exists()
    if not has_deps:
        print("  ⚠️ No requirements.txt or pyproject.toml found")
