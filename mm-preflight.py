#!/usr/bin/env python3
"""Market Monday preflight — syntax check before pipeline runs.
Checks:
  - Python syntax on all .py files in scripts/
  - Required files exist
  - Required env vars set (optional check)
"""
import ast
import os
import sys
import subprocess

SCRIPTS_DIR = "/home/ubuntu/market-monday/scripts"
REQUIRED_FILES = [
    "market-monday-pipeline.py",
    "market-monday-post.py",
]
DATA_DIR = os.path.expanduser("~/.hermes/market_monday")

def check_syntax():
    """Check Python syntax for all .py files."""
    errors = []
    for root, dirs, files in os.walk("/home/ubuntu/market-monday"):
        # Skip venv and test dirs
        dirs[:] = [d for d in dirs if d not in ("venv", "__pycache__", "tests", "cron-wrappers")]
        for f in files:
            if f.endswith(".py"):
                path = os.path.join(root, f)
                try:
                    with open(path, "r") as fp:
                        ast.parse(fp.read())
                except SyntaxError as e:
                    errors.append(f"{path}:{e.lineno}: {e.msg}")
    return errors

def check_required_files():
    """Check that required files exist."""
    missing = []
    for f in REQUIRED_FILES:
        path = os.path.join(SCRIPTS_DIR, f)
        if not os.path.exists(path):
            missing.append(path)
    return missing

def check_data_dir():
    """Check that data directory exists."""
    if not os.path.isdir(DATA_DIR):
        return [DATA_DIR]
    return []

def main():
    all_ok = True
    
    # Syntax check
    errors = check_syntax()
    if errors:
        print("❌ Syntax errors found:")
        for e in errors:
            print(f"   {e}")
        all_ok = False
    else:
        print("✅ Syntax OK")
    
    # Required files
    missing = check_required_files()
    if missing:
        print("❌ Missing required files:")
        for m in missing:
            print(f"   {m}")
        all_ok = False
    else:
        print("✅ Required files exist")
    
    # Data dir
    missing_data = check_data_dir()
    if missing_data:
        print("⚠️ Data directory missing, creating:")
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            print(f"   Created: {DATA_DIR}")
        except Exception as e:
            print(f"   ❌ Could not create: {e}")
            all_ok = False
    else:
        print("✅ Data directory exists")
    
    if all_ok:
        print("\n🚀 Preflight passed — pipeline ready")
        sys.exit(0)
    else:
        print("\n❌ Preflight failed")
        sys.exit(1)

if __name__ == "__main__":
    main()