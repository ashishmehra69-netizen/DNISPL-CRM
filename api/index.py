"""Vercel API entrypoint for DNISPL CRM."""

from pathlib import Path
import importlib.util

backend_path = Path(__file__).resolve().with_name("backend.py")
spec = importlib.util.spec_from_file_location("backend_main", backend_path)
backend = importlib.util.module_from_spec(spec)
spec.loader.exec_module(backend)

app = backend.app
