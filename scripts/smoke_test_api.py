#!/usr/bin/env python3
"""Run bounded smoke tests against a JUMP Agent API deployment."""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request


BASE_URL = os.environ.get("JUMP_AGENT_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
API_KEY = os.environ.get("JUMP_API_KEY", "")
KEY_FILE = os.environ.get("JUMP_API_KEY_FILE", "/srv/jump/api_key")

if not API_KEY and os.path.exists(KEY_FILE):
    with open(KEY_FILE) as handle:
        API_KEY = handle.read().strip()


def call(method: str, path: str, payload: dict | None = None, timeout: int = 120) -> dict:
    data = None
    headers = {}
    if API_KEY:
        headers["Authorization"] = f"Bearer {API_KEY}"
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(BASE_URL + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return {"status": response.status, "body": json.loads(response.read().decode())}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
        raise RuntimeError(f"{method} {path} failed {exc.code}: {body}") from exc


TESTS = [
    ("GET", "/health", None),
    ("GET", "/jump-agent", None),
    ("GET", "/tools", None),
    ("GET", "/datasets", None),
    ("GET", "/resolve?q=JCP2022_085227", None),
    ("POST", "/activity/summary", {"dataset": "compound_no_source7", "preprocessing": "activity_no_target2", "filter": "all_sources", "activity_params": "default"}),
    ("POST", "/chemical/properties", {"ids": ["JCP2022_085227"]}),
    ("POST", "/annotations", {"table": "repurposing_hub_annotations", "ids": ["JCP2022_085227"]}),
    ("POST", "/wells/cell-counts", {"ids": ["JCP2022_085227"], "limit": 3}),
    ("POST", "/similarity/neighbors", {"id": "JCP2022_085227", "modality": "compound", "top_k": 3}),
    ("POST", "/features/interpretable", {"ids": ["JCP2022_085227"], "limit": 3}),
    ("POST", "/gallery/images", {"ids": ["JCP2022_085227"], "limit": 1}),
    ("GET", "/artifacts/search?q=chemical-space&limit=3", None),
    ("GET", "/artifacts/read?relative_path=chemical-space/chemical_space_summary.json", None),
    ("GET", "/provenance", None),
]


def summarize(path: str, result: dict) -> dict:
    body = result["body"]
    summary = {"status": result["status"], "keys": list(body.keys())[:8]}
    if isinstance(body.get("results"), list):
        summary["n_results"] = len(body["results"])
    if "summary" in body:
        summary["summary"] = body["summary"]
    if path == "/health":
        summary["ok"] = body.get("ok")
    return summary


def main() -> int:
    for method, path, payload in TESTS:
        result = call(method, path, payload)
        print(f"PASS {method} {path} {json.dumps(summarize(path, result), default=str)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
