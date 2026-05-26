#!/usr/bin/env python3
"""Download the latest JUMP Hub/JUMPrr Zenodo record.

The script is resumable: partial files are saved with a `.part` suffix and
`curl -C -` continues from the existing size.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(os.environ.get("JUMP_SERVER_ROOT", "/srv/jump"))
DEST = ROOT / "data" / "jump_hub_zenodo"
LOG = ROOT / "logs" / "download_zenodo.log"
MANIFEST_DIR = ROOT / "manifests"
LATEST_API = os.environ.get("JUMP_ZENODO_LATEST_API", "https://zenodo.org/api/records/15029005/versions/latest")


def log(message: str) -> None:
    line = f"[{time.strftime('%Y-%m-%dT%H:%M:%S%z')}] {message}"
    print(line, flush=True)
    with LOG.open("a") as handle:
        handle.write(line + "\n")


def human(size: int) -> str:
    value = float(size)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if value < 1024 or unit == "TB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{size} B"


def md5sum(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024 * 16), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    DEST.mkdir(parents=True, exist_ok=True)
    LOG.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)

    log(f"Fetching latest Zenodo record: {LATEST_API}")
    record = json.loads(urlopen(LATEST_API, timeout=60).read())
    record_id = record["id"]
    files = record["files"]

    (MANIFEST_DIR / f"zenodo_{record_id}_record.json").write_text(json.dumps(record, indent=2))
    (MANIFEST_DIR / f"zenodo_{record_id}_files.json").write_text(
        json.dumps(
            [
                {
                    "key": item["key"],
                    "size": item["size"],
                    "checksum": item.get("checksum"),
                    "url": item["links"]["self"],
                }
                for item in files
            ],
            indent=2,
        )
    )

    log(f"Zenodo record {record_id}: {len(files)} files, total {human(sum(item['size'] for item in files))}")

    for index, item in enumerate(files, 1):
        key = item["key"]
        expected_size = int(item["size"])
        checksum = item.get("checksum", "")
        expected_md5 = checksum.split(":", 1)[1] if checksum.startswith("md5:") else None
        final = DEST / key
        part = DEST / f"{key}.part"
        url = item["links"]["self"]

        if final.exists() and final.stat().st_size == expected_size:
            log(f"[{index}/{len(files)}] SKIP size-ok {key} ({human(expected_size)})")
            continue

        if final.exists() and final.stat().st_size != expected_size:
            log(f"[{index}/{len(files)}] Resuming mismatched final file as part: {key}")
            if part.exists():
                part.unlink()
            final.rename(part)

        log(f"[{index}/{len(files)}] Downloading {key} ({human(expected_size)})")
        rc = subprocess.call(
            [
                "curl",
                "-L",
                "--fail",
                "--retry",
                "12",
                "--retry-delay",
                "20",
                "--retry-all-errors",
                "-C",
                "-",
                "-o",
                str(part),
                url,
            ]
        )
        if rc != 0:
            log(f"ERROR curl failed rc={rc} for {key}")
            return rc

        actual_size = part.stat().st_size
        if actual_size != expected_size:
            log(f"ERROR size mismatch for {key}: got {actual_size}, expected {expected_size}")
            return 2

        if expected_md5:
            log(f"Verifying md5 for {key}")
            actual_md5 = md5sum(part)
            if actual_md5 != expected_md5:
                log(f"ERROR md5 mismatch for {key}: got {actual_md5}, expected {expected_md5}")
                return 3

        part.rename(final)
        log(f"[{index}/{len(files)}] DONE {key}")

    log("Finished Zenodo downloads")
    subprocess.call(["du", "-sh", str(DEST)])
    return 0


if __name__ == "__main__":
    sys.exit(main())
