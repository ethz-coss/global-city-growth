

from pathlib import Path
import hashlib
import yaml
import sys
from typing import Dict, Any


def sha256_file(path: Path, bufsize: int) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(bufsize)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()
    

def get_locked_manifest(manifest: Dict[str, Any], data_root_path: Path) -> Dict[str, Any]:
    locked_manifest = {"datasets": []}
    for dataset in manifest.get("datasets", []):
        dataset_id = dataset.get("id")
        file_specs = dataset.get("files")

        relative_paths = []
        if "list" in file_specs:
            relative_paths.extend(file_specs["list"])

        elif "template" in file_specs:
            template = file_specs["template"]
            years = dataset.get("params").get("years")
            for year in years:
                relative_paths.append(template.format(year=year))

        file_records = []
        for relative_path in sorted(relative_paths):
            print(relative_path)
            full_path = data_root_path / relative_path

            if not full_path.exists():
                print(f"WARN: Missing file for dataset '{dataset_id}': {relative_path}", file=sys.stderr)
            
            sha256 = sha256_file(full_path, 1 << 20)
            file_records.append({
                "path": relative_path,
                "size_bytes": full_path.stat().st_size,
                "sha256": sha256
            })

        locked_manifest["datasets"].append({
            "id": dataset_id,
            "files": file_records
        })
    return locked_manifest


def save_locked_manifest(manifest_path: Path, locked_manifest_path: Path, data_root_path: Path):
    manifest = yaml.safe_load(manifest_path.read_text())
    locked_manifest = get_locked_manifest(manifest=manifest, data_root_path=data_root_path)
    locked_manifest_path.write_text(yaml.safe_dump(locked_manifest, sort_keys=False))


if __name__ == "__main__":
    data_root_path = Path("/Users/andrea/Desktop/PhD/Data/Pipeline/global-city-growth/data")
    save_locked_manifest(manifest_path=Path("/Users/andrea/Desktop/PhD/Data/Pipeline/global-city-growth/src/download/data_manifest.yml"), locked_manifest_path=Path("/Users/andrea/Desktop/PhD/Data/Pipeline/global-city-growth/src/download/data_manifest.lock.yml"), data_root_path=data_root_path)