"""Build and verify the installable integration and its version/provenance."""

import json
from pathlib import Path
import tomllib
from zipfile import ZIP_DEFLATED, ZipFile

root = Path(__file__).resolve().parent
source = root / "custom_components" / "registros_camaras"
manifest = json.loads((source / "manifest.json").read_text())
project = tomllib.loads((root / "pyproject.toml").read_text())["project"]
lock = tomllib.loads((root / "uv.lock").read_text())
locked = [package for package in lock["package"] if package["name"] == project["name"]]
if (
    len(locked) != 1
    or manifest["version"] != project["version"]
    or locked[0]["version"] != project["version"]
):
    raise SystemExit("Integration manifest, project and lock versions must match")
target = root / "dist" / "registros_camaras.zip"
target.parent.mkdir(exist_ok=True)
with ZipFile(target, "w", compression=ZIP_DEFLATED) as archive:
    archive.write(root / "LICENSE", "custom_components/registros_camaras/LICENSE")
    archive.write(root / "NOTICE", "custom_components/registros_camaras/NOTICE")
    for path in sorted(source.rglob("*")):
        if path.is_file() and path.suffix in {".py", ".json", ".png", ".yaml"}:
            archive.write(path, path.relative_to(root))
with ZipFile(target) as archive:
    if archive.testzip() is not None:
        raise SystemExit("Integration archive failed its integrity check")
    prefix = "custom_components/registros_camaras/"
    if any(not name.startswith(prefix) for name in archive.namelist()):
        raise SystemExit("Integration archive has an invalid installation layout")
    if json.loads(archive.read(prefix + "manifest.json")) != manifest:
        raise SystemExit("Packaged integration manifest differs from source")
    for name in ("LICENSE", "NOTICE"):
        if archive.read(prefix + name) != (root / name).read_bytes():
            raise SystemExit(f"Packaged {name} differs from project attribution")
print(target)
