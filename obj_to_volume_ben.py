"""obj_to_volume_ben — local OBJ mesh → precomputed volume folder.

Improvements over the upstream `obj_to_volume`:
  - Works on Windows (no cloudvolume.Mesh.put → no intervaltree / colon errors).
  - Pulls `info` bounds from the datastack's EM-source info, not hardcoded.
  - Writes filenames with `___` substitutes (Windows-safe); `bucket_upload_folder_ben`
    translates them to ':' on upload.
"""
import os
import argparse
import sys
from pathlib import Path

import numpy as np
import trimesh

# Same portable resolution pattern as json_to_volume_ben.py — first locate the
# sibling _ben scripts and tracer_tools/src.
def _find_ben_dir():
    here = Path(__file__).resolve().parent
    if (here / "json_to_volume_ben.py").exists():
        return here
    for sib in (here / "tracer_tools" / "src", here.parent / "tracer_tools" / "src"):
        if (sib / "json_to_volume_ben.py").exists():
            return sib
    return None

_ben_dir = _find_ben_dir()
if _ben_dir is None:
    print("ERROR: could not locate json_to_volume_ben.py (must be in the same folder, or under tracer_tools/src).")
    sys.exit(1)
sys.path.insert(0, str(_ben_dir))

from json_to_volume_ben import _write_volume_packaging, _write_mesh_files  # noqa: E402


def obj_to_volume_ben(datastack_name, obj_path, output_filepath, segid=1):
    """Build a precomputed mesh volume folder from a local OBJ file.

    Args:
      datastack_name: e.g. "brain_and_nerve_cord" — controls voxel scale and info bounds.
      obj_path: absolute path to a mesh OBJ (typically Blender-edited).
      output_filepath: folder where `image/` will be created.
      segid: integer segment id (manifest is `<segid>:0`, fragment `<segid>:0:1`).

    Returns: dict {'volume_path': <image_dir>, 'mesh': trimesh.Trimesh}
    """
    os.makedirs(output_filepath, exist_ok=True)

    mesh = trimesh.load(obj_path, process=False)
    if not isinstance(mesh, trimesh.Trimesh):
        # If obj loads as a Scene, concatenate dump.
        mesh = trimesh.util.concatenate(tuple(mesh.dump()))

    image_dir = _write_volume_packaging(output_filepath, datastack_name)
    _write_mesh_files(image_dir, mesh, segid=segid)
    return {"volume_path": image_dir, "mesh": mesh}


def _cli():
    p = argparse.ArgumentParser(description="OBJ mesh → precomputed volume folder")
    p.add_argument("obj_path")
    p.add_argument("--datastack", default="brain_and_nerve_cord")
    p.add_argument("--out", required=True, help="output folder")
    p.add_argument("--segid", type=int, default=1)
    args = p.parse_args()

    result = obj_to_volume_ben(
        datastack_name=args.datastack,
        obj_path=args.obj_path,
        output_filepath=args.out,
        segid=args.segid,
    )
    m = result["mesh"]
    print(f"vertices: {len(m.vertices)}, faces: {len(m.faces)}")
    print(f"volume: {result['volume_path']}")


if __name__ == "__main__":
    _cli()
