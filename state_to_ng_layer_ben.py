"""state_to_ng_layer_ben — one-shot: state.json + annotation layer → public NG layer.

Default flow (no Blender):
  state.json + layer name + method → mesh → packaged volume → uploaded → public URL.

With Blender (--blender):
  Same as above but pauses after writing the obj, waits for the user to edit
  in Blender, then continues with the edited obj.

Example:
  python state_to_ng_layer_ben.py state.json --layer annotation3 --name region_v2
  python state_to_ng_layer_ben.py state.json --layer annotation3 --name region_v2 --method alpha
  python state_to_ng_layer_ben.py state.json --layer annotation3 --name region_v2 --blender
"""
import os
import sys
import shutil
import argparse
import tempfile
from pathlib import Path


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
    print("ERROR: could not locate the *_ben.py modules. Place this script next to "
          "json_to_volume_ben.py / obj_to_volume_ben.py / bucket_upload_folder_ben.py.")
    sys.exit(1)
sys.path.insert(0, str(_ben_dir))

from json_to_volume_ben import json_to_volume_ben  # noqa: E402
from obj_to_volume_ben import obj_to_volume_ben  # noqa: E402
from bucket_upload_folder_ben import bucket_upload_folder_ben, ng_layer_source_for  # noqa: E402


def state_to_ng_layer_ben(
    json_filepath,
    layer_name,
    name,
    datastack_name="brain_and_nerve_cord",
    method="convex",
    alpha=None,
    bucket_root="nokura://tracers/ben",
    workdir=None,
    blender_pause=False,
    edited_obj=None,
    segid=1,
):
    """End-to-end: NG state.json annotation layer → public NG layer source URL.

    Args:
      json_filepath: absolute path to a NG state.json.
      layer_name: name of the annotation layer to mesh.
      name: short name for the new mesh (used as the bucket folder name).
      datastack_name: datastack the state is from (default BANC).
      method: "convex" or "alpha".
      alpha: alpha-shape radius in nm; None=auto.
      bucket_root: bucket prefix, default 'nokura://tracers/ben'.
      workdir: scratch folder for intermediates; default a temp dir.
      blender_pause: if True, write the obj and stop. Call again with edited_obj=<path> to continue.
      edited_obj: if provided, use this obj instead of regenerating from the JSON
                  (use when continuing after Blender editing).
      segid: integer segment id for the mesh.

    Returns dict with keys:
      'obj_path', 'volume_path', 'bucket_path', 'https_url', 'ng_source', 'alpha_used'.
    """
    if workdir is None:
        workdir = tempfile.mkdtemp(prefix=f"tracer_mesh_{name}_")
    os.makedirs(workdir, exist_ok=True)

    if edited_obj is not None:
        # User edited in Blender; package the edited obj directly.
        out = obj_to_volume_ben(
            datastack_name=datastack_name,
            obj_path=edited_obj,
            output_filepath=workdir,
            segid=segid,
        )
        obj_path = edited_obj
        volume_path = out["volume_path"]
        alpha_used = None
    elif blender_pause:
        # Generate obj only, then return — user runs Blender, then re-calls with edited_obj.
        gen = json_to_volume_ben(
            datastack_name=datastack_name,
            json_filepath=json_filepath,
            layer_name=layer_name,
            output_filepath=workdir,
            method=method,
            alpha=alpha,
            export_obj=True,
            segid=segid,
            build_volume=False,
        )
        return {
            "obj_path": gen["obj_path"],
            "volume_path": None,
            "bucket_path": None,
            "https_url": None,
            "ng_source": None,
            "alpha_used": gen["alpha_used"],
            "blender_pause": True,
            "workdir": workdir,
        }
    else:
        # Full automatic: generate hull/alpha, package, upload.
        gen = json_to_volume_ben(
            datastack_name=datastack_name,
            json_filepath=json_filepath,
            layer_name=layer_name,
            output_filepath=workdir,
            method=method,
            alpha=alpha,
            export_obj=True,
            segid=segid,
            build_volume=True,
        )
        obj_path = gen["obj_path"]
        volume_path = gen["volume_path"]
        alpha_used = gen["alpha_used"]

    bucket_path = f"{bucket_root.rstrip('/')}/{name}"
    upload = bucket_upload_folder_ben(local_path=volume_path, bucket_path=bucket_path, public_read=True)
    ng_source = ng_layer_source_for(upload["https_url"]) if upload["https_url"] else None

    return {
        "obj_path": obj_path,
        "volume_path": volume_path,
        "bucket_path": bucket_path,
        "https_url": upload["https_url"],
        "ng_source": ng_source,
        "alpha_used": alpha_used,
    }


def _cli():
    p = argparse.ArgumentParser(description="state.json annotation layer → public NG layer source URL")
    p.add_argument("json_filepath")
    p.add_argument("--layer", required=True, help="name of the annotation layer to mesh")
    p.add_argument("--name", required=True, help="short name for the new mesh (used as bucket folder)")
    p.add_argument("--datastack", default="brain_and_nerve_cord")
    p.add_argument("--method", choices=["convex", "alpha"], default="convex")
    p.add_argument("--alpha", type=float, default=None, help="alpha-shape radius (nm); default auto")
    p.add_argument("--bucket-root", default="nokura://tracers/ben")
    p.add_argument("--workdir", default=None, help="scratch folder for intermediates")
    p.add_argument("--blender", action="store_true", help="pause after writing obj for Blender editing")
    p.add_argument("--edited-obj", default=None, help="continue from a Blender-edited obj")
    p.add_argument("--segid", type=int, default=1)
    p.add_argument("--tracer-path", default=None, help="manual path to tracer_tools/src (only if auto-detect failed)")
    args = p.parse_args()

    result = state_to_ng_layer_ben(
        json_filepath=args.json_filepath,
        layer_name=args.layer,
        name=args.name,
        datastack_name=args.datastack,
        method=args.method,
        alpha=args.alpha,
        bucket_root=args.bucket_root,
        workdir=args.workdir,
        blender_pause=args.blender,
        edited_obj=args.edited_obj,
        segid=args.segid,
    )

    if result.get("blender_pause"):
        print(f"\nobj written for Blender editing:\n  {result['obj_path']}")
        if result["alpha_used"] is not None:
            print(f"  (alpha used: {result['alpha_used']:.1f} nm)")
        print(f"\nedit it, then re-run with:")
        print(f'  --edited-obj "<edited_obj_path>" --workdir "{result["workdir"]}"')
        return

    print(f"\n=== done ===")
    if result["alpha_used"] is not None:
        print(f"alpha used: {result['alpha_used']:.1f} nm")
    print(f"obj:    {result['obj_path']}")
    print(f"volume: {result['volume_path']}")
    print(f"bucket: {result['bucket_path']}")
    print(f"https:  {result['https_url']}")
    print(f"\nNG layer source (paste into a new layer's source field):")
    print(f"  {result['ng_source']}")
    print(f"state.json subsources: {{\"bounds\": true, \"mesh\": true}}, enableDefaultSubsources: false")


if __name__ == "__main__":
    _cli()
