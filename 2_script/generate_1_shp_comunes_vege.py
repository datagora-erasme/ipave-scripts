import sys
import os
import traceback
import datetime
import argparse
from pathlib import Path
import geopandas as gpd
import pandas as pd
from shapely import make_valid

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from utils.logger import setup_logger
from utils.functions import format_elapsed_time
from utils.constants import (
    INPUT_DATA_DIR,
    OUTPUT_DATA_DIR,
)

import warnings

warnings.filterwarnings("ignore")

logger = setup_logger(
    __name__, info_log_file="logs/info.log", error_log_file="logs/error.log"
)

# INFO: launching the script from shell command :
# python ./2_script/generate_1_shp_comunes_vege_clipped.py --dir "vegetation_stratifiee_2018_2154" --origin "OUTPUT" --mask "surfacique_voirie.shp" --name "vegetation_stratifiee_clipped_by_voirie_2018_2154.shp" --extension "*.shp"


def batch_clip_concat(
    input_dir: str,
    origin_input_dir: str,
    clip_file: str,
    output_name: str,
    pattern: str = "*.shp",
    recursive: bool = False,
    add_source_col: bool = True,
    use_pyogrio: bool = True,
):
    """
    Script for generating SHP of MDL cities with vegetalisation's KPIs and save datas on a new Shapefile.

    Use the script on the shell command like this :
    python ./2_script/generate_1_shp_comunes_vege_clipped.py --dir "vegetation_stratifiee_2018_2154" --origin "OUTPUT" --mask "surfacique_voirie.shp" --name "vegetation_stratifiee_clipped_by_voirie_2018_2154.shp" --extension "*.shp"

        Parameters:
            input_dir (string) : Name of the directory where the data files are
            origin_input_dir (string) : Select the parent directory : INPUT or OUTPUT (present in the directory ./0_geodatas/)
            clip_file (string) : Name of the file (.shp or .gpkg) input used to define clipping area (present in the directory ./0_geodatas/input/)
            output_name (string) : Name of the file output used to generate and save datas (.shp ou .gpkg)
            pattern (string) : Pattern used to select files (from extension) to import data files (ex: *.shp)
            recursive (bool) : If we're using recursive select option
            add_source_col (bool) : If we're adding a column from data sources
            use_pyogrio (bool) : If we're using "pyogrio" library for better perf

        Returns:
            None (but generate a Shapefile with generates datas resumed on the OUTPUT directory)
    """

    """
    """
    script_time_start = datetime.datetime.now()
    logger.info(f"🚀  Let's go !")
    engine = "pyogrio" if use_pyogrio else None
    logger.info(f"⚙️  Engine used : {engine if engine else '-default-'}")

    MASK_FILE_PATH = os.path.join(INPUT_DATA_DIR, clip_file)
    if origin_input_dir == "OUTPUT":
        DIR_VEGETATION_FILES_PATH = os.path.join(OUTPUT_DATA_DIR, input_dir)
    else:
        DIR_VEGETATION_FILES_PATH = os.path.join(INPUT_DATA_DIR, input_dir)

    if output_name.endswith(".shp") or output_name.endswith(".gpkg"):
        FILE_OUTPUT_PATH = os.path.join(OUTPUT_DATA_DIR, output_name)
    else:
        FILE_OUTPUT_PATH = os.path.join(OUTPUT_DATA_DIR, f"{output_name}.shp")

    logger.info(f"   ▶️  File used to clip : {MASK_FILE_PATH}")
    logger.info(
        f"   ▶️  Directory used for data files input : {DIR_VEGETATION_FILES_PATH}"
    )
    logger.info(f"   ▶️  File to generate at the end : {FILE_OUTPUT_PATH}")

    # INFO: STEP 1 - Read & import mask file
    clip_gdf = gpd.read_file(MASK_FILE_PATH, engine=engine)
    if clip_gdf.empty:
        logger.info(f"     ❌ MASK FILE NOT FOUND !")
        logger.info("")
        raise ValueError("Clip file is empty")
    logger.info(f"     ✅  MASK FILE FOUND !")

    # INFO: STEP 2 - Get & Read data files to clip
    files = list(
        Path(DIR_VEGETATION_FILES_PATH).rglob(pattern)
        if recursive
        else Path(DIR_VEGETATION_FILES_PATH).glob(pattern)
    )
    if not files:
        logger.info(f"     ❌ INPUT DATA FILES NOT FOUND !")
        raise FileNotFoundError("No data file is found")

    nb_files = len(files)
    logger.info(f"     ✅  INPUT DATA FILES FOUND on {DIR_VEGETATION_FILES_PATH} !")
    logger.info(f"     ✅  FILES FOUNDED : {str(nb_files)}")

    parts = []
    count = 0
    logger.info(f"   ⚙️   FETCH ALL INPUT DATA FILES & CLIP THEM !")
    # INFO: STEP 3 - Clip all data files imported
    for shp in files:
        try:
            time_start = datetime.datetime.now()
            logger.info(f"      ⚙️   CLIPPING INPUT DATA FILE {shp.name}...")
            gdf = gpd.read_file(shp, engine=engine)
            if gdf.empty or gdf.geometry.isna().all():
                continue

            if gdf.crs != clip_gdf.crs:
                gdf = gdf.to_crs(clip_gdf.crs)

            # Validate & Clean GeoDatas if needed
            gdf["geometry"] = gdf.geometry.apply(make_valid)
            gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()]

            if gdf.empty:
                continue

            clipped = gpd.clip(gdf, clip_gdf)

            if clipped.empty:
                continue

            if add_source_col:
                clipped["__source__"] = shp.name

            count += 1
            parts.append(clipped)
            time_end = datetime.datetime.now()
            time_elapsed = format_elapsed_time(time_start, time_end)
            logger.info(
                f"      ✅  CLIP {str(count)}/{str(nb_files)} DONE FOR {shp.name} in {time_elapsed} !"
            )
        except Exception as e:
            logger.info(f"      ❌  FAILED TO LOAD {shp.name} : {e}")
            logger.info("")
            logger.error(traceback.format_exc())

    if not parts:
        logger.info(f"   ❌  NO DATAS AFTER RUNNING IMPORTS & CLIPS")
        logger.info("")
        raise ValueError("No results after clipping (no datas)")

    # INFO: STEP 4 - Concat results
    logger.info(f"   ⚙️  CONCAT ALL RESULT DATAS...")
    result = pd.concat(parts, ignore_index=True)
    result = gpd.GeoDataFrame(result, geometry="geometry", crs=clip_gdf.crs)
    logger.info(f"   ✅  GEO DATA FRAME CREATED !")

    # INFO: STEP 5 - Save & export clipped datas
    logger.info(f"   ⚙️  GENERATE NEW FILE...")
    out = Path(FILE_OUTPUT_PATH)
    out.parent.mkdir(parents=True, exist_ok=True)

    if out.suffix.lower() == ".shp":
        result.to_file(out, driver="ESRI Shapefile", encoding="utf-8")
        logger.info(f"   ✅  FILE CREATED : {out} !")
    else:
        result.to_file(out, driver="GPKG")  # encoding géré nativement
        print(f"   ✅  FILE CREATED : {out} !")

    script_time_end = datetime.datetime.now()
    time_elapsed = format_elapsed_time(script_time_start, script_time_end)
    logger.info(f" 🌳 🌾 🌿 END OF SCRIPT IN {time_elapsed} 🌳 🌾 🌿")
    logger.info("")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="🧩  Script generate - Clip Végétalisation-Voirie / Communes -"
    )
    parser.add_argument(
        "--dir",
        nargs=1,
        required=True,
        help="Directory to select data files to clip",
    )
    parser.add_argument(
        "--origin",
        nargs=1,
        required=True,
        help="Parent folder of directory select (INPUT or OUTPUT)",
    )
    parser.add_argument(
        "--mask",
        nargs=1,
        required=True,
        help="Path of the mask to apply the clip",
    )
    parser.add_argument(
        "--name",
        nargs=1,
        required=True,
        help="Name of the final file generated (*.gpkg, *.shp)",
    )
    parser.add_argument(
        "--extension",
        nargs=1,
        required=True,
        help="Pattern of extensions data files selected (*.shp)",
    )

    args = parser.parse_args()
    bash_input_dir = args.dir[0]
    bash_origin_input_dir = args.origin[0]
    bash_clip_file = args.mask[0]
    bash_output_name = args.name[0]
    bash_pattern = args.extension[0]

    if bash_origin_input_dir not in ("INPUT", "OUTPUT"):
        bash_origin_input_dir = "INPUT"
    if bash_output_name.split(".")[-1] not in ("shp", "gpkg"):
        bash_output_name = bash_output_name.replace(".", "") + ".shp"

    result = batch_clip_concat(
        input_dir=bash_input_dir,
        origin_input_dir=bash_origin_input_dir,
        clip_file=bash_clip_file,
        output_name=bash_output_name,
        pattern=bash_pattern,
        recursive=False,  # True if wanted to import sub directories
        add_source_col=False,  # True if Add column "__source__"
        use_pyogrio=True,  # True if pyogrio available to Boost
    )
    print(f"OK : {len(result)} entities on the output data file.")
