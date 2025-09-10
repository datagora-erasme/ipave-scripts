import sys
import os
import traceback

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import geopandas as gpd
import datetime
import time
import argparse

from utils.logger import setup_logger
from utils.functions import wfs2gp_df, format_elapsed_time
from utils.constants import (
    INPUT_DATA_DIR,
    OUTPUT_DATA_DIR,
    RATE_M2_TO_KM2,
    RATE_MK2_TO_HA,
    ROUND_KM2,
)

import warnings

warnings.filterwarnings("ignore")

logger = setup_logger(
    __name__, info_log_file="logs/info.log", error_log_file="logs/error.log"
)


# INFO: launching the script from shell command :
# python ./2_script/generate_2_shp_kpi_vege.py --file "vegetation_stratifiee_clipped_by_voirie_2018_2154.shp" --origin "OUTPUT" --name "ipave_communes-gl_vege-voirie_kpis_2018_2154"


def batch_generate_kpis(input_file: str, origin_input_dir: str, output_name: str):
    """
    Script for generating SHP of MDL cities with vegetalisation's KPIs and save datas on a new Shapefile.

    Use the script on the shell command like this :
    python ./2_script/generate_2_shp_kpi_vege.py --file "vegetation_stratifiee_2018_2154_ALB.gpkg" --origin "OUTPUT" --name "vegetation_stratifiee_kpis_2018_2154"
    python ./2_script/generate_2_shp_kpi_vege.py --file "vegetation_stratifiee_clipped_by_voirie_2018_2154.shp" --origin "OUTPUT" --name "ipave_communes-gl_vege-voirie_kpis_2018_2154"

        Parameters:
            input_file (string) : Name of the file input used to extract datas (present in the directory ./0_geodatas/input/ - or ./0_geodatas/output/)
            origin_input_dir (string) : Select the parent directory : INPUT or OUTPUT (present in the directory ./0_geodatas/)
            output_name (string) : Name of the file output used to generate and save datas (as a Shapefile)

        Returns:
            None (but generate a Shapefile with generates datas resumed on the OUTPUT directory)
    """
    try:
        script_time_start = datetime.datetime.now()
        error = True
        if output_name:
            if output_name[-4:] != ".shp":
                output_name = f"{output_name}.shp"

        mdl_upper_layer_area = 0
        mdl_middle_layer_area = 0
        mdl_lower_layer_area = 0
        mdl_total_layer_area = 0
        logger.info(f"🚀 Let's go !")

        # INFO: check file format & import it (GPKG or SHP)
        # TOOD: how it works with GeoJSON ?
        logger.info(f"   ⚙️    ...checking files if exists...")

        if origin_input_dir == "OUTPUT":
            if os.path.join(OUTPUT_DATA_DIR, input_file):
                logger.info(f"     ✅  {input_file} FILE FOUND !")
                error = False
                gdf_result = gpd.read_file(os.path.join(OUTPUT_DATA_DIR, input_file))
            else:
                logger.info(f"     ❌ {input_file} FILE NOT FOUND !")
        else:
            if os.path.join(INPUT_DATA_DIR, input_file):
                logger.info(f"     ✅  {input_file} FILE FOUND !")
                error = False
                gdf_result = gpd.read_file(os.path.join(INPUT_DATA_DIR, input_file))
            else:
                logger.info(f"     ❌ {input_file} FILE NOT FOUND !")

        if error:
            logger.info(f" 💥  An error has occured  💥 ")
        else:
            # INFO: STEP 0 - Init empty GDF
            logger.info(f"   ⚙️    ...init GeoDataFrame for results...")
            columns_list = [
                "gid",
                "nom",
                "insee",
                "trigramme",
                "v_veg_h_ha",
                "v_veg_m_ha",
                "v_veg_b_ha",
                "v_veg_t_ha",
                "sup_ha",
                "geometry",
            ]
            gdf_voirie_vg_kpis = gpd.GeoDataFrame(
                columns=columns_list, geometry="geometry", crs="EPSG:2154"
            )
            logger.info(
                f"   ✅    ...Sucessfully ended with CRS={gdf_voirie_vg_kpis.crs} !"
            )

            # INFO: STEP 1 - Open cities open-data
            logger.info(f"   ⚙️    ...import Cities datas from open-datas WFS...")
            time_start = datetime.datetime.now()
            gdf_cities = wfs2gp_df(
                "metropole-de-lyon:adr_voie_lieu.adrcommunes_2024",
                "https://data.grandlyon.com/geoserver/metropole-de-lyon/ows?SERVICE=WFS",
                reprojMetro=True,
                targetProj="EPSG:2154",
            )
            time_end = datetime.datetime.now()
            time_elapsed = format_elapsed_time(time_start, time_end)
            logger.info(f"   ⚙️  ...CRS got : {gdf_cities.crs}")
            logger.info(f"   ✅  ...Sucessfully ended in {time_elapsed} !")

            # INFO: STEP 2 - Clip  gdf_result by Cities and foreach cities for KPIs calculations on Cities GDF
            logger.info(f"   ⚙️    ...cliping results by Cities and calcul KPIs...")
            logger.info(f"   ⚠️    ...warning : long time process...")
            time_start = time.time()

            for index, city in gdf_cities.iterrows():
                area_city = city["geometry"].area / RATE_M2_TO_KM2 * RATE_MK2_TO_HA
                area_unknown = 0
                upper_layer_area = 0
                middle_layer_area = 0
                lower_layer_area = 0
                total_layer_area = 0

                if city["communegl"] and city["trigramme"] != "LYO":
                    gdf_city_clipped = gpd.clip(
                        gdf_result, city["geometry"], keep_geom_type=False, sort=False
                    )

                    logger.info(f"       ⚙️  > calculate areas by layer...")
                    for polygon in gdf_city_clipped.itertuples():
                        polygon_strate_type = polygon.strate.lower()
                        polygon_area = polygon.geometry.area
                        if polygon_strate_type == "arborescent":
                            upper_layer_area += polygon_area
                            total_layer_area += polygon_area
                        elif polygon_strate_type == "arbustif":
                            middle_layer_area += polygon_area
                            total_layer_area += polygon_area
                        elif polygon_strate_type == "herbacee":
                            lower_layer_area += polygon_area
                            total_layer_area += polygon_area
                        else:
                            logger.info(
                                f"       ❌  Strate type doesn't match : '{polygon_strate_type}'"
                            )
                            area_unknown += polygon_area

                    upper_layer_area = (
                        upper_layer_area / RATE_M2_TO_KM2 * RATE_MK2_TO_HA
                    )
                    middle_layer_area = (
                        middle_layer_area / RATE_M2_TO_KM2 * RATE_MK2_TO_HA
                    )
                    lower_layer_area = (
                        lower_layer_area / RATE_M2_TO_KM2 * RATE_MK2_TO_HA
                    )
                    total_layer_area = (
                        total_layer_area / RATE_M2_TO_KM2 * RATE_MK2_TO_HA
                    )
                    last_index = len(gdf_voirie_vg_kpis)
                    gdf_voirie_vg_kpis.loc[last_index] = {
                        "gid": city["gid"],
                        "nom": city["nom"],
                        "insee": city["insee"],
                        "trigramme": city["trigramme"],
                        "sup_ha": area_city,
                        "v_veg_h_ha": upper_layer_area,
                        "v_veg_m_ha": middle_layer_area,
                        "v_veg_b_ha": lower_layer_area,
                        "v_veg_t_ha": total_layer_area,
                        "geometry": city["geometry"],
                    }

                    logger.info(f"       ✅    SUCCESS CALCULATED FOR {city['nom']}")

                    mdl_upper_layer_area += upper_layer_area
                    mdl_middle_layer_area += middle_layer_area
                    mdl_lower_layer_area += lower_layer_area
                    mdl_total_layer_area += total_layer_area

            # INFO: STEP 3 - Export result (Cities GDF) on SHP
            gdf_voirie_vg_kpis = gdf_voirie_vg_kpis.set_geometry("geometry")
            gdf_voirie_vg_kpis = gdf_voirie_vg_kpis.set_crs(
                "EPSG:2154", allow_override=True
            )
            time_end = time.time()
            logger.info(
                f"     ✅  ...Sucessfully ended in {time_end - time_start:.4f}s !"
            )

            logger.info(
                f"   ⚙️    ...export datas on Shapefile (CRS={gdf_voirie_vg_kpis.crs})..."
            )
            time_start = time.time()
            gdf_voirie_vg_kpis.to_file(os.path.join(OUTPUT_DATA_DIR, output_name))
            time_end = time.time()
            logger.info(
                f"     ✅  ...Sucessfully ended in {time_end - time_start:.4f}s !"
            )
            logger.info(
                f"ℹ️  FILE SAVED AT {os.path.join(OUTPUT_DATA_DIR, output_name)} !"
            )

            # INFO: STEP 4 - Return KPIs on global (MDL)
            logger.info(f"🗺️  Results on Metropole de Lyon :")
            logger.info(
                f"    🌳 Surface strate 'Haute' ~= {round(mdl_upper_layer_area, ROUND_KM2)} ha"
            )
            logger.info(
                f"    🌾 Surface strate 'Moyenne' ~= {round(mdl_middle_layer_area, ROUND_KM2)} ha"
            )
            logger.info(
                f"    🌿 Surface strate 'Basse' ~= {round(mdl_lower_layer_area, ROUND_KM2)} ha"
            )
            logger.info(
                f"    🧩 Surface totale végétalisée ~= {round(mdl_total_layer_area, ROUND_KM2)} ha"
            )

        script_time_end = datetime.datetime.now()
        time_elapsed = format_elapsed_time(script_time_start, script_time_end)
        logger.info(f" 🌳 🌾 🌿 END OF SCRIPT IN {time_elapsed} 🌳 🌾 🌿")
        logger.info("")
    except Exception as error:
        logger.info(f"🚨 🚨 🚨 🚨  An error as occured !  🚨 🚨 🚨 🚨")
        logger.info("")
        logger.error(f"   > {str(error)} > {traceback.print_exc()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="🧩 Script Generate KPIs -Végé-")
    parser.add_argument(
        "--file",
        nargs=1,
        required=True,
        help="Path of the input file (.gpkg, .shp)",
    )
    parser.add_argument(
        "--origin",
        nargs=1,
        required=True,
        help="Parent folder of directory select (INPUT or OUTPUT)",
    )
    parser.add_argument(
        "--name",
        nargs=1,
        required=True,
        help="name of the final Shapefile",
    )

    args = parser.parse_args()
    bash_input_file = args.file[0]
    bash_origin_input_dir = args.origin[0]
    bash_output_name = args.name[0]

    if bash_origin_input_dir not in ("INPUT", "OUTPUT"):
        bash_origin_input_dir = "INPUT"

    batch_generate_kpis(bash_input_file, bash_origin_input_dir, bash_output_name)
