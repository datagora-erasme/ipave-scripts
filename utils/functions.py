import logging, os
import psycopg2
import csv
from math import *
from datetime import datetime
from psycopg2.extras import RealDictCursor
import geopandas as gp
from pyproj import Transformer
import requests
from fiona import BytesCollection

from shapely.geometry import (
    Polygon,
    MultiPolygon,
)
from utils.constants import *

if os.getenv("BDD_DB_SYSTEM"):
    BDD_DB_SYSTEM = os.getenv("BDD_DB_SYSTEM").strip()
if os.getenv("BDD_CONFIG_HOST"):
    BDD_CONFIG_HOST = os.getenv("BDD_CONFIG_HOST").strip()
if os.getenv("BDD_CONFIG_USER"):
    BDD_CONFIG_USER = os.getenv("BDD_CONFIG_USER").strip()
if os.getenv("BDD_CONFIG_PASSWD"):
    BDD_CONFIG_PASSWD = os.getenv("BDD_CONFIG_PASSWD").strip()
if os.getenv("BDD_CONFIG_DB"):
    BDD_CONFIG_DB = os.getenv("BDD_CONFIG_DB").strip()
if os.getenv("BDD_CONFIG_SCHEMA"):
    BDD_CONFIG_SCHEMA = os.getenv("BDD_CONFIG_SCHEMA").strip()
if os.getenv("BDD_CONFIG_PORT"):
    BDD_CONFIG_PORT = os.getenv("BDD_CONFIG_PORT").strip()

# ---------------------
# ---- COLOR STYLE ----
# ---------------------


class style:
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    UNDERLINE = "\033[4m"
    RESET = "\033[0m"


def debugLog(color, message, level=logging.INFO, onlyFile=False):
    currLogger = logging.getLogger("main")
    # Log in file
    if level == logging.INFO:
        currLogger.info(message)
    elif level == logging.WARN:
        currLogger.warning(message)
    elif level == logging.ERROR:
        currLogger.error(message)
    elif level == logging.CRITICAL:
        currLogger.critical(message)
    else:
        currLogger.info(message)

    if not onlyFile:
        # Print in console
        print(color + message + "\n", style.RESET)


def list_extensions(dir: str):
    extensions = set()  # prevent duplicates values
    for file in os.listdir(dir):
        path_file = os.path.join(dir, file)
        if os.path.isfile(path_file):
            _, ext = os.path.splitext(file)
            if ext:  # prevent files without extension
                extensions.add(ext.lower())
    return sorted(extensions)


def startTimerLog(taskname):
    # Log time
    start_date = datetime.now()
    debugLog(
        style.MAGENTA,
        "Start task '{}' at {}".format(
            taskname, start_date.strftime("%d/%m/%Y, %H:%M:%S")
        ),
        logging.INFO,
    )

    # Create timer dict obj
    timer = {"taskname": taskname, "start_date": start_date}

    return timer


def endTimerLog(timer):
    # Log time end
    end_date = datetime.now()
    time_elapsed = datetime.now() - timer["start_date"]

    # Split timedelta
    time_el_d = time_elapsed.days
    time_el_h = floor(time_elapsed.seconds / 3600)
    time_el_m = floor(time_elapsed.seconds / 60)
    time_el_s = time_elapsed.seconds - (time_el_m * 60)
    time_el_ms = time_elapsed.microseconds

    # Log
    debugLog(
        style.MAGENTA,
        "End task '{}' at {} in {} days {} hours {} min {} sec {} micros".format(
            timer["taskname"],
            end_date.strftime("%d/%m/%Y, %H:%M:%S"),
            time_el_d,
            time_el_h,
            time_el_m,
            time_el_s,
            time_el_ms,
        ),
        logging.INFO,
    )


def connectDB(jsonEnable=False, setSearchpath=True):
    try:
        conn = psycopg2.connect(
            dbname=BDD_CONFIG_DB,
            user=BDD_CONFIG_USER,
            password=BDD_CONFIG_PASSWD,
            host=BDD_CONFIG_HOST,
            port=BDD_CONFIG_PORT,
            options=f"-c search_path={BDD_CONFIG_SCHEMA}",
        )
        cur = None
        if jsonEnable:
            cur = conn.cursor(cursor_factory=RealDictCursor)
        else:
            cur = conn.cursor()

        # # Set Schema 'base'
        # if setSearchpath:
        #     cur.execute(f'SET search_path TO '+ params_DB['schema'])

        return conn, cur

    except (Exception, psycopg2.Error) as error:
        debugLog(
            style.RED,
            "Error while trying to connect in PostgreSQL database : {}".format(error),
            logging.ERROR,
        )


def closeDB(conn, cur):
    try:
        # Commit (save change)
        conn.commit()

        # Close DB connection
        cur.close()

    except (Exception, psycopg2.Error) as error:
        debugLog(
            style.RED,
            "Error while trying to connect in PostgreSQL database : {}".format(error),
            logging.ERROR,
        )


def getGDFfromDB(DB_params, sqlQuery, projection):
    # Connect DB
    conn, cur = connectDB(DB_params)

    # Get data (schema in sqlQuery)
    df = gp.read_postgis(sqlQuery, conn, crs=projection)

    # Get length
    lenDF = len(df)

    # Log
    debugLog(
        style.GREEN,
        "Datas was loaded successfully (with {} entites) \n".format(lenDF),
        logging.INFO,
    )

    # Final close cursor & DB
    closeDB(conn, cur)

    return df


def insertGDFintoDB(
    DB_params, DB_schema, gdf, tablename, columnsListToDB, batch_size=10000
):
    # Connect DB
    conn, cur = connectDB(DB_params)

    # Nettoyer les valeurs
    def clean_value(v):
        if isinstance(v, str):
            v = v.replace("\n", " ").replace("\r", " ").replace(";", ",")
            return str(v)
        return str(v)

    gdf = gdf.applymap(clean_value)
    gdf = gdf.fillna("")

    # Préparer les données
    data = [tuple(row) for row in gdf[columnsListToDB].to_numpy()]

    # SQL
    columns = ", ".join(columnsListToDB)
    insert_query = f"INSERT INTO {DB_schema}.{tablename} ({columns}) VALUES %s"

    try:
        psycopg2.extras.execute_values(
            cur,
            insert_query,
            data,
            template=None,  # Laisse psycopg2 gérer proprement
            page_size=batch_size,
        )
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        debugLog(style.RED, "Error while inserting : {}".format(error), logging.ERROR)
        conn.rollback()
        cur.close()
        return 1

    closeDB(conn, cur)


def getCoordinatesFromStrAddress(
    str_address,
    proj_origin="EPSG:4326",
    proj_target="EPSG:3857",
    need_invert_coords=True,
):
    transformer = Transformer.from_crs(proj_origin, proj_target, always_xy=True)
    lat = None
    lon = None
    uri = "https://nominatim.openstreetmap.org/search"
    headers = {
        "User-Agent": "Exo-Map - contact@exo-dev.fr",
        "Content-Type": "application/json; charset=utf-8",
        "Connection": "keep-alive",
        "Keep-Alive": "timeout=20",
    }

    if len(str_address) == 0:
        return lat, lon

    params = {
        "q": str_address,
        "format": "jsonv2",
        "addressdetails": "1",
        "limit": "5",
    }

    try:
        response = requests.get(uri, params=params, headers=headers)

        if response.status_code == 200:
            json_data = response.json()
            if len(json_data) > 0:
                x, y = json_data[0]["lat"], json_data[0]["lon"]
                if proj_origin != proj_target:
                    if need_invert_coords:
                        lat, lon = transformer.transform(y, x)
                    else:
                        lat, lon = transformer.transform(x, y)
                else:
                    lat = x
                    lon = y
            return lat, lon
        else:
            debugLog(
                style.YELLOW,
                "No match for this address : {}".format(response.text),
                logging.ERROR,
            )
            return lat, lon

    except (Exception, psycopg2.Error) as error:
        debugLog(
            style.RED,
            "Error while trying to connect in PostgreSQL database : {}".format(error),
            logging.ERROR,
        )


def wfs2gp_df(
    layer_name,
    url,
    bbox=None,
    wfs_version="2.0.0",
    outputFormat="application/gml+xml; version=3.2",
    reprojMetro=False,
    targetProj=None,
    req_timeout=600,
    proxies=None,
):
    # Concat params
    params = dict(
        service="WFS",
        version=wfs_version,
        request="GetFeature",
        typeName=layer_name,
        outputFormat=outputFormat,
        crs=targetProj,
    )
    # Load data in Bytes
    with BytesCollection(
        requests.get(url, params=params, timeout=req_timeout, proxies=proxies).content
    ) as f:
        # Make GDF
        df = gp.GeoDataFrame.from_features(f)

    # Log
    # lenDF = len(df)
    # debugLog(style.GREEN, "API datas loaded successfully (with {} entites) \n".format(lenDF), logging.INFO)

    # Reproj
    if reprojMetro:
        df = df.set_crs("EPSG:4326")
    if targetProj:
        df = checkAndReproj(df, targetProj)

    return df


def checkAndReproj(df, targetProj):
    # Get actual DF proj
    currentProj = df.crs
    debugLog(
        style.YELLOW,
        "Current projection of dataframe : {}".format(currentProj),
        logging.INFO,
    )

    if currentProj != targetProj:
        # Reproj to targeted proj
        df = df.to_crs(targetProj)

        # Log
        newProj = df.crs
        debugLog(style.GREEN, "Successful reproj to : {}".format(newProj), logging.INFO)
    else:
        debugLog(style.GREEN, "No need to reproj this dataframe", logging.INFO)

    return df


def createGDFfromSpatialFile(filePath):
    try:
        # Read (GeoJSON or Shape file)
        currentGDF = gp.read_file(filePath)
        # Count
        lenDF = len(currentGDF)
        # Log
        debugLog(
            style.GREEN,
            "GeoJSON or Shape file '{}' loaded successfully (with {} entites)".format(
                filePath, lenDF
            ),
            logging.INFO,
        )
        # Return
        return currentGDF
    except Exception as error:
        debugLog(
            style.RED,
            "Error while trying to open file : {}".format(error),
            logging.INFO,
        )
        return None


def format_elapsed_time(start_time: datetime, end_time: datetime) -> str:
    elapsed = end_time - start_time
    total_seconds = round(elapsed.total_seconds(), 1)

    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0 or (hours > 0 and seconds > 0):
        parts.append(f"{minutes}min")
    if seconds > 0 or (hours == 0 and minutes == 0):
        if total_seconds < 60:
            parts.append(f"{total_seconds}s")
        else:
            parts.append(f"{round(seconds, 1)}s")

    return " ".join(parts)


def remove_small_holes(geom, area_thresh=2.0):
    """
    Supprime les trous (interiors) de surface inférieure à `area_thresh`
    """
    if geom.geom_type == "Polygon":
        exterior = geom.exterior
        new_interiors = []

        for ring in geom.interiors:
            hole = Polygon(ring)
            if hole.area >= area_thresh:
                new_interiors.append(ring)

        return Polygon(exterior, new_interiors)

    elif geom.geom_type == "MultiPolygon":
        return MultiPolygon(
            [remove_small_holes(poly, area_thresh) for poly in geom.geoms]
        )

    else:
        return geom  # ne pas traiter autres géométries


# Fonction robuste de lissage basée sur le nombre de sommets
def simplifier_geom(geom, seuil=150, tol_base=0.4, tol_min=0.1, tol_max=1.0):
    # """
    # seuil = 100         # on commence à simplifier dès 100 sommets
    # tol_base = 1.2      # à 100 sommets → tolérance = 1.2m
    # tol_max = 3.5       # on autorise jusqu’à 3.5m de simplification
    # tol_min = 0.2       # on reste doux sur les petites géos
    # """
    if geom.is_empty or geom.geom_type not in ["Polygon", "MultiPolygon"]:
        return geom

    try:
        if geom.geom_type == "Polygon":
            nb_pts = len(geom.exterior.coords)
        elif geom.geom_type == "MultiPolygon":
            nb_pts = max(len(part.exterior.coords) for part in geom.geoms)

        # Tolérance proportionnelle
        facteur = nb_pts / seuil
        tol = tol_base * facteur

        # Encadrer
        tol = max(min(tol, tol_max), tol_min)

        simple = geom.simplify(tol, preserve_topology=True)

        if simple.is_valid and not simple.is_empty:
            return simple
    except:
        pass

    return geom  # fallback
