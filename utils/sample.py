# import all utils methods
from utils.constants import *
from utils.functions import *

# code to execute
try:
    # sample connection DB
    print(" > Test connection DB & fetch")
    conn, cur = connectDB()

    # test from table public.edhec_asset
    query = "SELECT * FROM public.edhec_asset ORDER BY id ASC LIMIT 10;"
    print(query)
    print("")

    cur.execute(query)
    rows = cur.fetchall()
    for row in rows:
        # Do algorithms with datas
        print(f"   {row}")

    closeDB(conn, cur)
    print("")

    # sample Nominatim call
    print(" > Test call API Nominatim to get coordinates")
    str_address = "25 rue aristide briand 69800 saint-priest"
    # get coords from Nominatim (EPSG:4326) and reproj to (EPSG:3857)
    latitude, longitude = getCoordinatesFromStrAddress(str_address)
    print(f"{str_address} >> lat={str(latitude)}, lon={str(longitude)}")

except Exception as error:
    print(error)
