import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.constants import (
    BASE_DIR,
    INPUT_DATA_DIR,
    OUTPUT_DATA_DIR)

import rasterio

from utils.functions import *
from utils.vectorisation_vege_process import *

### Démarrage du script global
print("ℹ️  Début du script")
globaltimer = startTimerLog("global")

### Etape1 : Import du tiff pour traiter les données
print("ℹ️  Début Etape 1 : Import du tiff pour traiter les données")
etape1timer = startTimerLog("etape1")

raster_path = os.path.join(INPUT_DATA_DIR, "vegetation_stratifiee_2018_2154.tiff")
raster = rasterio.open(raster_path)

endTimerLog(etape1timer)
print("✅ Etape 1 terminée")

### Etape 1.1 skipped

### Etape 2 on découpe au territoire
etape2timer = startTimerLog("etape2")

# ================================================
# Specify here the insee code to take if you need
# ================================================
# TODO: Récup le raster et l'array depuis les params de lancement du script
# speArrayWrong = ['51561651'] (for example)
# speArraySATC = ['69292']

# Call big process function
vegeBigProcess(raster)

# End Etape 2
endTimerLog(etape2timer)
print("✅ Etape 2 terminée")

# End timer
endTimerLog(globaltimer)
print("✅ Script galobal terminé")
