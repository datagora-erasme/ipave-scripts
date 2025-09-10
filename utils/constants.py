import os

# INFO: WARNING -> Jupyter doesn't handle __file__ !
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DATA_DIR = os.path.join(BASE_DIR, "0_geodatas", "input")
OUTPUT_DATA_DIR = os.path.join(BASE_DIR, "0_geodatas", "output")

RATE_M2_TO_KM2 = 1000000
RATE_MK2_TO_HA = 100
ROUND_KM2 = 3
