import sys
from pathlib import Path


def setup_project():
    # Détection du dossier racine du projet (on cherche le dossier exo-map-scripts)
    current = Path.cwd()
    for parent in [current] + list(current.parents):
        if (parent / "utils").exists() and (parent / "0_geodatas").exists():
            root = parent
            break
    else:
        raise RuntimeError("Impossible de trouver le dossier racine du projet")

    # Ajoute la racine au PYTHONPATH pour pouvoir importer utils/config etc.
    sys.path.append(str(root))
    print(f"[INIT] Racine du projet ajoutée au path : {root}")