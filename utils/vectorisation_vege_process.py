from shapely.geometry import (
    mapping,
    shape,
)
import numpy as np
import pandas as pd
import geopandas as gpd
# from rasterio.mask import mask
from rasterio.plot import show
from rasterio.features import shapes
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

from utils.functions import *

from utils.constants import (
    BASE_DIR,
    INPUT_DATA_DIR,
    OUTPUT_DATA_DIR)


"""
Nom : vegeBigProcess (à changer à l'avenir...)
Description : Fonction générique de vectorisation d'un raster en découpage par commune
Paramètres :
    raster* : Variable de raster d'entrée ouvert avec rasterio (obligatoire)
        Exemple : raster = rasterio.open(raster_path)
    specificComList : Liste des communes à traiter (facultatif : si par renseigné, traitement de toutes les communes par défaut)
        Exemple : specificComList = ['69072', '69286']
"""
def vegeBigProcess(raster, specificComList=None):
    
    # DEBUG
    # print(raster)
    # print(specificComList)

    ### Etape 2 on découpe au territoire
    print("ℹ️  Début du découpage du traitement pour chaque commune")

    # On récupère le surfacique de la Métropole de Lyon
    communes_larges=wfs2gp_df("metropole-de-lyon:adr_voie_lieu.adrcommunes_2024","https://data.grandlyon.com/geoserver/metropole-de-lyon/ows?SERVICE=WFS", reprojMetro=True, targetProj='EPSG:2154')

    # On ne garde que les communes de la Métropole de Lyon
    communes=communes_larges[communes_larges["communegl"]==True] 

    # On retire l'entité qui comprends tous les arrondissements de Lyon
    communes=communes[communes["trigramme"]!='LYO']

    # DEBUG
    # print('GDF communes')
    # print(communes)

    print("✅ Chargement des communes terminé")

    for index, row in communes.iterrows():

        # FIXME: Import caca dans la loop mais sinon Python ne la voit pas...
        from rasterio.mask import mask
        
        # DEBUG print commune row
        # print(row)

        # Start Timer
        loopTimerName = "loopTimer_" + row['insee'] + '_' + row['trigramme'] + '_' + row['nom']
        looptimer = startTimerLog(loopTimerName)
        print('ℹ️  Traitement appliqué à la commune n°', index, ':',row['insee'], row['trigramme'], row['nom'])

        # Vérifie s'il faut tester une liste spécifique, et si la commune est dans cette liste
        if specificComList:

            if not row['insee'] in specificComList:
                print('☑️  Commune n°', index, ':', row['insee'], row['trigramme'], row['nom'], 'ignorée')
                # Fin du timer de l'item de loop ignoré
                endTimerLog(looptimer)
                # Skip current commune
                continue

        # Get current Geom
        currentGeom = row['geometry']
        # print(currentGeom)

        ### Clipper le raster à la geom séléctionnée
        raster_clipped, transform_clipped = mask(dataset=raster, shapes=[currentGeom], crop=True)
        raster_clipped = raster_clipped[0]

        # =================================
        # Starting geom process
        # =================================

        ### Etape 3 : Nettoyer les valeurs inutiles
        print("ℹ️  Début Etape 3 : Nettoyer les valeurs inutiles")

        # Timer
        etape3Com = "etape3_" + row['insee'] + '_' + row['trigramme'] + '_' + row['nom']
        etape3timer = startTimerLog(etape3Com)

        # 1. Vérifier les valeurs présentes
        valeurs = np.unique(raster_clipped)
        print("Valeurs uniques avant nettoyage :", valeurs)

        # 2. Convertir 255 (ou autre code NODATA si besoin) en NaN
        # NB : parfois, c’est 0 qui est utilisé comme NODATA dans les GeoTIFF
        # donc on adapte selon ce que tu observes :
        raster_clean = np.where(raster_clipped == 255, np.nan, raster_clipped)

        # 3. Revoir les valeurs restantes
        valeurs_utiles = np.unique(raster_clean[~np.isnan(raster_clean)])
        print("Valeurs uniques après nettoyage :", valeurs_utiles)

        # 4. Calculer extent à partir du transform raster clippé
        extent = (
            transform_clipped[2],  # minX
            transform_clipped[2] + raster_clipped.shape[1] * transform_clipped[0],  # maxX
            transform_clipped[5] + raster_clipped.shape[0] * transform_clipped[4],  # minY
            transform_clipped[5],  # maxY
        )

        endTimerLog(etape3timer)
        print("✅ Etape 3 terminée")

        ### Etape 4 : Vectoriser le raster sur la zone
        print("ℹ️  Début Etape 4 : Vectoriser le raster sur la zone")

        # Timer
        etape4Com = "etape4_" + row['insee'] + '_' + row['trigramme'] + '_' + row['nom']
        etape4timer = startTimerLog(etape4Com)

        # 1. Convertir en masque binaire si on veut ignorer les NaN
        mask = ~np.isnan(raster_clean)

        # 2. Extraire les formes (géométries) et leurs valeurs
        shape_gen = (
            {"geometry": shape(geom), "properties": {"classe": int(value)}}
            for geom, value in shapes(
                raster_clean.astype(np.int16), mask=mask, transform=transform_clipped
            )  # géoréférence les pixels
        )

        gdf_vect = gpd.GeoDataFrame.from_features(shape_gen, crs="EPSG:2154")

        code_classes = {
            1: "Herbacées",
            2: "Buisson (<1,5m)",
            3: "Arbustes (1,5m - 5m)",
            4: "Petits arbres (5 - 15m)",
            5: "Grands arbres (>15m)",
        }

        df_legend = pd.DataFrame(list(code_classes.items()), columns=["classe", "nom"])

        vege_vect_zone = gdf_vect.merge(df_legend, on="classe", how="left")

        endTimerLog(etape4timer)
        print("✅ Etape 4 terminée")

        ### Etape 5 : Nettoyer les surfaces et les éléments
        # print("ℹ️  Début Etape 5 : Nettoyer les surfaces et les éléments")

        # # Timer
        # etape5Com = "etape5_" + row['insee'] + '_' + row['trigramme'] + '_' + row['nom']
        # etape5timer = startTimerLog(etape5Com)

        # # Appliquer un petit buffer autour des entités
        # vege_buffer_zone = vege_vect_zone.copy()
        # vege_buffer_zone.geometry = vege_buffer_zone.geometry.buffer(0.2)

        # # faire une jointure combinée spatiale ET attributaire = fusion conditionnelle
        # # Pas possible nativement dans géopandas donc on doit passer par une autre façon de faire

        # # Ajouter un identifiant de "groupe spatial" basé sur intersection
        # vege_buffer_zone["groupe"] = -1  # valeur temporaire

        # # compteur de groupe
        # groupe_id = 0

        # for buffIdx, buffRow in vege_buffer_zone.iterrows():
        #     if vege_buffer_zone.loc[buffIdx, "groupe"] != -1:
        #         continue  # déjà affecté

        #     # Sélection des voisins spatiaux qui ont la même classe
        #     same_class = vege_buffer_zone[
        #         (vege_buffer_zone["classe"] == buffRow["classe"])
        #         & (vege_buffer_zone.geometry.intersects(buffRow.geometry))
        #     ]

        #     indices = same_class.index.tolist()
        #     vege_buffer_zone.loc[indices, "groupe"] = groupe_id
        #     groupe_id += 1

        # # Fusionner par groupe et classe
        # vege_fusion = vege_buffer_zone.dissolve(by=["classe", "groupe"], as_index=False)

        # # Réaffecter les noms de classes
        # vege_fusion["classe_nom"] = vege_fusion["classe"].map(code_classes)

        # endTimerLog(etape5timer)
        # print("✅ Etape 5 terminée")

        ### Etape 5 : (opti MiaouGPT) Nettoyer les surfaces et les éléments
        print("ℹ️  Début Etape 5 : Nettoyer les surfaces et les éléments")

        # Timer
        etape5Com = "etape5_" + row['insee'] + '_' + row['trigramme'] + '_' + row['nom']
        etape5timer = startTimerLog(etape5Com)

        # 1) Buffer vectorisé (assure-toi d'être en mètres ; sinon reprojette avant)
        vege_buffer_zone = vege_vect_zone.copy()
        vege_buffer_zone["geometry"] = vege_buffer_zone.geometry.buffer(0.2)

        # 2) Auto-sjoin spatial (pairs de géométries qui s’intersectent)
        #    - how='inner' évite les non-correspondances
        #    - predicate='intersects' s'appuie sur l'index spatial (STRtree)
        pairs = gpd.sjoin(
            vege_buffer_zone[["geometry"]],     # on ne garde QUE geometry ici
            vege_buffer_zone[["geometry"]],     # idem à droite
            how="inner",
            predicate="intersects",
        )

        # 3) Exclure les self-joins
        pairs = pairs[pairs.index != pairs["index_right"]]

        # 4) Garder seulement les paires de même classe
        left_cls  = vege_buffer_zone.loc[pairs.index, "classe"].to_numpy()
        right_cls = vege_buffer_zone.loc[pairs["index_right"], "classe"].to_numpy()
        pairs = pairs[left_cls == right_cls]

        # 5) Calcul des composantes connexes PAR CLASSE
        vege_buffer_zone["groupe"] = -1
        for cls, sub_idx in vege_buffer_zone.groupby("classe").groups.items():
            idx_list = list(sub_idx)
            pos = pd.Series(range(len(idx_list)), index=idx_list)  # map index→[0..k-1]

            p = pairs.loc[vege_buffer_zone.loc[pairs.index, "classe"].to_numpy() == cls]
            if p.empty:
                vege_buffer_zone.loc[idx_list, "groupe"] = np.arange(len(idx_list))
                continue

            rows = pos.loc[p.index].to_numpy()
            cols = pos.loc[p["index_right"]].to_numpy()

            data = np.ones(len(rows), dtype=np.uint8)
            k = len(idx_list)
            A = coo_matrix((data, (rows, cols)), shape=(k, k))
            A = A + A.T

            _, labels = connected_components(A, directed=False, return_labels=True)
            vege_buffer_zone.loc[idx_list, "groupe"] = labels

        # 6) Dissolve par groupe et classe
        vege_fusion = vege_buffer_zone.dissolve(by=["classe", "groupe"], as_index=False)

        # 7) Réaffecter les noms de classes
        vege_fusion["classe_nom"] = vege_fusion["classe"].map(code_classes)

        endTimerLog(etape5timer)
        print("✅ Etape 5 terminée")

        ### Etape 6 : Simplification des entités
        print("ℹ️  Début Etape 6 : Simplification des entités")

        # Timer
        etape6Com = "etape5_" + row['insee'] + '_' + row['trigramme'] + '_' + row['nom']
        etape6timer = startTimerLog(etape6Com)

        # Le but est de retirer l'effet dent de scie
        # Application à tout le GeoDataFrame
        vege_smooth = vege_fusion.copy()
        vege_smooth["geometry"] = vege_smooth.geometry.apply(simplifier_geom)

        # vege_smooth.plot(column="classe", cmap=cmap, legend=True)

        # TODO: Décaler dans utils...
        # Lissage "arrondi" avec buffer+ puis buffer-
        def buffer_smooth(geom, r=1.0):
            return geom.buffer(r).buffer(-r)

        # Application à tout le GeoDataFrame
        vege_lisse_buffer = vege_smooth.copy()
        vege_lisse_buffer["geometry"] = vege_lisse_buffer.geometry.apply(
            lambda g: buffer_smooth(g, r=1)
        )

        endTimerLog(etape6timer)
        print("✅ Etape 6 terminée")

        ### Etape 7 : Regroupement des entités
        print("ℹ️  Début Etape 7 : Regroupement des entités")

        # Timer
        etape7Com = "etape7_" + row['insee'] + '_' + row['trigramme'] + '_' + row['nom']
        etape7timer = startTimerLog(etape7Com)

        # Regroupement de la classe 2 & 3 et 4 & 5
        vege_lisse_buffer = vege_lisse_buffer.copy()

        # Mapping : classe initiale → groupe fusionné
        fusion_classes = {
            1: "herbacee",
            2: "arbustif",  # 2 + 3
            3: "arbustif",
            4: "arborescent",  # 4 + 5
            5: "arborescent",
        }

        # Appliquer la fusion
        vege_lisse_buffer["strate"] = vege_lisse_buffer["classe"].map(fusion_classes)
        vege_groupes = vege_lisse_buffer.dissolve(by="strate", as_index=False)
        vege_groupes = vege_groupes.explode(index_parts=False, ignore_index=True)
        # vege_groupes.plot(column="strate", cmap="Set2", legend=True)

        ### Nettoyage des géométries trop petites
        vege_clean = vege_groupes.copy()
        vege_clean["surface_m2"] = vege_clean.geometry.area

        # Filtrer uniquement les entités dont la surface est suffisante
        vege_clean = vege_clean[vege_clean["surface_m2"] >= 2.5]

        ### Nettoyage des petits trous
        # ⚠️ call de la fonction

        vege_clean["geometry"] = vege_clean.geometry.apply(
            lambda g: remove_small_holes(g, area_thresh=2.0)
        )

        endTimerLog(etape7timer)
        print("✅ Etape 7 terminée")

        ### Etape finale : Export de la commune
        print("ℹ️  Début Etape finale : Export de la commune")

        # Timer
        etapeFinCom = "etapeFin_" + row['insee'] + '_' + row['trigramme'] + '_' + row['nom']
        etapeFintimer = startTimerLog(etapeFinCom)

        # Construction du path (⚠️ PENSER AU TRIGRAMME DE LA COMMUNE)
        exportPath = os.path.join(OUTPUT_DATA_DIR, "vegetation_stratifiee_2018_2154_" + row["trigramme"] + ".shp")
        
        # Export
        vege_clean.to_file(filename=exportPath, encoding="utf-8")

        endTimerLog(etapeFintimer)
        print("✅ Etape finale terminée")

        # =================================
        # Ending geom process
        # =================================

        # Fin du timer de l'item de loop
        endTimerLog(looptimer)

# TODO: Faire le regroupement des fichiers exportés