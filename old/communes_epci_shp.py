#coding:utf-8

import geopandas as gpd


def jointure_spatiale_limite_administrative(zone_1,zone_2,liste_champs,nom):

    print(f"Début de la jointure spatiale entre {zone_1} et {zone_2}...")

    # Charger les GeoDataFrames
    polygones_1 = gpd.read_file(zone_1)
    polygones_2 = gpd.read_file(zone_2)

    # Vérifier et harmoniser les CRS
    polygones_1 = polygones_1.to_crs(polygones_2.crs)

    # Calculer les centroïdes
    polygones_1['centroid'] = polygones_1.geometry.centroid
    centroid_gdf = gpd.GeoDataFrame(polygones_1.drop(columns='geometry'), 
                                    geometry=polygones_1['centroid'], 
                                    crs=polygones_1.crs)

    # Jointure spatiale
    jointure_resultat = gpd.sjoin(centroid_gdf, polygones_2, how='left', predicate='intersects')

    # Restaurer le GeoDataFrame avec les polygones d'origine
    jointure_resultat = gpd.GeoDataFrame(jointure_resultat.drop(columns='centroid'),
                                        geometry=polygones_1.geometry,
                                        crs=polygones_1.crs)

    print(jointure_resultat.columns)

    # Séléction des champs                 
    jointure_resultat = jointure_resultat[liste_champs]

    print(jointure_resultat.columns)

    # Sauvegarde en shapefile
    jointure_resultat.to_file(chemin_dossier + "//" + f"{nom}.shp")

    print(f"Jointure spatiale entre {zone_1} et {zone_2} est terminée...")

chemin_dossier = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\limite_administrative"
zone_1 = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\limite_administrative\commune.shp"
zone_2 = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\limite_administrative\epci.shp"
nom = "commune_epci"
liste_champs = ['l_epci','l_cab','c_cainsee','geometry']
jointure_spatiale_limite_administrative(zone_1,zone_2,liste_champs,nom)

zone_1 = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\limite_administrative\commune_epci.shp"
zone_2 = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\limite_administrative\mgp.shp"
nom = "commune_epci_mgp"
liste_champs = ['l_epci','l_cab','c_cainsee','lib','geometry']
jointure_spatiale_limite_administrative(zone_1,zone_2,liste_champs,nom)

zone_1 = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\limite_administrative\commune_epci_mgp.shp"
zone_2 = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\limite_administrative\paris.shp"
nom = "idf"
liste_champs = ['Paris','l_epci','l_cab','c_cainsee','lib','geometry']
jointure_spatiale_limite_administrative(zone_1,zone_2,liste_champs,nom)



# Charger un GeoDataFrame
chemin_fichier = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\limite_administrative\idf.shp"
gdf = gpd.read_file(chemin_fichier)

# Champ à modifier
champ_cible = "Paris"

# Remplir les valeurs vides (NaN) avec "Non"
gdf[champ_cible] = gdf[champ_cible].fillna("Non")

# Enregistrer les modifications dans un nouveau fichier
chemin_sortie = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\limite_administrative\idf.shp"
gdf.to_file(chemin_sortie)

print("Les cases vides ont été remplies avec 'Non', et le fichier a été enregistré.")


# Charger un GeoDataFrame
chemin_fichier = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\limite_administrative\idf.shp"
gdf = gpd.read_file(chemin_fichier)

# Champ à modifier
champ_cible = "lib"

# Remplir les valeurs vides (NaN) avec "Non"
gdf[champ_cible] = gdf[champ_cible].fillna("Non MGP")

# Enregistrer les modifications dans un nouveau fichier
chemin_sortie = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\limite_administrative\idf.shp"
gdf.to_file(chemin_sortie)

print("Les cases vides ont été remplies avec 'Non', et le fichier a été enregistré.")