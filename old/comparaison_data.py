#coding:utf-8

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import re
from itertools import combinations
from collections import defaultdict
import geopandas as gpd



def localisation_sig_paris(data):

    # Localisation du shapefile parisien
    shapefile = r'P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\shapefile_ville\paris.shp'

    # Convertire les données aribnb en points shapefile
    points = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data.longitude, data.latitude),crs=4326) # WGS84

    # Conversion du système de projection
    points = points.to_crs(2154) # RGF_1993

    # Lire le fichier shapefile des communes idf
    zones = gpd.read_file(shapefile)

    # Conversion du système de projection
    zones = zones.to_crs(2154)

    # Vérifier et aligner les CRS
    if zones.crs != points.crs:
        points = points.to_crs(zones.crs)

    # Selection des champs 
    zones = zones[['c_arinsee','geometry']].copy()

    # Jointure spatiale par rapport aux communes
    points_in_zones = gpd.sjoin(points, zones, how="left", predicate="within")

    points_in_zones = points_in_zones[(points_in_zones['c_arinsee']==75000)]

    # Table de travail
    data_airbnb_temp = points_in_zones

    return data_airbnb_temp

# Prefixe données listings idf
nom_fichier_listings_idf = "listings_idf"

# Prefixe données listings paris
nom_fichier_listings_paris = "listings_paris"

# Chemin du dossier des annonces avec localisation
chemin_dossier_listings = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\LISTINGS_AIRBNB' 

# Liste des fichier dans le dossier
liste_fichier = os.listdir(chemin_dossier_listings)

# Obtenir la liste complète des fichiers dans le dossier avec leurs chemins commençant par
fichiers_idf = [os.path.join(chemin_dossier_listings, fichier) for fichier in os.listdir(chemin_dossier_listings) if os.path.isfile(os.path.join(chemin_dossier_listings, fichier)) and fichier.startswith(nom_fichier_listings_idf)]

# Obtenir la liste complète des fichiers dans le dossier avec leurs chemins commençant par
fichiers_paris = [os.path.join(chemin_dossier_listings, fichier) for fichier in os.listdir(chemin_dossier_listings) if os.path.isfile(os.path.join(chemin_dossier_listings, fichier)) and fichier.startswith(nom_fichier_listings_paris)]

# L'ensemble des fichiers listings idf et paris
fichiers = fichiers_idf + fichiers_paris

# Créer un dictionnaire pour stocker les fichiers par mois et année
fichiers_par_date = defaultdict(lambda: {'idf': [], 'paris': []})

# Parcourir les fichiers et extraire la date (année_mois)
for fichier in fichiers:
    try:
        # Extraire l'année et le mois du nom du fichier
        parts = fichier.split('_')
        annee = parts[-3]  # L'année est avant le mois
        mois = parts[-2]   # Le mois est avant le jour
        annee_mois = f"{annee}_{mois}"  # Formater en YYYY_MM
        
        # Vérifier si c'est un fichier "idf" ou "paris"
        if 'idf' in fichier.lower():
            fichiers_par_date[annee_mois]['idf'].append(fichier)
        elif 'paris' in fichier.lower():
            fichiers_par_date[annee_mois]['paris'].append(fichier)
    except IndexError:
        print(f"Nom de fichier non conforme : {fichier}")

chemin_sortie = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\analyse_erreur_idf"

id_uniques_df_idf_append = []
id_uniques_df_paris_append =[]
id_communs_append = []
date_append = []

data_id_uniques_df_idf_append = []
data_id_uniques_df_paris_append = []
data_id_communs_append = []

id = "listing_url"
# id = 'id'

# Afficher les fichiers regroupés par mois et année avec distinction "idf" et "paris"
for date, categories in fichiers_par_date.items():
    print(f"\nDate: {date}")
    print(f"Fichiers IDF: {categories['idf']}")
    print(f"Fichiers Paris: {categories['paris']}")

    if  len(categories['paris']) == 0 or not len(categories['idf']) == 0:

        # Lire les fichiers avec Pandas
        data_idf = pd.read_csv(categories['idf'][0], sep=';')
        data_paris = pd.read_csv(categories['paris'][0], sep=';')

        df_paris = localisation_sig_paris(data_paris)
        df_idf = localisation_sig_paris(data_idf)

        count_duplicates_idf = df_idf.duplicated(subset=[id]).sum()
        print(f"Nombres de doublons dans idf: {count_duplicates_idf}")
        count_duplicates_paris = df_paris.duplicated(subset=[id]).sum()
        print(f"Nombres de doublons dans paris: {count_duplicates_paris}")

        # ID présents dans df_idf mais pas dans df_paris
        id_uniques_df_idf = df_idf[~df_idf[id].isin(df_paris[id])]

        # ID présents dans df_paris mais pas dans df_idf
        id_uniques_df_paris = df_paris[~df_paris[id].isin(df_idf[id])]

        # ID communs dans les deux DataFrames
        id_communs = df_idf[df_idf[id].isin(df_paris[id])]

        # Affichage des résultats
        print("ID uniquement dans df_idf :\n", len(id_uniques_df_idf))
        print("\nID uniquement dans df_paris :\n", len(id_uniques_df_paris))
        print("\nID communs dans les deux bdd :\n", len(id_communs))

        id_uniques_df_idf.to_csv(chemin_sortie +"\\" + f"{id}_idf_uniquement_{date}.csv", sep=';') 
        id_uniques_df_paris.to_csv(chemin_sortie +"\\" + f"{id}_paris_uniquement_{date}.csv", sep=';') 
        id_communs.to_csv(chemin_sortie +"\\" + f"{id}_commun_idf_paris_{date}.csv", sep=';') 

        id_uniques_df_idf_append.append(len(id_uniques_df_idf))
        id_uniques_df_paris_append.append(len(id_uniques_df_paris))
        id_communs_append.append(len(id_communs))
        date_append.append(date)

    else:
        next

df = pd.DataFrame({
    'date': date_append,
    'ID uniquement dans df_idf': id_uniques_df_idf_append,
    'ID uniquement dans df_paris': id_uniques_df_paris_append,
    'Communs dans les deux bdd': id_communs_append})

print(df)
df.to_csv(chemin_sortie +"\\" + "comparaison_url_data_idf_paris.csv", sep=';') 