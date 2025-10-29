#coding:utf-8

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import re
from itertools import combinations
from collections import defaultdict
import geopandas as gpd
from datetime import datetime

# Ce script permet de préparer les données d'Inside Airbnb pour le traitement ultérieure des données et le calul des indicateurs...

# ----------------------------------------------------
# 🔧 Les fonctions de spatialisation des données
# ----------------------------------------------------

def localisation_sig_paris(data):

    # Localisation du shapefile parisien
    shapefile = r"\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\003_Données\INSIDE_AIRBNB\shapefile_ville\idf_vs_paris.shp"
    
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
    zones = zones[['paris','geometry']].copy()

    # Jointure spatiale par rapport aux communes
    points_in_zones = gpd.sjoin(points, zones, how="left", predicate="within")

    # Table de travail
    data_airbnb_temp = points_in_zones

    return data_airbnb_temp


def localisation_sig_communes(data):
    shapefile_ville = r"\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\003_Données\INSIDE_AIRBNB\shapefile_ville\commmunes_in_mgp_epci.shp"
    
    # 🔹 Nettoyage automatique des colonnes parasites avant toute opération
    colonnes_a_supprimer = [c for c in data.columns if c.lower().startswith("index_")]
    if colonnes_a_supprimer:
        print(f"🧹 Suppression des colonnes conflictuelles : {colonnes_a_supprimer}")
        data = data.drop(columns=colonnes_a_supprimer, errors="ignore")
    
    # Convertir les données Airbnb en points shapefile
    points = gpd.GeoDataFrame(
        data,
        geometry=gpd.points_from_xy(data.longitude, data.latitude),
        crs=4326  # WGS84
    )

    # Conversion du système de projection
    points = points.to_crs(2154)  # RGF_1993

    # Lire le shapefile des communes IDF
    communes = gpd.read_file(shapefile_ville)

    # Conversion du système de projection
    communes = communes.to_crs(2154)

    # Vérifier et aligner les CRS
    if communes.crs != points.crs:
        points = points.to_crs(communes.crs)

    # Sélectionner les champs utiles
    communes = communes[['l_epci', 'l_cab', 'c_cainsee', 'lib', 'geometry']].copy()

    # 🔹 Jointure spatiale avec protection contre doublons
    points_in_zones = gpd.sjoin(points, communes, how="left", predicate="within")

    # Table de travail
    data_airbnb_temp = points_in_zones

    return data_airbnb_temp

# ----------------------------------------------------
# 🔧 Préparation et pré-traitement des données
# ----------------------------------------------------

# Prefixe données listings idf
nom_fichier_listings_idf = "listings_idf"

# Prefixe données listings paris
nom_fichier_listings_paris = "listings_paris"

# Chemin du dossier des annonces avec localisation
chemin_dossier_listings = r"\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\003_Données\INSIDE_AIRBNB\LISTINGS_AIRBNB" 

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

# Chemin de sortie des données traitées
chemin_sortie = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\003_Données\INSIDE_AIRBNB\LISTINGS_IDF_NET"

# Définir la limite de date (après janvier 2025 → traitement partiel)
limite = datetime(2025, 1, 1)

# Afficher les fichiers regroupés par mois et année avec distinction "idf" et "paris"
for date, categories in fichiers_par_date.items():

    if len(categories['idf']) == 0:
        continue  # rien à faire s'il n'y a pas de fichier idf

    print(f"\nDate: {date}")
    print(f"Fichiers IDF: {categories['idf']}")
    print(f"Fichiers Paris: {categories['paris']}")

    jour = categories['idf'][0][-6:].replace('.csv', '')

    # Extraire année et mois pour comparer
    annee, mois = map(int, date.split('_'))
    date_fichier = datetime(annee, mois, 1)

    # Charger les données IDF
    data_idf = pd.read_csv(categories['idf'][0], sep=';')

    # Cas 1 : après janvier 2025 → ne traiter que IDF
    if date_fichier >= limite:
        print("⚠️ Mois postérieur à janvier 2025 → on ne fusionne pas avec Paris.")
        df_idf_net = localisation_sig_paris(data_idf)  # seulement IDF
        df_idf_net_loc = localisation_sig_communes(df_idf_net)

    # Cas 2 : jusqu’à janvier 2025 → traitement complet avec Paris
    else:
        if len(categories['paris']) == 0:
            print("Aucun fichier Paris pour cette date.")
            continue

        data_paris = pd.read_csv(categories['paris'][0], sep=';')

        df_paris = localisation_sig_paris(data_paris)
        df_idf = localisation_sig_paris(data_idf)

        df_paris = df_paris[df_paris['paris'] == 'oui']
        df_idf_paris = df_idf[df_idf['paris'] == 'oui']
        df_idf_communes = df_idf[df_idf['paris'] == 'non']

        df_idf_net = pd.concat([df_idf_communes, df_paris], ignore_index=True)

        # Supprimer les colonnes conflictuelles (suffixées "_right")
        df_idf_net = df_idf_net.drop(
            columns=df_idf_net.columns[df_idf_net.columns.str.endswith("_right")],
            errors="ignore"
        )

        df_idf_net_loc = localisation_sig_communes(df_idf_net)

    # Sauvegarde des résultats (SHP et CSV)
    chemin_shp = f"{chemin_sortie}\\SHP\\listings_idf_{date}_{jour}.shp"
    chemin_csv = f"{chemin_sortie}\\listings_idf_{date}_{jour}.csv"

    df_idf_net_loc.to_file(chemin_shp)
    df_idf_net_loc.to_csv(chemin_csv, sep=';')