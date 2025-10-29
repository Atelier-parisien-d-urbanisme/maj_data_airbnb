#coding:utf-8

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from datetime import date
from forex_python.converter import CurrencyRates
import geopandas as gpd

# Ce script permet de traiter, de géolocaliser et de calculer le nombres de commentaires pour chaques communes à partir des données AIRBNB provenant d'INSIDE AIRBNB...

# ----------------------------------------------------
# 🔧 Fonction de traitements des commentaires
# ----------------------------------------------------

def traitement_reviews_data_airbnb_sig(chemin_dossier_listings,chemin_dossier_sortie,shapefile,nom_fichier_listings,champs):
    
    # Liste des fichier dans le dossier
    liste_fichier = os.listdir(chemin_dossier_listings)
    
    # Création d'une table de stockage
    data_airbnb_listings = []
    
    # Lire et stocker les fichiers *.csv dans une seule table
    for fichier in liste_fichier:  
        
        # Selection des fichiers commençant par 
        if fichier.startswith(nom_fichier_listings):
            
            print(fichier)

            # Lire les listings inside airbnb 
            read_data_airbnb_listings = pd.read_csv(chemin_dossier_listings + "\\" + fichier,sep=';')

            # Selection des champs utiles
            data_airbnb_read= read_data_airbnb_listings[['id','latitude','longitude','listing_url','room_type','ville','date_tele']]

            # Enlever les chambres hotels
            data_airbnb_read = data_airbnb_read[(data_airbnb_read['room_type'] != 'Hotel room')]

            # Convertire les données aribnb en points shapefile
            points = gpd.GeoDataFrame(data_airbnb_read, geometry=gpd.points_from_xy(data_airbnb_read.longitude, data_airbnb_read.latitude),crs=4326) # WGS84

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
            zones = zones[['geometry'] + champs] 

            # Jointure spatiale par rapport aux communes
            points_in_zones = gpd.sjoin(points, zones, how="left", predicate="within")

            # Renommer le champ
            points_in_zones = points_in_zones.rename(columns={'id': 'listing_id'})

            # Stockage des données
            data_airbnb_listings.append(points_in_zones)

    # Concatener les tables stockées
    data_airbnb_listings = pd.concat(data_airbnb_listings)

    # Supprimer les doublons basés sur toutes les colonnes
    data_airbnb_listings = data_airbnb_listings.drop_duplicates()
    print(data_airbnb_listings)
    
    # Champs à vérifier pour les doublons
    fields_to_check = ["listing_url","listing_id"] + champs

    # Identifier les doublons (tous les doublons sauf la première occurrence)
    duplicates_to_remove = data_airbnb_listings[data_airbnb_listings.duplicated(subset=fields_to_check, keep="first")]

    # Garder uniquement la première occurrence
    data_airbnb_listings_cleaned = data_airbnb_listings.drop_duplicates(subset=fields_to_check, keep="first")

    # Afficher les résultats
    print("Doublons à supprimer (sauf la première occurrence) :")
    print(duplicates_to_remove)

    print("\nDonnées après suppression des doublons :")
    print(data_airbnb_listings_cleaned)

    # Compter les occurrences de chaque combinaison unique
    counts = data_airbnb_listings_cleaned.groupby(fields_to_check).size().reset_index(name="Occurrences")
    # Afficher les combinaisons ayant plus d'une occurrence
    duplicates_summary = counts[counts["Occurrences"] > 1]

    print("Résumé des doublons :")
    print(duplicates_summary)

    ##### TRAITEMENT REVIEWS #####

    # Création d'une table de stockage
    # data_airbnb_reviews = []
    data_airbnb_reviews = pd.DataFrame(columns=['listing_id','id','date'])

    # Liste des fichier dans le dossier
    liste_fichier_reviews = os.listdir(chemin_dossier_reviews)
    
    for fichier in liste_fichier_reviews:  # Lire et stocker les fichiers *.csv dans une seule fichier
        if fichier.startswith(nom_fichier_reviews):
            
            print(fichier)

            # Lire les *.csv
            data_airbnb_read = pd.read_csv(chemin_dossier_reviews+"\\"+ fichier,sep=';',engine='python')

            # Sélection des champs utiles
            data_airbnb_read = data_airbnb_read[['listing_id','id','comments','date']]
            
            # Identitfier dans les commentaires avec les messages automatiques
            data_airbnb_read['com_auto'] = data_airbnb_read['comments'].str.contains('This is an automated posting') 

            # Enlever les messages automatiques
            data_airbnb_read = data_airbnb_read[(data_airbnb_read['com_auto'] == False)].sort_values(by=["id"])
            
            # Sélection des champs utiles
            data_airbnb_read = data_airbnb_read[['listing_id','id','date']]

            # Ajouter les lignes tout en s'assurant qu'il n'y a pas de doublons
            data_airbnb_read = data_airbnb_read.merge(data_airbnb_reviews, on=['listing_id','id','date'], how="left", indicator=True)

            # Sélectionner uniquement les lignes non présentes dans la table cible
            data_airbnb_read = data_airbnb_read[data_airbnb_read["_merge"] == "left_only"].drop(columns=["_merge"])

            # Ajouter ces lignes uniques à la table cible
            data_airbnb_reviews = pd.concat([data_airbnb_reviews, data_airbnb_read], ignore_index=True)
        
    print(data_airbnb_reviews)

    # Jointure entre listings et reviews
    data_airbnb_reviews_merge = pd.merge(data_airbnb_reviews, data_airbnb_listings_cleaned, on='listing_id', how='inner')

    # Sélectionner des colonnes spécifiques
    data_airbnb_reviews_merge = data_airbnb_reviews_merge[['listing_id', 'id', 'date'] + champs]

    # Visualiser les doublons
    tous_les_doublons = data_airbnb_reviews_merge[data_airbnb_reviews_merge.duplicated(keep=False)].sort_values(by=[champs[0],"id"])
    print("Toutes les occurrences des doublons :\n", tous_les_doublons)

    # Supprimer les doublons basés sur toutes les colonnes
    data_airbnb_reviews_merge = data_airbnb_reviews_merge.drop_duplicates()

    # Visualiser les doublons
    tous_les_doublons = data_airbnb_reviews_merge[data_airbnb_reviews_merge.duplicated(keep=False)].sort_values(by=[champs[0],"id"])
    print("Toutes les occurrences des doublons :\n", tous_les_doublons)

    # Comptages des commentraires
    data_airbnb_reviews_concat_sel_groupby = data_airbnb_reviews_merge.groupby(['listing_id','id','date'] + champs).count()

    # Ré-initialiser l'index
    data_airbnb_reviews_concat_sel_groupby = data_airbnb_reviews_concat_sel_groupby.reset_index()

    # Sélection des champs
    data_airbnb_reviews_concat_sel_groupby = data_airbnb_reviews_concat_sel_groupby[['listing_id','id','date'] + champs]
    
    # Conversion du champ date
    data_airbnb_reviews_concat_sel_groupby['date'] = pd.to_datetime(data_airbnb_reviews_concat_sel_groupby['date'])

    # Création d'un champ mois
    data_airbnb_reviews_concat_sel_groupby['mois'] = data_airbnb_reviews_concat_sel_groupby['date'].dt.month

    # Création d'un champ année
    data_airbnb_reviews_concat_sel_groupby['annee'] = data_airbnb_reviews_concat_sel_groupby['date'].dt.year

    # Suppression des doublons
    data_airbnb_reviews_concat_sel_groupby = data_airbnb_reviews_concat_sel_groupby.drop_duplicates()

    # Comptages des commentraires
    data_airbnb_reviews_concat_sel_groupby_count = data_airbnb_reviews_concat_sel_groupby.groupby(['mois','annee'] + champs).count()

    # Ré-initialiser l'index
    data_airbnb_reviews_concat_sel_groupby_count = data_airbnb_reviews_concat_sel_groupby_count.reset_index()

    # Sélection des champs
    data_airbnb_reviews_concat_sel_groupby_count = data_airbnb_reviews_concat_sel_groupby_count[['mois','annee','listing_id'] + champs]

    # Rennomer le champ
    data_airbnb_reviews_concat_sel_groupby_count.rename({'listing_id':'nbres_commentaires'},axis=1, inplace=True)

    # Trier par année, mois et communes
    data_airbnb_reviews_concat_sel_groupby_count_sort = data_airbnb_reviews_concat_sel_groupby_count.sort_values(by=['annee', 'mois'] + champs)

    print(data_airbnb_reviews_concat_sel_groupby_count_sort)

    # Sauvegarder en *.csv
    data_airbnb_reviews_concat_sel_groupby_count_sort.to_csv(chemin_dossier_sortie + '\\' + nom_export,index = True, sep=';')

# ----------------------------------------------------
# 1️⃣ Paramètrages pour l'Île de France
# ----------------------------------------------------

champ_1 = 'l_cab'
champ_2 = 'c_cainsee'
champ_3 = 'l_epci'
champ_4 = 'lib'
champs = [champ_1,champ_2,champ_3,champ_4]
limite_administrative = 'limite_administrative'
nom ='idf'
nom_1 = "IDF"

# ----------------------------------------------------
# 2️⃣ Paramètrages pour Paris
# ----------------------------------------------------

# champ_1 = 'l_ar' 
# champ_2 = 'c_arinsee'
# champs = [champ_1,champ_2]
# limite_administrative = 'shapefile_ville'
# nom ='paris'
# nom_1 = "PARIS"

# ----------------------------------------------------
# 🔧 Traitement des commentaires d'Airbnb
# ----------------------------------------------------

nom_export = 'DATA_AIRBNB_{}_COMMENTAIRES_SIG_COMMUNES_SANS_HOTEL.csv'.format(nom_1)
shapefile = r'P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\003_Données\INSIDE_AIRBNB\{}\{}.shp'.format(limite_administrative,nom)
nom_fichier_listings = "listings_{}".format(nom) # début des noms des fichiers avec ville pour récupérer les coordonnées des annonces
nom_fichier_reviews = "reviews_{}".format(nom) # début des noms des fichiers avec ville pour les commentaires
chemin_dossier_listings = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\003_Données\INSIDE_AIRBNB\LISTINGS_AIRBNB" # Chemin du dossier des annonces avec localisation
chemin_dossier_reviews = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\003_Données\INSIDE_AIRBNB\REVIEWS_AIRBNB" # Chemin du dossier des commentaires
chemin_dossier_sortie = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\003_Données\INSIDE_AIRBNB\SORTIE" # Chemin du dossier de sortie

traitement_reviews_data_airbnb_sig(chemin_dossier_listings,chemin_dossier_sortie,shapefile,nom_fichier_listings,champs)
