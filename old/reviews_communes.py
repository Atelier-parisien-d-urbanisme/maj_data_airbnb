#coding:utf-8

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from datetime import date
from forex_python.converter import CurrencyRates
import geopandas as gpd

# Ce script permet de traiter, de géolocaliser et de calculer le nombres de commentaires pour chaques communes à partir des données AIRBNB provenant d'INSIDE AIRBNB...


def traitement_reviews_data_airbnb_sig(chemin_dossier_listings,chemin_dossier_sortie,shapefile,nom_fichier_listings,champ_1,champ_2):
    
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
            data_airbnb_read= read_data_airbnb_listings[['id','latitude','longitude','listing_url','room_type','price','availability_365','calculated_host_listings_count','reviews_per_month','license','last_scraped','ville','date_tele']]

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
            zones = zones[[champ_1,champ_2, 'geometry']] 

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


    ##### TRAITEMENT REVIEWS #####

    # Création d'une table de stockage
    data_airbnb_reviews = []

    liste_fichier_reviews = os.listdir(chemin_dossier_reviews)
    
    for fichier in liste_fichier_reviews:  # Lire et stocker les fichiers *.csv dans une seule fichier
        if fichier.startswith(nom_fichier_reviews):
            
            print(fichier)

            # Lire les *.csv
            data_airbnb_read = pd.read_csv(chemin_dossier_reviews+"\\"+ fichier,sep=';',engine='python')

            # Sélection des champs utiles
            data_airbnb_read = data_airbnb_read[['listing_id','id','date','reviewer_id','comments','reviewer_name','ville','date_tele']]
    
    # Stockage des données
    data_airbnb_reviews.append(data_airbnb_read)

    # Concatener les tables stockées
    data_airbnb_reviews = pd.concat(data_airbnb_reviews)

    # Supprimer les doublons basés sur toutes les colonnes
    data_airbnb_reviews = data_airbnb_reviews.drop_duplicates()

    # Jointure sur les champ 'id' et 'listing_id'
    data_airbnb_reviews_merge = pd.merge(data_airbnb_reviews, data_airbnb_listings, on='listing_id', how='inner')

    # Sélectionner des colonnes spécifiques
    data_airbnb_reviews_merge = data_airbnb_reviews_merge[['listing_id', 'id', 'reviewer_id', 'date', 'comments', champ_1, champ_2]]

    # Identitfier dans les commentaires avec les messages automatiques
    data_airbnb_reviews_merge['com_auto'] = data_airbnb_reviews_merge['comments'].str.contains('This is an automated posting') 

    # Enlever les messages automatiques
    data_airbnb_reviews_sel = data_airbnb_reviews_merge[(data_airbnb_reviews_merge['com_auto'] == False)]

    # Comptages des commentraires
    data_airbnb_reviews_concat_sel_groupby = data_airbnb_reviews_sel.groupby(['listing_id','id','date',champ_1,champ_2]).count()

    # Ré-initialiser l'index
    data_airbnb_reviews_concat_sel_groupby = data_airbnb_reviews_concat_sel_groupby.reset_index()
    
    # Sélection des champs
    data_airbnb_reviews_concat_sel_groupby = data_airbnb_reviews_concat_sel_groupby[['listing_id','id','date',champ_1,champ_2]]
    
    # Conversion du champ date
    data_airbnb_reviews_concat_sel_groupby['date'] = pd.to_datetime(data_airbnb_reviews_concat_sel_groupby['date'])

    # Création d'un champ mois
    data_airbnb_reviews_concat_sel_groupby['mois'] = data_airbnb_reviews_concat_sel_groupby['date'].dt.month

    # Création d'un champ année
    data_airbnb_reviews_concat_sel_groupby['annee'] = data_airbnb_reviews_concat_sel_groupby['date'].dt.year

    # Suppression des doublons
    data_airbnb_reviews_concat_sel_groupby = data_airbnb_reviews_concat_sel_groupby.drop_duplicates()

    # Comptages des commentraires
    data_airbnb_reviews_concat_sel_groupby_count = data_airbnb_reviews_concat_sel_groupby.groupby(['mois','annee', champ_1, champ_2]).count()

    # Ré-initialiser l'index
    data_airbnb_reviews_concat_sel_groupby_count = data_airbnb_reviews_concat_sel_groupby_count.reset_index()

    # Sélection des champs
    data_airbnb_reviews_concat_sel_groupby_count = data_airbnb_reviews_concat_sel_groupby_count[['mois','annee','listing_id', champ_1, champ_2]]

    # Rennomer le champ
    data_airbnb_reviews_concat_sel_groupby_count.rename({'listing_id':'nbres_commentaires'},axis=1, inplace=True)

    # Trier par année, mois et communes
    data_airbnb_reviews_concat_sel_groupby_count_sort = data_airbnb_reviews_concat_sel_groupby_count.sort_values(by=['annee', 'mois', champ_1, champ_2])

    print(data_airbnb_reviews_concat_sel_groupby_count_sort)

    # Sauvegarder en *.csv
    data_airbnb_reviews_concat_sel_groupby_count_sort.to_csv(chemin_dossier_sortie + '\\' + nom_export,index = True, sep=';')


champ_1 = 'l_cab' #'l_ar' 
champ_2 = 'c_cainsee' # 'c_arinsee'
nom_export = 'DATA_AIRBNB_IDF_COMMENTAIRES_SIG_COMMUNES_2024_SANS_HOTEL.csv'
shapefile = r'P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\shapefile_ville\idf.shp'
nom_fichier_listings = "listings_idf" # début des noms des fichiers avec ville pour récupérer les coordonnées des annonces
nom_fichier_reviews = "reviews_idf" # début des noms des fichiers avec ville pour les commentaires
chemin_dossier_listings = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\LISTINGS_AIRBNB' # Chemin du dossier des annonces avec localisation
chemin_dossier_reviews = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\REVIEWS_AIRBNB' # Chemin du dossier des commentaires
chemin_dossier_sortie = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\commune_output' # Chemin du dossier de sortie

traitement_reviews_data_airbnb_sig(chemin_dossier_listings,chemin_dossier_sortie,shapefile,nom_fichier_listings,champ_1,champ_2)
