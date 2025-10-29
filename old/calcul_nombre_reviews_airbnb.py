#coding:utf-8

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from datetime import date
from forex_python.converter import CurrencyRates
import geopandas as gpd

# Ce script permet de traiter, de géolocaliser et de calculer le nombres de commentaires sur les données AIRBNB provenant d'INSIDE AIRBNB...

def traitement_data_airbnb_reviews_sig(chemin_dossier_reviews,chemin_dossier_listings,chemin_dossier_sortie,shapefile,nom_fichier_listings,nom_fichier_reviews):
        
    liste_fichier_listings = os.listdir(chemin_dossier_listings)
    
    ##### TRAITEMENT LISTINGS #####
    data_airbnb_sig = []

    for fichier in liste_fichier_listings:  # Lire et stocker les fichiers *.csv dans une seule fichier
        if fichier.startswith(nom_fichier_listings):
            
            print(fichier)
            data_airbnb_read = pd.read_csv(chemin_dossier_listings+"\\"+ fichier,sep=';')
            data_airbnb_read = data_airbnb_read[(data_airbnb_read['room_type'] != 'Hotel room')]
            data_airbnb = data_airbnb_read[['id','latitude','longitude','ville']]
            
            data_airbnb_sig.append(data_airbnb)
            
    data_airbnb_sig_concat = pd.concat(data_airbnb_sig)
    data_airbnb_sig_concat_groupby = data_airbnb_sig_concat.groupby(['id','latitude','longitude']).count()
    data_airbnb_sig_concat_groupby_reset = data_airbnb_sig_concat_groupby.reset_index()
    print(data_airbnb_sig_concat_groupby_reset)
            
    ville_shape = gpd.read_file(shapefile) # lecture du shapefile 
    print(ville_shape)
    points_gdf = gpd.GeoDataFrame(data_airbnb_sig_concat_groupby_reset, geometry=gpd.points_from_xy(data_airbnb_sig_concat_groupby_reset.longitude, data_airbnb_sig_concat_groupby_reset.latitude),crs=4326) # WGS84
    points_gdf = points_gdf.to_crs(2154) # RGF_1993
    print(points_gdf)
    intersect_shape= gpd.sjoin(points_gdf, ville_shape[['l_ar', 'geometry']], how='left', predicate='intersects') # l_ar pour les arrondissements à modifier pour les autres villes
    intersect_in_shape = intersect_shape[(intersect_shape['l_ar'].notnull())]
    
    # intersect_in_shape = intersect_shape[(intersect_shape['l_ar']=='9e')] # arrondissements à modifier pour les autres ville
    
    print(intersect_in_shape)
        
    fig,ax = plt.subplots(figsize=(10,10))
    ville_shape.plot("l_ar",ax=ax)
    points_gdf.plot(ax=ax)
    # plt.show()   

    ##### TRAITEMENT REVIEWS #####
    data_airbnb_reviews = []
    
    liste_fichier_reviews = os.listdir(chemin_dossier_reviews)
    
    for fichier in liste_fichier_reviews:  # Lire et stocker les fichiers *.csv dans une seule fichier
        if fichier.startswith(nom_fichier_reviews):
            
            print(fichier)
            data_airbnb_read = pd.read_csv(chemin_dossier_reviews+"\\"+ fichier,sep=';',engine='python')
            data_airbnb = data_airbnb_read[['listing_id','id','date','reviewer_id','comments','reviewer_name','ville','date_tele']]
                            
            data_airbnb_reviews.append(data_airbnb)
    
    print(data_airbnb_reviews)
    
    data_airbnb_reviews_concat = pd.concat(data_airbnb_reviews)
    print(data_airbnb_reviews_concat)
 
    data_airbnb_reviews_concat['com_auto'] = data_airbnb_reviews_concat['comments'].str.contains('This is an automated posting') 
    print(data_airbnb_reviews_concat)
    
    data_airbnb_reviews_concat = data_airbnb_reviews_concat[(data_airbnb_reviews_concat['com_auto'] == False)]
    print(data_airbnb_reviews_concat)
    
    data_airbnb_reviews_concat['in_shape'] = data_airbnb_reviews_concat['listing_id'].isin(intersect_in_shape["id"])
    data_airbnb_reviews_concat_sel = data_airbnb_reviews_concat[(data_airbnb_reviews_concat['in_shape'] == True)]

    data_airbnb_reviews_concat_sel_groupby = data_airbnb_reviews_concat_sel.groupby(['listing_id','id','date']).count()
    print(data_airbnb_reviews_concat_sel_groupby)
    data_airbnb_reviews_concat_sel_groupby = data_airbnb_reviews_concat_sel_groupby.reset_index()
    print(data_airbnb_reviews_concat_sel_groupby)
    
    data_airbnb_reviews_concat_sel_groupby = data_airbnb_reviews_concat_sel_groupby[['listing_id','id','date']]
    print(data_airbnb_reviews_concat_sel_groupby)
    
    data_airbnb_reviews_concat_sel_groupby['date'] = pd.to_datetime(data_airbnb_reviews_concat_sel_groupby['date']) # Conversion du champ date
    data_airbnb_reviews_concat_sel_groupby['mois'] = data_airbnb_reviews_concat_sel_groupby['date'].dt.month
    data_airbnb_reviews_concat_sel_groupby['annee'] = data_airbnb_reviews_concat_sel_groupby['date'].dt.year
    print(data_airbnb_reviews_concat_sel_groupby)
    
    # data_airbnb_reviews_concat_sel_groupby_count = data_airbnb_reviews_concat_sel_groupby.groupby(['mois','annee']).count()
    # data_airbnb_reviews_concat_sel_groupby_count = data_airbnb_reviews_concat_sel_groupby_count.reset_index()
    # data_airbnb_reviews_concat_sel_groupby_count = data_airbnb_reviews_concat_sel_groupby_count[['mois','annee','listing_id']]
    # data_airbnb_reviews_concat_sel_groupby_count.rename({'listing_id':'nbres_commentaires'},axis=1, inplace=True)
    # data_airbnb_reviews_concat_sel_groupby_count_sort = data_airbnb_reviews_concat_sel_groupby_count.sort_values(by=['annee', 'mois'])
    # print(data_airbnb_reviews_concat_sel_groupby_count_sort)
    
    
    data_airbnb_reviews_concat_sel_groupby_count_sort = data_airbnb_reviews_concat_sel_groupby
    data_airbnb_reviews_concat_sel_groupby_count_sort.to_csv(chemin_dossier_sortie + nom_export,index = True, sep=';')     


annee = [2015,2016,2017,2018,2019,2020,2021,2022,2023,2024]



for i in annee:
    
    print("Traitement de 2015 à 2018, actuellement en cour",i)
    nom_export = '/new_output/NBRES_COMMENTAIRES_SIG_PARIS_{}.csv'.format(i)
    shapefile = r'\\zsfa\ZSF-APUR\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\shapefile_ville\paris.shp'
    nom_fichier_listings = "listings_paris_{}".format(i) # début des noms des fichiers avec ville pour récupérer les coordonnées des annonces
    nom_fichier_reviews = "reviews_paris_{}".format(i) # début des noms des fichiers avec ville pour les annonces
    chemin_dossier_reviews = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\REVIEWS_AIRBNB' # Chemin du dossier des commentaires
    chemin_dossier_listings = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\LISTINGS_AIRBNB' # Chemin du dossier des annonces avec localisation
    chemin_dossier_sortie = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB' # Chemin du dossier de sortie
    traitement_data_airbnb_reviews_sig(chemin_dossier_reviews,chemin_dossier_listings,chemin_dossier_sortie,shapefile,nom_fichier_listings,nom_fichier_reviews)




chemin_dossier_sortie_data = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\new_output'
liste_fichier_data= os.listdir(chemin_dossier_sortie_data)

data_airbnb_traite = []

for fichier in liste_fichier_data:  # Lire et stocker les fichiers *.csv dans une seule fichier
    if fichier.startswith("NBRES_COMMENTAIRES_SIG_PARIS_"):
        
        lire_data_airbnb = pd.read_csv(chemin_dossier_sortie_data + "\\" + fichier,sep=';',engine='python')
        data_airbnb = lire_data_airbnb[['listing_id','id','date','mois','annee']]
        
        
        data_airbnb_traite.append(data_airbnb)
        
data_airbnb_traite_concat = pd.concat(data_airbnb_traite)
data_airbnb_traite_concat = data_airbnb_traite_concat.drop_duplicates()
data_airbnb_traite_concat_groupby = data_airbnb_traite_concat.groupby(['mois','annee']).count()
data_airbnb_traite_concat_groupby = data_airbnb_traite_concat_groupby.reset_index()
data_airbnb_traite_concat_groupby_sort = data_airbnb_traite_concat_groupby.sort_values(by=['annee', 'mois'])
data_airbnb_traite_concat_groupby_sort = data_airbnb_traite_concat_groupby_sort[['mois','annee','listing_id']]
data_airbnb_traite_concat_groupby_sort.rename({'listing_id':'nbres_commentaires'},axis=1, inplace=True)
data_airbnb_traite_concat_groupby_sort.to_csv(chemin_dossier_sortie_data + "/DATA_AIRBNB_NBRES_COMMENTAIRES_SIG_PARIS_2015_2024_SANS_HOTEL.csv",index = True, sep=';') 
