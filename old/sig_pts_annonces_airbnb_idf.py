# coding:utf-8

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import geopandas as gpd


# Ce script permet de traiter, de géolocaliser et de calculer les différents indicateurs sur les données AIRBNB provenant d'INSIDE AIRBNB...


def traitement_reviews_data_airbnb_sig(chemin_dossier_listings,chemin_dossier_sortie,shapefile,nom_fichier_listings):
    
    liste_fichier = os.listdir(chemin_dossier_listings)

    
    liste_fichier_dossier = [fichier for fichier in liste_fichier if fichier.startswith(nom_fichier_listings)]
    

    
    for fichier in liste_fichier_dossier: 
  
        print(fichier)
        read_data_airbnb = pd.read_csv(chemin_dossier_listings+"\\"+ fichier,sep=';')
        data_airbnb = read_data_airbnb[['id','latitude','longitude','room_type','ville','date_tele']]

                
        ville_shape = gpd.read_file(shapefile) # lecture du shapefile 
        ville_shape = ville_shape.to_crs(2154)
        points_gdf = gpd.GeoDataFrame(data_airbnb, geometry=gpd.points_from_xy(data_airbnb.longitude, data_airbnb.latitude),crs=4326) # WGS84
        points_gdf = points_gdf.to_crs(2154) # RGF_1993
        
        plt.show()

        print(chemin_dossier_sortie + "//"+ fichier[:23])
        points_gdf.to_file(chemin_dossier_sortie + "//"+ fichier[:23] + ".shp")


shapefile = r'P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\shapefile_ville\paris.shp'
nom_fichier_listings = "listings_idf_2024_09" # début des noms des fichiers avec ville pour récupérer les coordonnées des annonces
chemin_dossier_listings = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\LISTINGS_AIRBNB' # Chemin du dossier des annonces avec localisation
chemin_dossier_sortie = r'P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\points\idf' # Chemin du dossier de sortie


traitement_reviews_data_airbnb_sig(chemin_dossier_listings,chemin_dossier_sortie,shapefile,nom_fichier_listings)

