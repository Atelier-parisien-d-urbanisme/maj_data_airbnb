#coding:utf-8

import pandas as pd
import numpy as np
import requests
import gzip
import os

chemin_dossier_reviews = r'\\zsfa\ZSF-APUR\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\A transferer APUR' # Chemin du dossier reviews
chemin_dossier_reviews_out = r'\\zsfa\ZSF-APUR\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB'

def extraction_reviews_data_airbnb(chemin_dossier_reviews):
        
    liste_dossier_reviews = os.listdir(chemin_dossier_reviews)
    print(liste_dossier_reviews)
    
    
    for i in liste_dossier_reviews:
      
      dossier_entree = chemin_dossier_reviews + "\\" + i
      liste_fichier_reviews = os.listdir(dossier_entree)
      
      nom_dossier = i.replace("InsideAirbnb_","")
      nom_dossier_split = nom_dossier.split("-")
      v = 'paris'
      a = nom_dossier_split[0]
      m = nom_dossier_split[1]
      j = nom_dossier_split[2]
    
      for i in liste_fichier_reviews:
        
        if i.__contains__("listings") and i.__contains__(".gz"):
          fichier_entree = dossier_entree + "\\" + i
          print(fichier_entree)
            
          fichier_sortie = chemin_dossier_reviews_out + "\\" + "listings_{}_{}_{}_{}.csv".format(v,a,m,j)
          print(fichier_sortie)
          
          with open(fichier_entree, 'rb') as entree, open(fichier_sortie, 'w', encoding='utf8') as sortie:
            decom_str = gzip.decompress(entree.read()).decode('utf-8')
            sortie.write(decom_str)
            
          data_airbnb_temp = pd.read_csv(fichier_sortie)
          data_airbnb_temp['ville'] = '{}'.format(v)
          data_airbnb_temp['date_tele'] = '{}-{}-{}'.format(a,m,j)
          data_airbnb_temp.to_csv(fichier_sortie,index = True, sep=';')
          
        else:
          next
                           
    print("Extraction data AirBnb terminée...")
                            
extraction_reviews_data_airbnb(chemin_dossier_reviews)