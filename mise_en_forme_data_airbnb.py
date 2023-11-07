#coding:utf-8

import pandas as pd
import os

# Ce script permet de mettre en forme les données AIRBNB provenant d'INSIDE AIRBNB...

chemin_dossier = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB' # Chemin du dossier

def mise_en_forme_listings_data_airbnb(chemin_dossier_traitement):
    
    liste_fichier = os.listdir(chemin_dossier_traitement)
    
    for fichier in liste_fichier:
        
        print("Mise en forme de {}...".format(fichier))
        
        nom = fichier.replace('reviews_','')
        nom = nom.replace('.csv','')
        nom = nom.replace('_',',')
        nom_split = nom.split(',')
    
        v = nom_split[0]
        a = nom_split[1]
        m = nom_split[2]
        j = nom_split[3]

        fichier_entree = chemin_dossier_traitement + "\\" + "{}".format(fichier)
        
        data_airbnb_temp = pd.read_csv(fichier_entree)
        data_airbnb_temp['ville'] = '{}'.format(v)
        data_airbnb_temp['date_tele'] = '{}-{}-{}'.format(a,m,j)
        data_airbnb_temp['date_tele'] = pd.to_datetime(data_airbnb_temp['date_tele'])
        print(data_airbnb_temp)
        data_airbnb_temp.to_csv(fichier_entree,index = True, sep=';')
    
    print("Mise en forme data Inside AirBnb terminée...")

chemin_dossier_traitement = r'P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\traitement'  # Chemin du dossier avec les fichiers à traiter            

mise_en_forme_listings_data_airbnb(chemin_dossier_traitement)