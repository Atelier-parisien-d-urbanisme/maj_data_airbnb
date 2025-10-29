#coding:utf-8

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from datetime import date
from forex_python.converter import CurrencyRates
import geopandas as gpd

# Ce script permet de traiter, de géolocaliser et de calculer le nombres de commentaires sur les données AIRBNB provenant d'INSIDE AIRBNB...

def traitement_data_airbnb_reviews_sig(chemin_dossier_reviews,chemin_dossier_sortie,nom_fichier_reviews,nom_export):


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

    data_airbnb_reviews_concat_sel_groupby = data_airbnb_reviews_concat.groupby(['listing_id','id','date']).count()
    print(data_airbnb_reviews_concat_sel_groupby)
    data_airbnb_reviews_concat_sel_groupby = data_airbnb_reviews_concat_sel_groupby.reset_index()
    print(data_airbnb_reviews_concat_sel_groupby)
    
    data_airbnb_reviews_concat_sel_groupby = data_airbnb_reviews_concat_sel_groupby[['listing_id','id','date']]
    print(data_airbnb_reviews_concat_sel_groupby)
    
    data_airbnb_reviews_concat_sel_groupby['date'] = pd.to_datetime(data_airbnb_reviews_concat_sel_groupby['date']) # Conversion du champ date
    data_airbnb_reviews_concat_sel_groupby['mois'] = data_airbnb_reviews_concat_sel_groupby['date'].dt.month
    data_airbnb_reviews_concat_sel_groupby['annee'] = data_airbnb_reviews_concat_sel_groupby['date'].dt.year
    print(data_airbnb_reviews_concat_sel_groupby)    
    
    data_airbnb_reviews_concat_sel_groupby_count_sort = data_airbnb_reviews_concat_sel_groupby
    data_airbnb_reviews_concat_sel_groupby_count_sort.to_csv(chemin_dossier_sortie + nom_export,index = True, sep=';')     


# annee = [2024]


# for i in annee:
    
#     print("Traitement de 2015 à 2024, actuellement en cour",i)
#     nom_export = '/new_output/NBRES_COMMENTAIRES_SIG_IDF_{}.csv'.format(i)
#     nom_fichier_reviews = "reviews_idf_{}".format(i) # début des noms des fichiers avec ville pour les annonces
#     chemin_dossier_reviews = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\REVIEWS_AIRBNB' # Chemin du dossier des commentaires
    # chemin_dossier_sortie = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB' # Chemin du dossier de sortie
    # traitement_data_airbnb_reviews_sig(chemin_dossier_reviews,chemin_dossier_sortie,nom_fichier_reviews,nom_export)




chemin_dossier_sortie_data = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\new_output'
liste_fichier_data= os.listdir(chemin_dossier_sortie_data)

data_airbnb_traite = []

for fichier in liste_fichier_data:  # Lire et stocker les fichiers *.csv dans une seule fichier
    if fichier.startswith("NBRES_COMMENTAIRES_SIG_IDF_"):
        
        lire_data_airbnb = pd.read_csv(chemin_dossier_sortie_data + "\\" + fichier,sep=';',engine='python')
        data_airbnb = lire_data_airbnb[['listing_id','id','date','mois','annee']]
        
        
        data_airbnb_traite.append(data_airbnb)
        
data_airbnb_traite_concat = pd.concat(data_airbnb_traite)
data_airbnb_traite_concat = data_airbnb_traite_concat.drop_duplicates()


condition_2 = (data_airbnb_traite_concat['annee'] >= 2023) & (data_airbnb_traite_concat['mois'] >= 7)
df_filtered = data_airbnb_traite_concat[condition_2]
print(df_filtered)

df_filtered = df_filtered.groupby(['listing_id']).count()
df_filtered = df_filtered.reset_index()
df_filtered = df_filtered[['listing_id','id']]
df_filtered.rename({'id':'nbres_commentaires_sup_07_23'},axis=1, inplace=True)

print(df_filtered)

data_airbnb_traite_concat_groupby = data_airbnb_traite_concat.groupby(['listing_id']).count()
data_airbnb_traite_concat_groupby = data_airbnb_traite_concat_groupby.reset_index()
data_airbnb_traite_concat_groupby_sort = data_airbnb_traite_concat_groupby[['listing_id','id']]
data_airbnb_traite_concat_groupby_sort.rename({'id':'nbres_commentaires'},axis=1, inplace=True)

print(data_airbnb_traite_concat_groupby_sort)


table_joined = pd.merge(data_airbnb_traite_concat_groupby_sort, df_filtered, on='listing_id', how='inner')



table_joined.to_csv(chemin_dossier_sortie_data + "/DATA_AIRBNB_NBRES_COMMENTAIRES_SIG_IDF.csv",index = True, sep=';') 
