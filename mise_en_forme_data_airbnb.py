#coding:utf-8

import pandas as pd
import os

# Ce script permet de mettre en forme les données AIRBNB IDF provenant d'INSIDE AIRBNB...
# Le script lit les fichiers en format *.csv nommer sous la forme type_ville_année_mois_jours... 

# ----------------------------------------------------
# 🔧 Fonction de traitement des données
# ----------------------------------------------------

def mise_en_forme_listings_data_airbnb(chemin_dossier_traitement, type_fichier):

    liste_fichier = os.listdir(chemin_dossier_traitement)
    print("Fichiers trouvés :", liste_fichier)
    
    for fichier in liste_fichier:

        if fichier.startswith(type_fichier) and fichier.endswith(".csv"):

            print(f"Mise en forme de {fichier}...")

            # Nettoyage du nom
            nom = fichier.replace(type_fichier, '').replace('.csv', '')
            nom_split = nom.split('_')

            # On suppose que les 3 derniers sont AAAA, MM, JJ
            v = "_".join(nom_split[:-3])   # la ville peut contenir des "_"
            a, m, j = nom_split[-3:]

            # Chemin d’entrée
            fichier_entree = os.path.join(chemin_dossier_traitement, fichier)
            
            # Lecture CSV
            data_airbnb_temp = pd.read_csv(fichier_entree, sep=',')
            
            # Ajout colonnes
            data_airbnb_temp['ville'] = v
            data_airbnb_temp['date_tele'] = pd.to_datetime(f"{a}-{m}-{j}")
            
            print(data_airbnb_temp.head())  # aperçu seulement
            
            # Réécriture en CSV (avec ; si besoin)
            fichier_sortie = fichier_entree  # ou un autre nom si tu veux garder l’original
            data_airbnb_temp.to_csv(fichier_sortie, index=False, sep=';')
    
    print("✅ Mise en forme data Inside AirBnb terminée.")

# ----------------------------------------------------
# 🔧 Boucle de traitement des données d'Inside Airbnb
# ----------------------------------------------------

liste_type_fichier = ['reviews_','listings_']

for i in liste_type_fichier:

    type_fichier = i
    chemin_dossier_traitement = r'P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\003_Données\INSIDE_AIRBNB\traitement'
    mise_en_forme_listings_data_airbnb(chemin_dossier_traitement, type_fichier)
