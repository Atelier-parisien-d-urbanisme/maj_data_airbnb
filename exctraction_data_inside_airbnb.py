#coding:utf-8

import os
import numpy as np
import requests
import gzip
import pandas as pd
from datetime import datetime
from io import StringIO

# Ce script permet d'extraire et traiter les données AIRBNB provenant du site INSIDE AIRBNB...

# ----------------------------------------------------
# 🔧 Fonction d'extraction des données listings
# ----------------------------------------------------

def extraction_listings_data_airbnb(ville_url, villes, annees, jours, mois, chemin_dossier_listings):
    
    os.makedirs(chemin_dossier_listings, exist_ok=True)  # crée le dossier si non existant
    liste_fichier_listings = os.listdir(chemin_dossier_listings)
    print("Fichiers déjà présents:", len(liste_fichier_listings))
    
    for i, v in enumerate(villes):
        vu = ville_url[i]
        print("\n--- Ville:", v, "---")
        
        for a in annees:
            for m in mois:
                for j in jours:
                    try:
                        # Vérifie si la date est valide
                        datetime(int(a), int(m), int(j))
                    except ValueError:
                        continue  # saute les dates impossibles (ex: 2025-02-30)
                    
                    fichier_sortie = os.path.join(
                        chemin_dossier_listings,
                        f"listings_{v}_{a}_{m}_{j}.csv"
                    )
                    
                    # Si déjà téléchargé → skip
                    if fichier_sortie.split("\\")[-1] in liste_fichier_listings:
                        print("⚠️ Existe déjà:", fichier_sortie)
                        continue
                    
                    URL = f"https://data.insideairbnb.com/{vu}/{a}-{m}-{j}/data/listings.csv.gz"
                    # print("⏳ Téléchargement:", URL)
                    
                    r = requests.get(URL)
                    if r.status_code == 200:
                        try:
                            decom_str = gzip.decompress(r.content).decode("utf-8")
                            data_airbnb_temp = pd.read_csv(StringIO(decom_str)) 
                            data_airbnb_temp["ville"] = v
                            data_airbnb_temp["date_tele"] = f"{a}-{m}-{j}"
                            data_airbnb_temp.to_csv(fichier_sortie, index=False, sep=";")
                            print("✅ Sauvegardé:", fichier_sortie)
                        except Exception as e:
                            print("❌ Erreur décompression/lecture:", e)
                            continue
                    else:
                        # print("⚠️ Non disponible:", URL)
                        continue
    
    print("\n✅ Extraction terminée.")

# ----------------------------------------------------
# 🔧 Fonction d'extraction des données reviews
# ----------------------------------------------------

def extraction_reviews_data_airbnb(ville_url, villes, annees, jours, mois, chemin_dossier_reviews):
    
    os.makedirs(chemin_dossier_reviews, exist_ok=True)  # crée le dossier si besoin
    liste_fichier_reviews = os.listdir(chemin_dossier_reviews)
    print("Fichiers déjà présents:", len(liste_fichier_reviews))
    
    for i, v in enumerate(villes):
        vu = ville_url[i]
        print(f"\n--- Ville: {v} ---")
        
        for a in annees:
            for m in mois:
                for j in jours:
                    try:
                        # Vérifie que la date est valide (évite 30 février etc.)
                        datetime(int(a), int(m), int(j))
                    except ValueError:
                        continue
                    
                    fichier_sortie = os.path.join(
                        chemin_dossier_reviews,
                        f"reviews_{v}_{a}_{m}_{j}.csv"
                    )
                    
                    # Skip si déjà téléchargé
                    if os.path.basename(fichier_sortie) in liste_fichier_reviews:
                        continue
                    
                    URL = f"https://data.insideairbnb.com/{vu}/{a}-{m}-{j}/data/reviews.csv.gz"
                    print("⏳ Téléchargement:", URL)
                    
                    r = requests.get(URL)
                    if r.status_code == 200:
                        try:
                            decom_str = gzip.decompress(r.content).decode("utf-8")
                            data_airbnb_temp = pd.read_csv(StringIO(decom_str))  # ✅ fix
                            data_airbnb_temp["ville"] = v
                            data_airbnb_temp["date_tele"] = f"{a}-{m}-{j}"
                            data_airbnb_temp.to_csv(fichier_sortie, index=False, sep=";")
                            print("✅ Sauvegardé:", fichier_sortie)
                        except Exception as e:
                            # print("❌ Erreur décompression/lecture:", e)
                            continue
                    else:
                        # print("⚠️ Non disponible:", URL)
                        continue
    
    print("\n✅ Extraction des reviews terminée.")

# ----------------------------------------------------
# 🔧 Paramètrages d'extraction des données
# ----------------------------------------------------

chemin_dossier_listings = r'P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\003_Données\INSIDE_AIRBNB\LISTINGS_AIRBNB' # Chemin du dossier listings
chemin_dossier_reviews = r'P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\003_Données\INSIDE_AIRBNB\REVIEWS_AIRBNB' # Chemin du dossier reviews

ville_url = ['france/ile-de-france/paris','japan/kant%C5%8D/tokyo','united-states/ny/new-york-city','the-netherlands/north-holland/amsterdam','spain/catalonia/barcelona','germany/be/berlin','united-kingdom/england/london','france/auvergne-rhone-alpes/lyon','france/nouvelle-aquitaine/bordeaux']
villes = ['paris','tokyo','new_york','amsterdam','barcelone','berlin','londres','lyon','bordeaux']
annees = [2025] 
jours = ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31']
mois = ['01','02','03','04','05','06','07','08','09','10','11','12']

extraction_listings_data_airbnb(ville_url, villes, annees, jours, mois, chemin_dossier_listings)

extraction_reviews_data_airbnb(ville_url,villes,annees,jours,mois,chemin_dossier_reviews)