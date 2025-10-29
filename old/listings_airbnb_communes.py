#coding:utf-8

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from datetime import date
import geopandas as gpd
import requests

# Ce script permet de traiter, de géolocaliser et de calculer les différents indicateurs sur les données AIRBNB provenant d'INSIDE AIRBNB sur l'ensemble des communes d'idf...

###### nettoyage ######
def nettoyage(df: pd.DataFrame):
    """
    Nettoie un DataFrame :
    - Vérifie et affiche les doublons sur 'listing_url' et 'id'
    - Supprime les doublons
    - Génère un résumé des valeurs manquantes
    """

    # Vérification et affichage des doublons
    for col in ["listing_url", "id"]:
        duplicate_counts = df[col].value_counts()
        count_duplicates = duplicate_counts[duplicate_counts > 1]

        if not count_duplicates.empty:
            print(f"⚠️ Doublons sur id et listing_url détectés dans '{col}' ({len(count_duplicates)} valeurs en double)")
            
            # Affichage des lignes en doublon
            doublons = df[df[col].isin(count_duplicates.index)]
            print(f"👉 Lignes en doublon pour '{col}':")
            print(doublons.sort_values(col).to_string(index=False))
        else:
            print(f"✅ Aucun doublon sur id et listing_url dans '{col}'")

    # Identifier les lignes dupliquées sur toutes les colonnes
    doublons = df[df.duplicated(keep=False)]  # keep=False marque toutes les occurrences

    if not doublons.empty:
        print(f"⚠️ Doublons détectés sur toutes les colonnes ({len(doublons)} lignes en double)")
        print("👉 Lignes en doublon :")
        print(doublons.to_string(index=False))
    else:
        print("✅ Aucun doublon sur toutes les colonnes")

    # Suppression des doublons
    df = df.drop_duplicates()

    # Résumé des valeurs manquantes
    na_count = df.isna().sum()
    na_percentage = (na_count / len(df)) * 100
    na_summary = pd.DataFrame({
        'NaN compte': na_count,
        'compte total': len(df),
        'NaN pourcentage': na_percentage
    })

    # Ajouter infos supplémentaires si dispo
    if "date_tele" in df.columns and not df["date_tele"].empty:
        na_summary["date_tele"] = df["date_tele"].iloc[0]
    if "ville" in df.columns and not df["ville"].empty:
        na_summary["ville"] = df["ville"].iloc[0]

    # Retourner à la fois le dataframe nettoyé et le résumé
    return df, na_summary


def traitement_listings_data_airbnb_sig(chemin_dossier_listings,chemin_dossier_sortie,shapefile_ville,nom_fichier_listings,champ_1,champ_2,nom_export_na,nom_export,champ_3,champ_4,champ_5,nom_ville):
    
    # Liste des fichier dans le dossier
    liste_fichier = sorted(set(os.listdir(chemin_dossier_listings)))

    # Table de stockage de données
    data_airbnb_append = []

    # Synthèse des valeurs nulls
    data_NA = []

    data_airbnb_temp = pd.DataFrame()  # initialisation
    
    # Lire et stocker les fichiers *.csv dans une seule table
    for fichier in liste_fichier:  
        
        # Selection des fichiers commençant par 
        if fichier.startswith(nom_fichier_listings):
            
            print(fichier)

            # Lire les listings inside airbnb 
            read_data_airbnb_listings = pd.read_csv(
                chemin_dossier_listings + "\\" + fichier,
                sep=';',
                encoding='latin1',        # ou 'ISO-8859-1'
                low_memory=False          # pour éviter le DtypeWarning
            )

            # Selection des champs utiles
            data_airbnb_read= read_data_airbnb_listings[['id','latitude','longitude','listing_url','room_type','price','availability_365','number_of_reviews_ltm','calculated_host_listings_count','reviews_per_month','license','last_scraped','ville','date_tele']].copy()

            # Convertire les données aribnb en points shapefile
            points = gpd.GeoDataFrame(data_airbnb_read, geometry=gpd.points_from_xy(data_airbnb_read.longitude, data_airbnb_read.latitude),crs=4326) # WGS84

            # Regarder avec la fonction nettoyage les valeurs doublons et nulls
            etat_data = nettoyage(data_airbnb_read)[1]

            # Conversion du système de projection
            points = points.to_crs(2154) # RGF_1993

            # Lire le fichier shapefile des communes idf
            zones = gpd.read_file(shapefile_ville)

            # Conversion du système de projection
            zones = zones.to_crs(2154)

            # Vérifier et aligner les CRS
            if zones.crs != points.crs:
                points = points.to_crs(zones.crs)

            # Selection des champs
            if nom_ville in ['idf']:
                zones = zones[[champ_1,champ_2,champ_3,champ_4,champ_5,'geometry']].copy()
            else:
                zones = zones[[champ_1,champ_2, 'geometry']].copy()

            # Jointure spatiale par rapport aux communes
            points_in_zones = gpd.sjoin(points, zones, how="left", predicate="intersects")

            # Table de travail
            data_airbnb_temp = points_in_zones


        ######### conversion prix et taux #########

        if not data_airbnb_temp.empty:

            # Conversion des dates
            data_airbnb_temp['date_tele'] = pd.to_datetime(data_airbnb_temp['date_tele'])
            data_airbnb_temp['last_scraped'] = pd.to_datetime(data_airbnb_temp['last_scraped'], format='%Y-%m-%d', errors='coerce')

            # Nettoyage du prix
            data_airbnb_temp['price'] = (
                data_airbnb_temp['price']
                .astype(str)
                .str.replace(r'[\s$,€]', '', regex=True)
                .str.replace(',', '.', regex=False)
                .astype(float))
            
            def get_rate(base, target, date):
                date_str = date.strftime("%Y-%m-%d") if hasattr(date, "strftime") else str(date)
                url = f"https://api.frankfurter.app/{date_str}?from={base}&to={target}"
                try:
                    r = requests.get(url, timeout=10)
                    data = r.json()
                    return data.get("rates", {}).get(target, 1.0)
                except Exception as e:
                    print(f"Erreur API Frankfurter pour {base}->{target}:", e)
                    return 1.0

            # Map des villes -> devises
            ville_to_currency = {
                'new_york': 'USD',
                'londres': 'GBP',
                'tokyo': 'JPY'
            }

            # Conversion des prix
            def convertir_prix_par_ville(df):
                df['date_taux_de_change'] = df['date_tele']
                df['taux_de_change'] = 1.0  # défaut EUR
                df['prix_euro'] = df['price']  # défaut EUR

                for ville, currency in ville_to_currency.items():
                    mask = df['ville'].str.lower() == ville
                    if mask.any():
                        taux = get_rate(currency, "EUR", df.loc[mask, 'date_tele'].iloc[0])
                        df.loc[mask, 'taux_de_change'] = taux
                        df.loc[mask, 'prix_euro'] = df.loc[mask, 'price'] * taux
                return df

            data_airbnb_temp = convertir_prix_par_ville(data_airbnb_temp)


            ######### availability et prix #########

            # Selection seulement des dispos différents de 0
            data_airbnb_dispo = data_airbnb_temp[(data_airbnb_temp['availability_365'] != 0)]

            # Selection seulement des logements entiers
            data_airbnb_logement_entier = data_airbnb_dispo[(data_airbnb_dispo['room_type'] == 'Entire home/apt')]

            # Selection seulement des logements entiers avec commentaires
            data_airbnb_logement_entier_reviews = data_airbnb_logement_entier[(data_airbnb_logement_entier['number_of_reviews_ltm'] > 0 )]

            # Selection des champs
            data_airbnb_logement_entier_reviews_sel = data_airbnb_logement_entier_reviews[['prix_euro',champ_2]]

            # Suppression des 5 % de valeurs extrêmes (valeurs aberrantes)
            low, high = 0.05, 0.95
            data_airbnb_logement_entier_reviews_sel_corr_com = data_airbnb_logement_entier_reviews_sel[data_airbnb_logement_entier_reviews_sel.select_dtypes(include=['number']).apply(lambda x: x.between(x.quantile(low), x.quantile(high))).all(axis=1)]
            
            # Moyenne des prix et réinitialisation de l'index
            data_airbnb_logement_entier_reviews_sel_corr_com = data_airbnb_logement_entier_reviews_sel_corr_com.groupby(champ_2).mean().reset_index()

            # Renomer le champ
            data_airbnb_logement_entier_reviews_sel_corr_com = data_airbnb_logement_entier_reviews_sel_corr_com.rename(columns={'prix_euro': 'prix_moyen_1j_dispo_corr_com'})

            # Selection des champs
            data_airbnb_logement_entier_sel = data_airbnb_logement_entier[['prix_euro',champ_2]]
            
            # Suppression des 5 % de valeurs extrêmes (valeurs aberrantes)
            low, high = 0.05, 0.95
            data_airbnb_logement_entier_dispo_prix_mean_corr = data_airbnb_logement_entier_sel[data_airbnb_logement_entier_sel.select_dtypes(include=['number']).apply(lambda x: x.between(x.quantile(low), x.quantile(high))).all(axis=1)]
            
            # Moyenne des prix et réinitialisation de l'index
            data_airbnb_logement_entier_dispo_prix_mean_corr = data_airbnb_logement_entier_dispo_prix_mean_corr.groupby(champ_2).mean().reset_index()

            # Renomer le champ
            data_airbnb_logement_entier_dispo_prix_mean_corr = data_airbnb_logement_entier_dispo_prix_mean_corr.rename(columns={'prix_euro': 'prix_moyen_1j_dispo_corr'})

            # Moyenne des prix et réinitialisation de l'index
            data_airbnb_logement_entier_dispo_prix_mean = data_airbnb_logement_entier_sel.groupby(champ_2).mean().reset_index()

            # Renomer le champ
            data_airbnb_logement_entier_dispo_prix_mean = data_airbnb_logement_entier_dispo_prix_mean.rename(columns={'prix_euro': 'prix_moyen_1j_dispo'})

            # Selection des champs
            data_airbnb_logement_entier_sel_1 = data_airbnb_logement_entier[['availability_365',champ_2]]

            # Comptage et réinitialisation de l'index
            data_airbnb_logement_entier_dispo_count = data_airbnb_logement_entier_sel_1.groupby(champ_2).count().reset_index()

            # Renomer le champ
            data_airbnb_logement_entier_dispo_count = data_airbnb_logement_entier_dispo_count.rename(columns={'availability_365': 'nbres_annonces_dispo_log_365'})

            # Selection des chambres diffèrents des hotels
            data_airbnb_sans_hotel_dispo = data_airbnb_dispo[(data_airbnb_dispo['room_type'] != 'Hotel room')]

            # Selection des champs
            data_airbnb_sans_hotel_dispo = data_airbnb_sans_hotel_dispo[['availability_365',champ_2]]

            # Comptage et réinitialisation de l'index
            data_airbnb_sans_hotel_dispo_count = data_airbnb_sans_hotel_dispo.groupby(champ_2).count().reset_index()
            
            # Renomer le champ
            data_airbnb_sans_hotel_dispo_count = data_airbnb_sans_hotel_dispo_count.rename(columns={'availability_365': 'nbres_annonces_dispo_sauf_hotel_365'})
    
    
            ######### license #########

            # Selection seulement des logements entiers
            data_airbnb_logement_entier = data_airbnb_temp[(data_airbnb_temp['room_type'] == 'Entire home/apt')]

            # Selection des champs
            data_airbnb_logement_entier = data_airbnb_logement_entier[['license',champ_2]]

            # Conversion le champ en texte
            data_airbnb_logement_entier['license'] = data_airbnb_logement_entier[['license']].astype(str)

            # Chercher les licences valide, mobilité, hotel et vide dans le champ des licence
            data_airbnb_logement_entier['license_valide'] = data_airbnb_logement_entier.apply(lambda x: str(x['license']).startswith(str(x[champ_2])) if pd.notnull(x['license']) and pd.notnull(x[champ_2]) else False,axis=1)
            data_airbnb_logement_entier['licence_mobilite'] = data_airbnb_logement_entier['license'].str.contains('mobi', na = False)
            data_airbnb_logement_entier['licence_hotel'] = data_airbnb_logement_entier['license'].str.contains('Exempt', na = False)                
            data_airbnb_logement_entier['licence_vide'] = data_airbnb_logement_entier['license'].isna()

            # Compter les vrais et faux pour chaque licence
            data_airbnb_license = data_airbnb_logement_entier.groupby(champ_2).agg({'license_valide': ['sum'],'licence_mobilite': ['sum'],'licence_hotel': ['sum'],'licence_vide': ['sum','count']})

            # Renommer les champs
            data_airbnb_license.columns = ['license_valide', 'licence_mobilite', 'licence_hotel', 'licence_vide','licence_total']

            # Calculer les licences qui restent
            data_airbnb_license['licence_autres'] = data_airbnb_license['licence_total'] - (data_airbnb_license['license_valide'] + data_airbnb_license['licence_mobilite'] + data_airbnb_license['licence_hotel'] + data_airbnb_license['licence_vide'])
            
            # Réinitialiser l'index de la table
            data_airbnb_license = data_airbnb_license.reset_index()

            ######### type_logement (room_type) #########

            # Selection des champs
            data_airbnb_type_logement = data_airbnb_temp[['room_type',champ_2]].copy()

            # Chercher les hotels, les chambres partagées,priveés et entiers dans le champ des type de chambres
            data_airbnb_type_logement['nbres_chambres_hotels'] = data_airbnb_type_logement['room_type'] =='Hotel room'
            data_airbnb_type_logement['nbres_logements_entiers'] = data_airbnb_type_logement['room_type'] =='Entire home/apt'
            data_airbnb_type_logement['nbres_chambres_privees'] = data_airbnb_type_logement['room_type'] =='Private room'
            data_airbnb_type_logement['nbres_chambres_partagees'] = data_airbnb_type_logement['room_type'] =='Shared room'

            # Compter les vrais et faux pour chaque type de logement
            data_airbnb_type_logement = data_airbnb_type_logement.groupby(champ_2).agg({'nbres_chambres_hotels': ['sum'],'nbres_logements_entiers': ['sum'],'nbres_chambres_privees': ['sum'],'nbres_chambres_partagees': ['sum','count']})
            
            # Renommer les champs
            data_airbnb_type_logement.columns = ['nbres_chambres_hotels', 'nbres_logements_entiers', 'nbres_chambres_privees', 'nbres_chambres_partagees','nombres_annonces']

            # Calculer la part de logement entier
            data_airbnb_type_logement['part_de_logements_entiers_(%)'] = data_airbnb_type_logement['nbres_logements_entiers'] / (data_airbnb_type_logement['nombres_annonces'] - data_airbnb_type_logement['nbres_chambres_hotels'])*100
            
            # Calculer le nombres d'annonces hors sans les hotels
            data_airbnb_type_logement["nombres_annonces_hors_hotels"] = data_airbnb_type_logement['nombres_annonces'] - data_airbnb_type_logement['nbres_chambres_hotels']

            # Réinitialiser l'index de la table
            data_airbnb_type_logement = data_airbnb_type_logement.reset_index()
     

            ######### annonces_par_loueur (calculated_host_listings_count) #########

            # Enlever les hotels de la selection
            data_airbnb_sans_hotel = data_airbnb_temp[(data_airbnb_temp['room_type'] != 'Hotel room')]

            # Selection des champs
            data_airbnb_mutli_loueur = data_airbnb_sans_hotel[['calculated_host_listings_count',champ_2]].copy()
            
            # Chercher les lignes ou les conditions sont vrais ou fausses
            data_airbnb_mutli_loueur['annonces_par_loueur_(1)'] = data_airbnb_mutli_loueur['calculated_host_listings_count'] == 1
            data_airbnb_mutli_loueur['annonces_par_loueur_(2_a_9)'] = (data_airbnb_mutli_loueur['calculated_host_listings_count'] >= 2) & (data_airbnb_mutli_loueur['calculated_host_listings_count'] <= 9)
            data_airbnb_mutli_loueur['annonces_par_loueur_(10_et_plus)'] = data_airbnb_mutli_loueur['calculated_host_listings_count'] >= 10
            
            # Compter les vrais et faux pour chaque type de logement
            data_airbnb_mutli_loueur = data_airbnb_mutli_loueur.groupby(champ_2).agg({'annonces_par_loueur_(1)': ['sum'],'annonces_par_loueur_(2_a_9)': ['sum'],'annonces_par_loueur_(10_et_plus)': ['sum','count']})
            
            # Renommer les champs
            data_airbnb_mutli_loueur.columns = ['annonces_par_loueur_(1)', 'annonces_par_loueur_(2_a_9)', 'annonces_par_loueur_(10_et_plus)', 'total_annonces']
            
            # Calculer la part des multiloueurs
            data_airbnb_mutli_loueur['part_annonces_de_multiloueurs_(%)'] = (data_airbnb_mutli_loueur['annonces_par_loueur_(2_a_9)'] + data_airbnb_mutli_loueur['annonces_par_loueur_(10_et_plus)']) / data_airbnb_mutli_loueur['total_annonces']*100

            # Réinitialiser l'index de la table
            data_airbnb_mutli_loueur = data_airbnb_mutli_loueur.reset_index()


            ######### annonces_par_loueur_365 (calculated_host_listings_count, availability_365) #########

            # Enlever les hotels de la selection
            data_airbnb_sans_hotel = data_airbnb_temp[(data_airbnb_temp['room_type'] != 'Hotel room')]

            # Selection des locations disponible ou moins 1 jour
            data_airbnb_sans_hotel_dispo = data_airbnb_sans_hotel[(data_airbnb_sans_hotel['availability_365'] != 0)]

            # Selection des champs
            data_airbnb_mutli_loueur_365 = data_airbnb_sans_hotel_dispo[['calculated_host_listings_count',champ_2]].copy()

            # Chercher les lignes ou les conditions sont vrais ou fausses
            data_airbnb_mutli_loueur_365['annonces_par_loueur_365_(1)'] = data_airbnb_mutli_loueur_365['calculated_host_listings_count'] == 1
            data_airbnb_mutli_loueur_365['annonces_par_loueur_365_(2_a_9)'] = (data_airbnb_mutli_loueur_365['calculated_host_listings_count'] >= 2) & (data_airbnb_mutli_loueur_365['calculated_host_listings_count'] <= 9)
            data_airbnb_mutli_loueur_365['annonces_par_loueur_365_(10_et_plus)'] = data_airbnb_mutli_loueur_365['calculated_host_listings_count'] >= 10

            # Compter les vrais et faux pour chaque type de logement
            data_airbnb_mutli_loueur_365 = data_airbnb_mutli_loueur_365.groupby(champ_2).agg({'annonces_par_loueur_365_(1)': ['sum'],'annonces_par_loueur_365_(2_a_9)': ['sum'],'annonces_par_loueur_365_(10_et_plus)': ['sum','count']})

            # Renommer les champs
            data_airbnb_mutli_loueur_365.columns = ['annonces_par_loueur_365_(1)', 'annonces_par_loueur_365_(2_a_9)', 'annonces_par_loueur_365_(10_et_plus)', 'total_annonces']

            # Calculer la part des multiloueurs
            data_airbnb_mutli_loueur_365['part_annonces_de_multiloueurs_365_(%)'] = (data_airbnb_mutli_loueur_365['annonces_par_loueur_365_(2_a_9)'] + data_airbnb_mutli_loueur_365['annonces_par_loueur_365_(10_et_plus)']) / data_airbnb_mutli_loueur_365['total_annonces']*100

            # Réinitialiser l'index de la table
            data_airbnb_mutli_loueur_365 = data_airbnb_mutli_loueur_365.reset_index()

            ######### nbr_commentaires + 1,75 (reviews_per_month) #########

            # Enlever les hotels de la selection
            data_airbnb_sans_hotel = data_airbnb_temp[(data_airbnb_temp['room_type'] != 'Hotel room')]

            # Selection des champs
            data_airbnb_nbr_commentaires = data_airbnb_sans_hotel[['reviews_per_month',champ_2]].copy()
            
            # Remplacer les lignes vides par 0
            data_airbnb_nbr_commentaires = data_airbnb_nbr_commentaires.fillna(0)

            # Chercher les lignes ou les conditions sont vrais ou fausses
            data_airbnb_nbr_commentaires['nbres_commentaires_(0)'] = data_airbnb_nbr_commentaires['reviews_per_month'] == 0
            data_airbnb_nbr_commentaires['nbres_commentaires_(0_a_1.75)'] = (data_airbnb_nbr_commentaires['reviews_per_month'] > 0) & (data_airbnb_nbr_commentaires['reviews_per_month'] < 1.75)
            data_airbnb_nbr_commentaires['nbres_commentaires_(1.75_et_plus)'] = data_airbnb_nbr_commentaires['reviews_per_month'] >= 1.75

            # Compter les vrais et faux pour chaque type de commentaires
            data_airbnb_nbr_commentaires = data_airbnb_nbr_commentaires.groupby(champ_2).agg({'nbres_commentaires_(0)': ['sum'],'nbres_commentaires_(0_a_1.75)': ['sum'],'nbres_commentaires_(1.75_et_plus)': ['sum','count']})

            # Renommer les champs
            data_airbnb_nbr_commentaires.columns = ['nbres_commentaires_(0)', 'nbres_commentaires_(0_a_1.75)', 'nbres_commentaires_(1.75_et_plus)', 'nbres_commentaires_total']            

            # Calculer la part des commentaires
            data_airbnb_nbr_commentaires['part_de_commentaires_(1.75_et_plus)'] = data_airbnb_nbr_commentaires['nbres_commentaires_(1.75_et_plus)'] / data_airbnb_nbr_commentaires['nbres_commentaires_total'] * 100

            # Réinitialiser l'index de la table
            data_airbnb_nbr_commentaires = data_airbnb_nbr_commentaires.reset_index()

            ######### nbr_commentaires + 12m #########

            # Enlever les hotels de la selection
            data_airbnb_sans_hotel = data_airbnb_temp[(data_airbnb_temp['room_type'] != 'Hotel room')]

            # Selection seulement des logements avec commentaires
            data_airbnb_sans_hotel_reviews = data_airbnb_sans_hotel[(data_airbnb_sans_hotel['number_of_reviews_ltm'] > 0 )]

            # Selection des champs
            data_airbnb_sans_hotel_reviews = data_airbnb_sans_hotel_reviews[['reviews_per_month',champ_2]].copy()
            
            # Remplacer les lignes vides par 0
            data_airbnb_sans_hotel_reviews = data_airbnb_sans_hotel_reviews.fillna(0)

            # Compter
            data_airbnb_sans_hotel_reviews = (data_airbnb_sans_hotel_reviews.groupby(champ_2).agg(nbres_commentaires_12m=('reviews_per_month', 'count')))

            # Réinitialiser l'index de la table
            nbres_commentaires_12m = data_airbnb_sans_hotel_reviews.reset_index()

            ######### nbr_commentaires + 12m + 365 #########

            # Enlever les hotels de la selection
            data_airbnb_sans_hotel = data_airbnb_temp[(data_airbnb_temp['room_type'] != 'Hotel room')]

            # Selection seulement des logements avec commentaires et disponibilité
            data_airbnb_sans_hotel_reviews_365 = data_airbnb_sans_hotel[(data_airbnb_sans_hotel['number_of_reviews_ltm'] > 0) & (data_airbnb_sans_hotel['availability_365'] > 0)]

            # Selection des champs
            data_airbnb_sans_hotel_reviews_365 = data_airbnb_sans_hotel_reviews_365[['reviews_per_month',champ_2]].copy()
            
            # Remplacer les lignes vides par 0
            data_airbnb_sans_hotel_reviews_365 = data_airbnb_sans_hotel_reviews_365.fillna(0)

            # Compter
            data_airbnb_sans_hotel_reviews_365 = (data_airbnb_sans_hotel_reviews_365.groupby(champ_2).agg(nbres_commentaires_12m_365=('reviews_per_month', 'count')))

            # Réinitialiser l'index de la table
            nbres_commentaires_12m_365 = data_airbnb_sans_hotel_reviews_365.reset_index()
                       
            ######### availability (availability_365) #########

            # Enlever les hotels de la selection
            data_airbnb_sans_hotel = data_airbnb_temp[(data_airbnb_temp['room_type'] != 'Hotel room')]

            # Selection des champs
            data_airbnb_availability = data_airbnb_sans_hotel[['availability_365',champ_2]].copy()

            # Chercher les lignes ou les conditions sont vrais ou fausses
            data_airbnb_availability['disponibilite_aucune'] = data_airbnb_availability['availability_365'] == 0
            data_airbnb_availability['disponibilite_inf_120_jours'] = (data_airbnb_availability['availability_365'] > 0) & (data_airbnb_availability['availability_365'] < 120)
            data_airbnb_availability['disponibilite_sup_120_jours'] = data_airbnb_availability['availability_365'] >= 120

            # Compter les vrais et faux pour chaque type de disponibilité
            data_airbnb_availability = data_airbnb_availability.groupby(champ_2).agg({'disponibilite_aucune': ['sum'],'disponibilite_inf_120_jours': ['sum'],'disponibilite_sup_120_jours': ['sum','count']})

            # Renommer les champs
            data_airbnb_availability.columns = ['disponibilite_aucune','disponibilite_inf_120_jours','disponibilite_sup_120_jours','dispo_total']

            # Calculer la part des dispos supérieur à 120 jours
            data_airbnb_availability['part_disponibilite_sup_120_jours_(%)'] = data_airbnb_availability['disponibilite_sup_120_jours'] / data_airbnb_availability['dispo_total'] * 100
           
            # Réinitialiser l'index de la table
            data_airbnb_availability = data_airbnb_availability.reset_index()


            ######### prix logement entier (price) #########
            
            # Selection seulement des logements entiers
            data_airbnb_logement_entier = data_airbnb_temp[(data_airbnb_temp['room_type'] == 'Entire home/apt')]

            # Selection des champs
            data_airbnb_price = data_airbnb_logement_entier[['prix_euro',champ_2]].copy()

            # Chercher les lignes ou les conditions sont vrais ou fausses
            data_airbnb_price['prix_logement_entier_inf_100_euro'] = data_airbnb_price['prix_euro'] <= 100
            data_airbnb_price['prix_logement_entier_sup_100_euro'] = data_airbnb_price['prix_euro'] > 100
            data_airbnb_price['prix_moyen'] = data_airbnb_price['prix_euro']

            # Compter les vrais et faux pour chaque type de prix
            data_airbnb_price = data_airbnb_price.groupby(champ_2).agg({'prix_logement_entier_inf_100_euro': ['sum'],'prix_logement_entier_sup_100_euro': ['sum'],'prix_moyen': ['mean','count']})

            # Renommer les champs
            data_airbnb_price.columns = ['prix_logement_entier_inf_100_euro','prix_logement_entier_sup_100_euro','prix_moyen','prix_total']

            # Calculer la part des prix
            data_airbnb_price['part_logement_entier_inf_100_euro'] = data_airbnb_price['prix_logement_entier_inf_100_euro'] / data_airbnb_price['prix_total'] * 100
            data_airbnb_price['part_logement_entier_sup_100_euro'] = data_airbnb_price['prix_logement_entier_sup_100_euro'] / data_airbnb_price['prix_total'] * 100

            # Réinitialiser l'index de la table
            data_airbnb_price = data_airbnb_price.reset_index()


            ######### prix logement entier 365 (price, availability_365) #########
            
            # Selection seulement des logements entiers
            data_airbnb_logement_entier = data_airbnb_temp[(data_airbnb_temp['room_type'] == 'Entire home/apt')]

            # Selection des locations disponible ou moins 1 jour
            data_airbnb_logement_entier_dispo = data_airbnb_logement_entier[(data_airbnb_logement_entier['availability_365'] != 0)]

            # Selection des champs
            data_airbnb_price_365 = data_airbnb_logement_entier_dispo[['prix_euro',champ_2]].copy()

            # Chercher les lignes ou les conditions sont vrais ou fausses
            data_airbnb_price_365['prix_logement_entier_inf_100_euro_365'] = data_airbnb_price_365['prix_euro'] <= 100
            data_airbnb_price_365['prix_logement_entier_sup_100_euro_365'] = data_airbnb_price_365['prix_euro'] > 100

            # Compter les vrais et faux pour chaque type de prix
            data_airbnb_price_365 = data_airbnb_price_365.groupby(champ_2).agg({'prix_logement_entier_inf_100_euro_365': ['sum'],'prix_logement_entier_sup_100_euro_365': ['sum'],'prix_euro': ['mean','count']})

            # Renommer les champs
            data_airbnb_price_365.columns = ['prix_logement_entier_inf_100_euro_365','prix_logement_entier_sup_100_euro_365','prix_moyen','prix_total']

            # Calculer la part des prix
            data_airbnb_price_365['part_logement_entier_inf_100_euro_365'] = data_airbnb_price_365['prix_logement_entier_inf_100_euro_365'] / data_airbnb_price_365['prix_total'] * 100
            data_airbnb_price_365['part_logement_entier_sup_100_euro_365'] = data_airbnb_price_365['prix_logement_entier_sup_100_euro_365'] / data_airbnb_price_365['prix_total'] * 100

            # Suppression des colonnes
            data_airbnb_price_365 = data_airbnb_price_365.drop(["prix_moyen", "prix_total"], axis=1)

            # Réinitialiser l'index de la table
            data_airbnb_price_365 = data_airbnb_price_365.reset_index()


            ######### date, ville, change et commune #########

            # Selection des champs
            date_telechargement = data_airbnb_temp[['date_tele',champ_2]].copy()

            # Suppression des doublons afin de récupérer la date de téléchargement
            date_telechargement = date_telechargement.drop_duplicates(subset=champ_2)

            # Selection des champs
            ville = data_airbnb_temp[['ville',champ_2]]

            # Suppression des doublons afin de récupérer la ville
            ville = ville.drop_duplicates(subset=champ_2)

            # Selection des champs
            taux_de_change = data_airbnb_temp[['taux_de_change',champ_2]]

            # Suppression des doublons afin de récupérer le taux par code commune
            taux_de_change = taux_de_change.drop_duplicates(subset=champ_2)

            # Selection des champs
            date_taux_change = data_airbnb_temp[['date_taux_de_change',champ_2]]

            # Suppression des doublons afin de récupérer la date du taux par code commune
            date_taux_change = date_taux_change.drop_duplicates(subset=champ_2)

            # Selection des champs
            if nom_ville in ['idf']:
                liste_champs_idf = [champ_1,champ_2,champ_3,champ_4,champ_5]
                nom_commune = data_airbnb_temp[liste_champs_idf]
            else:
                liste_champs_ville = [champ_1,champ_2]
                nom_commune = data_airbnb_temp[liste_champs_ville]

            # Suppression des doublons afin de récupérer le nom des communes par code commune
            nom_commune = nom_commune.drop_duplicates(subset=champ_2)

            # Liste des tables à joindre ensemble
            dataframes = [nom_commune,ville,date_telechargement,data_airbnb_logement_entier_dispo_prix_mean,data_airbnb_logement_entier_dispo_count,data_airbnb_sans_hotel_dispo_count,
                          data_airbnb_license,data_airbnb_type_logement,data_airbnb_mutli_loueur,data_airbnb_mutli_loueur_365,data_airbnb_nbr_commentaires, data_airbnb_availability, 
                          data_airbnb_price_365,data_airbnb_price,date_taux_change,taux_de_change,
                          nbres_commentaires_12m,nbres_commentaires_12m_365,data_airbnb_logement_entier_dispo_prix_mean_corr,data_airbnb_logement_entier_reviews_sel_corr_com]

            # Fusionner tous les tables sur le champ champ_2
            merged_df = dataframes[0]
            for df in dataframes[1:]:
                merged_df = pd.merge(merged_df, df, on=champ_2, how="left")

            liste_champs = ['ville','date_tele','nombres_annonces','nbres_chambres_hotels','nombres_annonces_hors_hotels','nbres_logements_entiers',
                                                'nbres_chambres_privees','nbres_chambres_partagees',
                                                'part_de_logements_entiers_(%)','nbres_annonces_dispo_log_365','nbres_annonces_dispo_sauf_hotel_365',
                                                'annonces_par_loueur_(1)','annonces_par_loueur_(2_a_9)','annonces_par_loueur_(10_et_plus)','part_annonces_de_multiloueurs_(%)',
                                                'annonces_par_loueur_365_(1)', 'annonces_par_loueur_365_(2_a_9)', 'annonces_par_loueur_365_(10_et_plus)','part_annonces_de_multiloueurs_365_(%)',
                                                'nbres_commentaires_(0)','nbres_commentaires_(0_a_1.75)','nbres_commentaires_(1.75_et_plus)',
                                                'part_de_commentaires_(1.75_et_plus)','nbres_commentaires_total','nbres_commentaires_12m','nbres_commentaires_12m_365','disponibilite_aucune',
                                                'disponibilite_inf_120_jours','disponibilite_sup_120_jours','part_disponibilite_sup_120_jours_(%)',
                                                'prix_logement_entier_inf_100_euro_365','prix_logement_entier_sup_100_euro_365','part_logement_entier_inf_100_euro_365','part_logement_entier_sup_100_euro_365',
                                                'prix_logement_entier_inf_100_euro','prix_logement_entier_sup_100_euro','part_logement_entier_inf_100_euro',
                                                'part_logement_entier_sup_100_euro','prix_moyen','prix_moyen_1j_dispo','prix_moyen_1j_dispo_corr','prix_moyen_1j_dispo_corr_com','taux_de_change','date_taux_de_change',
                                                'license_valide','licence_mobilite','licence_vide','licence_hotel','licence_autres']
            # Listes des indicateurs rangés
            if nom_ville in ['idf']:
                liste_champs_idf = [champ_1,champ_2,champ_3,champ_4,champ_5]
                merged_df = merged_df[liste_champs_idf + liste_champs]
            else:
                liste_champs_ville = [champ_1,champ_2]
                merged_df = merged_df[liste_champs_ville + liste_champs]

            # Stocker les données
            data_airbnb_append.append(merged_df)

            # Stocker les données NA
            data_NA.append(etat_data)
    
    # Assemblage des données na
    if len(data_NA) > 0:
        data_NA_concat = pd.concat(data_NA)
        # Exporter les données na en csv
        data_NA_concat.to_csv(chemin_dossier_sortie + nom_export_na, index = True, sep=';')
    else:
        print("Warning: No data to concatenate in data_NA")
        data_NA_concat = pd.DataFrame()  

    # Assemblage des données
    data_airbnb_sig_concat = pd.concat(data_airbnb_append)

    # 🔧 Éliminer les doublons complets ou par combinaison clé
    data_airbnb_sig_concat = data_airbnb_sig_concat.drop_duplicates(subset=[champ_2, 'date_tele'], keep='last')

    # Trier les données
    data_airbnb_sig_concat_sort = data_airbnb_sig_concat.sort_values(by=[champ_2, 'date_tele'])

    # Exporter les données en csv
    data_airbnb_sig_concat_sort.to_csv(chemin_dossier_sortie + nom_export, index = False, sep=';', decimal=',')
    

liste_villes = ['idf']
liste_shape = ['idf']
liste_champs_1 = ['l_cab']
liste_champs_2 = ['c_cainsee']
champ_3 = 'l_epci'
champ_4 = 'lib' # MGP oui/non
champ_5 = 'Paris' # oui/non
dossier = 'limite_administrative'
VILLE = "idf_TEST"

# liste_villes = ['tokyo']
# liste_shape = ['tokyo']
# liste_champs_1 = ['ADM1_EN']
# liste_champs_2 = ['ADM2_EN']
# champ_3 = None
# champ_4 = None
# champ_5 = None
# dossier = 'shapefile_ville'
# VILLE = "tokyo"

# liste_villes = ['londres']
# liste_shape = ['londres']
# liste_champs_1 = ['NAME']
# liste_champs_2 = ['GSS_CODE']
# champ_3 = None
# champ_4 = None
# champ_5 = None
# dossier = 'shapefile_ville'
# VILLE = "londres"

# liste_villes = ['new_york']
# liste_shape = ['new_york']
# liste_champs_1 = ['boro_name']
# liste_champs_2 = ['boro_code']
# champ_3 = None
# champ_4 = None
# champ_5 = None
# dossier = 'shapefile_ville'
# VILLE = "new_york"

# liste_villes = ['amsterdam','barcelone','berlin','bordeaux','londres','lyon','new_york','idf','paris','tokyo']
# liste_champs_1 = ['Stadsdeel','NOM','Gemeinde_n','nom','NAME','nom','boro_name','l_cab','l_ar','ADM1_EN']
# liste_champs_2 = ['Stadsdeelc','FID_1','Gemeinde_s','ident','GSS_CODE','nomreduit','boro_code','c_cainsee','c_arinsee','ADM2_EN']


for i in range(0,len(liste_villes),1):

    shape = liste_shape[i]
    nom_ville = liste_villes[i]
    nom_ville_sortie = VILLE # Noms des fichiers de sorties
    champ_1 = liste_champs_1[i]
    champ_2 = liste_champs_2[i]

    print(nom_ville,shape,champ_1,champ_2)

    nom_export_na = '/commune_output/DATA_AIRBNB_NA_{}_NET.csv'.format(nom_ville_sortie)
    nom_export = '/commune_output/DATA_AIRBNB_INDICATEURS_{}_NET.csv'.format(nom_ville_sortie)
    shapefile_ville = r'P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\003_Données\INSIDE_AIRBNB\{}\{}.shp'.format(dossier,shape)
    nom_fichier_listings = "listings_{}".format(nom_ville) # début des noms des fichiers avec ville pour récupérer les coordonnées des annonces
    chemin_dossier_listings = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\003_Données\INSIDE_AIRBNB\LISTINGS_IDF_NET' # Chemin du dossier des annonces avec localisation
    chemin_dossier_sortie = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\003_Données\INSIDE_AIRBNB' # Chemin du dossier de sortie

    traitement_listings_data_airbnb_sig(chemin_dossier_listings,chemin_dossier_sortie,shapefile_ville,nom_fichier_listings,champ_1,champ_2,nom_export_na,nom_export,champ_3,champ_4,champ_5,nom_ville)