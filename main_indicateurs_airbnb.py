#coding:utf-8

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from datetime import date
import geopandas as gpd
import requests
import matplotlib.pyplot as plt
import listings_indicateurs_airbnb

# Ce script permet de traiter, de géolocaliser et de calculer les différents indicateurs sur les données AIRBNB provenant d'INSIDE AIRBNB sur l'ensemble des communes d'idf...

# ----------------------------------------------------
# 🔧 Fonction de traitement des données
# ----------------------------------------------------

def traitement_listings_data_airbnb_sig(chemin_dossier_listings,chemin_dossier_sortie,shapefile_ville,nom_fichier_listings,champ_1,champ_2,nom_export_na,nom_export,champ_3,champ_4,champ_5,nom_ville,chemin_dossier_sortie_graph):
    
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
            
            # Table de travail
            data_airbnb_temp = listings_indicateurs_airbnb.traitement(fichier,chemin_dossier_listings,shapefile_ville,nom_ville,champ_1,champ_2,champ_3,champ_4,champ_5)[0]

        if not data_airbnb_temp.empty:

            # ----------------------------------------------------
            # 1️⃣ Conversion dates, prix et taux 
            # ----------------------------------------------------

            data_airbnb_temp = listings_indicateurs_airbnb.conversion(data_airbnb_temp)

            # ----------------------------------------------------
            # 2️⃣ Prix des annonces
            # ----------------------------------------------------

            prix_moy_1j_dispo_corr_com = listings_indicateurs_airbnb.prix_moyen_1j_dispo_corr_com(data_airbnb_temp, champ_2, afficher_graph=False, dossier_sortie=chemin_dossier_sortie_graph, nom_fichier=fichier)[0]
            prix_moy_1j_dispo_corr = listings_indicateurs_airbnb.prix_moyen_1j_dispo_corr(data_airbnb_temp, champ_2, afficher_graph=False, dossier_sortie=chemin_dossier_sortie_graph, nom_fichier=fichier)[0]
            prix_moy_1j_dispo = listings_indicateurs_airbnb.prix_moyen_1j_dispo(data_airbnb_temp, champ_2, afficher_graph=False, dossier_sortie=chemin_dossier_sortie_graph, nom_fichier=fichier)
            prix_med_1j_dispo = listings_indicateurs_airbnb.prix_median_1j_dispo(data_airbnb_temp, champ_2)
            prix_moyen_log_entier = listings_indicateurs_airbnb.prix_moyen_log(data_airbnb_temp, champ_2, afficher_graph=False, dossier_sortie=chemin_dossier_sortie_graph, nom_fichier=fichier)
            prix_par_chambre = listings_indicateurs_airbnb.prix_chambre(data_airbnb_temp, champ_2)
            prix_logements = listings_indicateurs_airbnb.prix_logement(data_airbnb_temp, champ_2)
            prix_logements_365 = listings_indicateurs_airbnb.prix_logement_365(data_airbnb_temp, champ_2)
            
            # ----------------------------------------------------
            # 3️⃣ Comptage des licences
            # ----------------------------------------------------

            licences = listings_indicateurs_airbnb.licence(data_airbnb_temp, champ_2)

            # ----------------------------------------------------
            # 4️⃣ Nombres d'annonces
            # ----------------------------------------------------

            nbres_annonces_logments = listings_indicateurs_airbnb.nbres_annonces(data_airbnb_temp, champ_2)
            nbres_annonces_dispo_logs_365 = listings_indicateurs_airbnb.nbres_annonces_dispo_log_365(data_airbnb_temp, champ_2)
            nbres_annonces_logs_dispo_sauf_hotel_365 = listings_indicateurs_airbnb.nbres_annonces_dispo_sauf_hotel_365(data_airbnb_temp, champ_2)

            # ----------------------------------------------------
            # 5️⃣ Nombre d’annonces par loueurs
            # ----------------------------------------------------

            annonces_loueurs = listings_indicateurs_airbnb.annonce_loueur(data_airbnb_temp, champ_2)
            annonces_loueurs_365 = listings_indicateurs_airbnb.annonce_loueur_365(data_airbnb_temp, champ_2)

            # ----------------------------------------------------
            # 6️⃣ Nombres de commentaires
            # ----------------------------------------------------

            nbres_commentaires = listings_indicateurs_airbnb.nbre_commentaire(data_airbnb_temp, champ_2)
            nbres_commentaires_12m = listings_indicateurs_airbnb.nbre_commentaire_12m(data_airbnb_temp, champ_2)
            nbres_commentaires_12m_365 = listings_indicateurs_airbnb.nbre_commentaire_12m_365(data_airbnb_temp, champ_2)

            # ----------------------------------------------------
            # 7️⃣ Comptage des disponibilités 
            # ----------------------------------------------------

            disponibilites = listings_indicateurs_airbnb.disponibilite(data_airbnb_temp, champ_2)

            # ----------------------------------------------------
            # 8️⃣ Dates, ville, change et commune
            # ----------------------------------------------------

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

            # ----------------------------------------------------
            # 9️⃣ Assemblage des données
            # ----------------------------------------------------

            # Liste des tables à joindre ensemble
            dataframes = [nom_commune, ville, date_telechargement, nbres_annonces_dispo_logs_365, nbres_annonces_logs_dispo_sauf_hotel_365,
                          licences, nbres_annonces_logments, annonces_loueurs, annonces_loueurs_365, nbres_commentaires, disponibilites, 
                          prix_logements_365, prix_logements, date_taux_change,taux_de_change, nbres_commentaires_12m,nbres_commentaires_12m_365,
                          prix_moy_1j_dispo,prix_moy_1j_dispo_corr,prix_moy_1j_dispo_corr_com,prix_par_chambre, prix_med_1j_dispo,prix_moyen_log_entier]

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
                                                'prix_par_chambre', 'prix_median_1j_dispo','prix_moyen_log',
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
            data_NA.append(listings_indicateurs_airbnb.traitement(fichier,chemin_dossier_listings,shapefile_ville,nom_ville,champ_1,champ_2,champ_3,champ_4,champ_5)[1])
    
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
    
# ----------------------------------------------------
# 1️⃣ Paramètrages pour l'Île de France
# ----------------------------------------------------

liste_villes = ['idf']
liste_shape = ['idf']
liste_champs_1 = ['l_cab']
liste_champs_2 = ['c_cainsee']
champ_3 = 'l_epci'
champ_4 = 'lib' # MGP oui/non
champ_5 = 'Paris' # oui/non
dossier = 'limite_administrative'
VILLE = "idf_TEST"

# ----------------------------------------------------
# 2️⃣ Paramètrages pour Tokyo
# ----------------------------------------------------

# liste_villes = ['tokyo']
# liste_shape = ['tokyo']
# liste_champs_1 = ['ADM1_EN']
# liste_champs_2 = ['ADM2_EN']
# champ_3 = None
# champ_4 = None
# champ_5 = None
# dossier = 'shapefile_ville'
# VILLE = "tokyo"

# ----------------------------------------------------
# 3️⃣ Paramètrages pour Londres
# ----------------------------------------------------

# liste_villes = ['londres']
# liste_shape = ['londres']
# liste_champs_1 = ['NAME']
# liste_champs_2 = ['GSS_CODE']
# champ_3 = None
# champ_4 = None
# champ_5 = None
# dossier = 'shapefile_ville'
# VILLE = "londres"

# ----------------------------------------------------
# 4️⃣ Paramètrages pour New York
# ----------------------------------------------------

# liste_villes = ['new_york']
# liste_shape = ['new_york']
# liste_champs_1 = ['boro_name']
# liste_champs_2 = ['boro_code']
# champ_3 = None
# champ_4 = None
# champ_5 = None
# dossier = 'shapefile_ville'
# VILLE = "new_york"

# ----------------------------------------------------
# 5️⃣ Paramètrages pour d'autres villes
# ----------------------------------------------------

# liste_villes = ['amsterdam','barcelone','berlin','bordeaux','londres','lyon','new_york','idf','paris','tokyo']
# liste_champs_1 = ['Stadsdeel','NOM','Gemeinde_n','nom','NAME','nom','boro_name','l_cab','l_ar','ADM1_EN']
# liste_champs_2 = ['Stadsdeelc','FID_1','Gemeinde_s','ident','GSS_CODE','nomreduit','boro_code','c_cainsee','c_arinsee','ADM2_EN']

# ----------------------------------------------------
# 🔧 Boucle de traitement des indicateurs Airbnb
# ----------------------------------------------------

for i in range(0,len(liste_villes),1):

    shape = liste_shape[i]
    nom_ville = liste_villes[i]
    nom_ville_sortie = VILLE # Noms des fichiers de sorties
    champ_1 = liste_champs_1[i]
    champ_2 = liste_champs_2[i]

    print(nom_ville,shape,champ_1,champ_2)

    nom_export_na = '/SORTIE/DATA_AIRBNB_NA_{}_NET.csv'.format(nom_ville_sortie)
    nom_export = '/SORTIE/DATA_AIRBNB_INDICATEURS_{}_NET.csv'.format(nom_ville_sortie)
    shapefile_ville = r'P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\003_Données\INSIDE_AIRBNB\{}\{}.shp'.format(dossier,shape)
    nom_fichier_listings = "listings_{}".format(nom_ville) # début des noms des fichiers avec ville pour récupérer les coordonnées des annonces
    chemin_dossier_listings = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\003_Données\INSIDE_AIRBNB\LISTINGS_IDF_NET' # Chemin du dossier des annonces avec localisation
    chemin_dossier_sortie = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\003_Données\INSIDE_AIRBNB' # Chemin du dossier de sortie
    chemin_dossier_sortie_graph = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\003_Données\INSIDE_AIRBNB\SORTIE\graph'

    traitement_listings_data_airbnb_sig(chemin_dossier_listings,chemin_dossier_sortie,shapefile_ville,nom_fichier_listings,champ_1,champ_2,nom_export_na,nom_export,champ_3,champ_4,champ_5,nom_ville,chemin_dossier_sortie_graph)