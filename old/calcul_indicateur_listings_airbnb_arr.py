#coding:utf-8

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from datetime import date
from forex_python.converter import CurrencyRates
import geopandas as gpd

# Ce script permet de traiter, de géolocaliser et de calculer les différents indicateurs sur les données AIRBNB provenant d'INSIDE AIRBNB...


def liste_fichier_prefix(dossier, prefix):
    return [fichier for fichier in os.listdir(dossier) if fichier.startswith(prefix)]

def traitement_reviews_data_airbnb_sig(chemin_dossier_reviews,chemin_dossier_listings,chemin_dossier_sortie,shapefile,nom_fichier_listings,nom_fichier_reviews,arrondissement):
    
    ##### TRAITEMENT LISTINGS #####
    
    data_airbnb_append = []
    
    liste_fichier = liste_fichier_prefix(chemin_dossier_listings,nom_fichier_listings)
    taille_dossier = np.size(liste_fichier)

    for i in range(0,taille_dossier,1):  

        fichier = liste_fichier[i]
        print(fichier)
        print(arrondissement)
            
        read_data_airbnb = pd.read_csv(chemin_dossier_listings+"\\"+ fichier,sep=';')
        data_airbnb = read_data_airbnb[['id','latitude','longitude','ville','date_tele']]
        data_airbnb = data_airbnb.drop_duplicates()
        

        ville_shape = gpd.read_file(shapefile) # lecture du shapefile 
        ville_shape = ville_shape.to_crs(2154)
        points_gdf = gpd.GeoDataFrame(data_airbnb, geometry=gpd.points_from_xy(data_airbnb.longitude, data_airbnb.latitude),crs=4326) # WGS84
        points_gdf = points_gdf.to_crs(2154) # RGF_1993
        
        intersect_shape= gpd.sjoin(points_gdf, ville_shape[[champ_shape, 'geometry']], how='left', predicate='intersects') 
        
        intersect_in_shape = intersect_shape[(intersect_shape[champ_shape]==arrondissement)] # arrondissements à modifier pour les autres ville
        
            
        fig,ax = plt.subplots(figsize=(10,10))
        ville_shape.plot(champ_shape,ax=ax)
        intersect_in_shape.plot(ax=ax)
        
        # intersect_in_shape.to_file(chemin_dossier_sortie + '/pts_shapefile_arrondissements_paris/pts_{}_arr.shp'.format(arrondissement), driver='ESRI Shapefile')
        
        # plt.show()   
    
   
        read_data_airbnb_listings = pd.read_csv(chemin_dossier_listings+"\\"+ fichier,sep=';')
        data_airbnb_temp = read_data_airbnb_listings[['id','latitude','longitude','listing_url','room_type','price','availability_365','calculated_host_listings_count','reviews_per_month','license','last_scraped','ville','date_tele']]
        
        
        if (data_airbnb_temp.size != 0): # permet de passer les mois sans données
            
            data_airbnb_temp['in_shape'] = data_airbnb_temp['id'].isin(intersect_in_shape["id"])
            data_airbnb_temp = data_airbnb_temp[(data_airbnb_temp['in_shape'] == True)]
            
            data_airbnb_temp['date_tele'] = pd.to_datetime(data_airbnb_temp['date_tele'])
            data_airbnb_temp['last_scraped'] = pd.to_datetime(data_airbnb_temp['last_scraped'])
            data_airbnb_temp['price'] = data_airbnb_temp['price'].astype(str)
            data_airbnb_temp['price'] = data_airbnb_temp['price'].str.replace('$','',regex=True)
            data_airbnb_temp['price'] = data_airbnb_temp['price'].str.replace(',','',regex=True)
            data_airbnb_temp['price'] = data_airbnb_temp['price'].astype(float) 
        
            c = CurrencyRates()
            if 'new_york' in data_airbnb_temp.values:
                print('new_york')
                
                def get_rate(x):
                    print(x)
                    try:
                        op = c.get_rate('USD', 'EUR', x)
                        print(op)
                    except Exception as re:
                        print(re)
                        op=None 
                    return op
                
                date_taux_change = data_airbnb_temp['last_scraped'][1]
                print(date_taux_change)
                data_airbnb_temp['date_taux_de_change'] = date_taux_change
                taux_de_change = get_rate(date_taux_change)
                data_airbnb_temp['taux_de_change'] = taux_de_change
                data_airbnb_temp['prix_euro'] = data_airbnb_temp['price']*data_airbnb_temp['taux_de_change']
                
            elif 'londres' in data_airbnb_temp.values:
                print('londres')
                
                def get_rate(x):
                    print(x)
                    try:
                        op = c.get_rate('GBP', 'EUR', x)
                        print(op)
                    except Exception as re:
                        print(re)
                        op=None 
                    return op
                
                date_taux_change = data_airbnb_temp['last_scraped'][1]
                data_airbnb_temp['date_taux_de_change'] = date_taux_change
                taux_de_change = get_rate(date_taux_change)
                data_airbnb_temp['taux_de_change'] = taux_de_change
                data_airbnb_temp['prix_euro'] = data_airbnb_temp['price']*data_airbnb_temp['taux_de_change']
                
            else:
                print('europe')
                
                date_taux_change = 'Deja en euro'
                taux_de_change = 'Deja en euro'
                data_airbnb_temp['prix_euro'] = data_airbnb_temp['price']
            
            ######### availability et prix #########
            data_airbnb_dispo = data_airbnb_temp[(data_airbnb_temp['availability_365'] != 0)]
            data_airbnb_logement_entier = data_airbnb_dispo[(data_airbnb_dispo['room_type'] == 'Entire home/apt')]
            data_airbnb_logement_entier = data_airbnb_logement_entier[['prix_euro','id']]
            data_airbnb_logement_entier_dispo_prix_mean = data_airbnb_logement_entier['prix_euro'].mean()
            data_airbnb_logement_entier_dispo_count = data_airbnb_logement_entier['id'].count()
            
            data_airbnb_sans_hotel_dispo = data_airbnb_dispo[(data_airbnb_dispo['room_type'] != 'Hotel room')]
            data_airbnb_sans_hotel_dispo_count = data_airbnb_sans_hotel_dispo['id'].count()
            
            ######### license #########
            data_airbnb_logement_entier = data_airbnb_temp[(data_airbnb_temp['room_type'] == 'Entire home/apt')]
            
            if 'paris' in data_airbnb_logement_entier.values:
                
                data_airbnb_license = data_airbnb_logement_entier[['license']].astype(str)
                
                data_airbnb_avec_licence_valide = data_airbnb_license['license'].str.startswith('75', na = False)
                data_airbnb_avec_licence_mobilite = data_airbnb_license['license'].str.contains('mobi', na = False)
                data_airbnb_avec_licence_hotel = data_airbnb_license['license'].str.contains('Exempt', na = False)
                data_airbnb_avec_licence_vide = data_airbnb_license['license'].isna()
                
                count_data_airbnb_avec_licence_valide = data_airbnb_avec_licence_valide.value_counts()
                count_data_airbnb_avec_licence_mobilite = data_airbnb_avec_licence_mobilite.value_counts()
                count_data_airbnb_avec_licence_hotel = data_airbnb_avec_licence_hotel.value_counts()
                count_data_airbnb_avec_licence_vide = data_airbnb_avec_licence_vide.value_counts()

                if True in count_data_airbnb_avec_licence_hotel:
                    nbres_licence_hotel = count_data_airbnb_avec_licence_hotel[1]
                else:
                    nbres_licence_hotel = 0
                                
                if True in count_data_airbnb_avec_licence_valide:
                    nbres_licence_valide = count_data_airbnb_avec_licence_valide[1]
                else:
                    nbres_licence_valide = 0
                    
                if True in count_data_airbnb_avec_licence_vide:
                    nbres_licence_vide = count_data_airbnb_avec_licence_vide[1]
                else:
                    nbres_licence_vide = 0
                                                    
                if True in count_data_airbnb_avec_licence_mobilite:
                    nbres_licence_mobilite = count_data_airbnb_avec_licence_mobilite[1]
                else:
                    nbres_licence_mobilite = 0

            else:
                nbres_licence_valide = 0
                nbres_licence_mobilite = 0
                nbres_licence_vide = 0
                nbres_licence_hotel = 0
            
            data_airbnb_avec_licence_autres = data_airbnb_logement_entier.count()
            nbres_licence_autres =  data_airbnb_avec_licence_autres[0] - (nbres_licence_valide + nbres_licence_mobilite + nbres_licence_vide + nbres_licence_hotel)

            ######### type_logement (room_type) #########
            data_airbnb_type_logement = data_airbnb_temp[['room_type','id']]
            count_data_airbnb_type_logement = data_airbnb_type_logement.groupby(by='room_type').count()
            count_data_airbnb_type_logement = count_data_airbnb_type_logement.reset_index()
            count_data_airbnb_type_logement.rename({'id':'nombre','room_type':'type_logement'},axis=1, inplace=True)
            
            condition = count_data_airbnb_type_logement['type_logement'].str.contains('Hotel room', case=False)

            if any(condition):
                hotel = count_data_airbnb_type_logement.loc[count_data_airbnb_type_logement['type_logement'] =='Hotel room', 'nombre'].values[0]
            else:
                
                new_data = {'nombre': 0, 'type_logement': 'Hotel room'}
                new_data = pd.DataFrame([new_data])
                count_data_airbnb_type_logement = pd.concat([count_data_airbnb_type_logement, new_data], ignore_index=True)
                hotel = 0
            
            
            logement_entier = count_data_airbnb_type_logement.loc[count_data_airbnb_type_logement['type_logement'] =='Entire home/apt', 'nombre'].values[0]
        
            chambre_privee = count_data_airbnb_type_logement.loc[count_data_airbnb_type_logement['type_logement'] =='Private room', 'nombre'].values[0]
        
            chambre_partagee = count_data_airbnb_type_logement.loc[count_data_airbnb_type_logement['type_logement'] =='Shared room', 'nombre'].values[0]
            
            base_count = count_data_airbnb_type_logement['nombre'].sum()-hotel
            part_logement_entier = (logement_entier)/base_count*100

            nbre_logement_entier = len(data_airbnb_temp)

            ######### annonces_par_loueur (calculated_host_listings_count) #########
            data_airbnb_sans_hotel = data_airbnb_temp[(data_airbnb_temp['room_type'] != 'Hotel room')]
            data_airbnb_mutli_loueur = data_airbnb_sans_hotel[['calculated_host_listings_count','id']]
            count_data_airbnb_mutli_loueur = data_airbnb_mutli_loueur.groupby(by='calculated_host_listings_count').count()
            count_data_airbnb_mutli_loueur = count_data_airbnb_mutli_loueur.reset_index()

            count_data_airbnb_mutli_loueur_1 = count_data_airbnb_mutli_loueur[(count_data_airbnb_mutli_loueur['calculated_host_listings_count'] == 1)]
            count_data_airbnb_mutli_loueur_1_sum = count_data_airbnb_mutli_loueur_1['id'].sum()

            count_data_airbnb_mutli_loueur_2_9 = count_data_airbnb_mutli_loueur[(count_data_airbnb_mutli_loueur['calculated_host_listings_count'] >= 2) & (count_data_airbnb_mutli_loueur['calculated_host_listings_count'] <= 9)]
            count_data_airbnb_mutli_loueur_2_9_sum = count_data_airbnb_mutli_loueur_2_9['id'].sum()

            count_data_airbnb_mutli_loueur_10_plus = count_data_airbnb_mutli_loueur[(count_data_airbnb_mutli_loueur['calculated_host_listings_count'] >= 10)]
            count_data_airbnb_mutli_loueur_10_plus_sum = count_data_airbnb_mutli_loueur_10_plus['id'].sum()

            count_data_airbnb_mutli_loueur_total = count_data_airbnb_mutli_loueur_1_sum + count_data_airbnb_mutli_loueur_2_9_sum + count_data_airbnb_mutli_loueur_10_plus_sum

            part_annonce_multiloueur = (count_data_airbnb_mutli_loueur_10_plus_sum+count_data_airbnb_mutli_loueur_2_9_sum)/count_data_airbnb_mutli_loueur_total*100

            ######### nbr_commentaires + 1,75 (reviews_per_month) #########
            data_airbnb_nbr_commentaires = data_airbnb_sans_hotel[['reviews_per_month','id']]
            data_airbnb_nbr_commentaires = data_airbnb_nbr_commentaires.fillna(0)
            
            data_airbnb_nbr_commentaires_hotel = data_airbnb_temp[['reviews_per_month','id']]
            data_airbnb_nbr_commentaires_hotel = data_airbnb_nbr_commentaires_hotel.fillna(0)
    
            count_data_airbnb_nbr_commentaires = data_airbnb_nbr_commentaires.groupby(by='reviews_per_month').count()
            count_data_airbnb_nbr_commentaires = count_data_airbnb_nbr_commentaires.reset_index()

            count_data_airbnb_nbr_commentaires_0 = count_data_airbnb_nbr_commentaires[(count_data_airbnb_nbr_commentaires['reviews_per_month'] == 0)]
            count_data_airbnb_nbr_commentaires_0_sum = count_data_airbnb_nbr_commentaires_0['id'].sum()

            count_data_airbnb_nbr_commentaires_0_175 = count_data_airbnb_nbr_commentaires[(count_data_airbnb_nbr_commentaires['reviews_per_month'] > 0) & (count_data_airbnb_nbr_commentaires['reviews_per_month'] < 1.75)]
            count_data_airbnb_nbr_commentaires_0_175_sum = count_data_airbnb_nbr_commentaires_0_175['id'].sum()

            count_data_airbnb_nbr_commentaires_175_plus = count_data_airbnb_nbr_commentaires[(count_data_airbnb_nbr_commentaires['reviews_per_month'] >= 1.75)]
            count_data_airbnb_nbr_commentaires_175_plus_sum = count_data_airbnb_nbr_commentaires_175_plus['id'].sum()

            count_data_airbnb_nbr_commentaires_total = count_data_airbnb_nbr_commentaires_0_sum + count_data_airbnb_nbr_commentaires_0_175_sum + count_data_airbnb_nbr_commentaires_175_plus_sum
            
            count_data_airbnb_nbr_commentaires_total_hotel = data_airbnb_nbr_commentaires_hotel['reviews_per_month'].sum()
        
            part_nbres_commentaires_175 = count_data_airbnb_nbr_commentaires_175_plus_sum/count_data_airbnb_nbr_commentaires_total*100
            
            ######### availability (availability_365) #########
            data_airbnb_availability = data_airbnb_sans_hotel[['availability_365','id']]
            count_data_airbnb_availability = data_airbnb_availability.groupby(by='availability_365').count()
            count_data_airbnb_availability = count_data_airbnb_availability.reset_index()

            count_data_airbnb_availability_0 = count_data_airbnb_availability[(count_data_airbnb_availability['availability_365'] == 0)]
            count_data_airbnb_availability_0_sum = count_data_airbnb_availability_0['id'].sum()

            count_data_airbnb_availability_120 = count_data_airbnb_availability[(count_data_airbnb_availability['availability_365'] > 0) & (count_data_airbnb_availability['availability_365'] < 120)]
            count_data_airbnb_availability_120_sum = count_data_airbnb_availability_120['id'].sum()

            count_data_airbnb_availability_120_plus = count_data_airbnb_availability[(count_data_airbnb_availability['availability_365'] >= 120)]
            count_data_airbnb_availability_120_plus_sum = count_data_airbnb_availability_120_plus['id'].sum()

            count_data_airbnb_availability_total = count_data_airbnb_availability_120_plus_sum + count_data_airbnb_availability_120_sum + count_data_airbnb_availability_0_sum

            perce_jours = count_data_airbnb_availability_120_plus_sum/count_data_airbnb_availability_total*100
            
            ######### prix logement entier (price) #########
            data_airbnb_logement_entier = data_airbnb_temp[(data_airbnb_temp['room_type'] == 'Entire home/apt')]
            data_airbnb_price = data_airbnb_logement_entier[['prix_euro','id']]
            count_data_airbnb_price = data_airbnb_price.groupby(by='prix_euro').count()
            count_data_airbnb_price = count_data_airbnb_price.reset_index()

            count_data_airbnb_price_100 = count_data_airbnb_price[(count_data_airbnb_price['prix_euro'] <= 100)]
            count_data_airbnb_price_100_sum = count_data_airbnb_price_100['id'].sum()

            count_data_airbnb_price_100_plus = count_data_airbnb_price[(count_data_airbnb_price['prix_euro'] > 100)]
            count_data_airbnb_price_100_plus_sum = count_data_airbnb_price_100_plus['id'].sum()

            count_data_airbnb_price_total = count_data_airbnb_price_100_sum + count_data_airbnb_price_100_plus_sum

            count_data_airbnb_price_mean = data_airbnb_price['prix_euro'].mean()

            part_logement_entier_inf_100 = count_data_airbnb_price_100_sum/count_data_airbnb_price_total*100
            part_logement_entier_sup_100 = count_data_airbnb_price_100_plus_sum/count_data_airbnb_price_total*100

            
            table_data_airbnb = pd.DataFrame(columns=['ville','date','nombres_annonces','nbres_chambres_hotels','nombres_annonces_hors_hotels','nbres_logements_entiers',
                                            'nbres_chambres_privees','nbres_chambres_partagees',
                                            'part_de_logements_entiers_(%)','nbres_annonces_dispo_log_365','nbres_annonces_dispo_sauf_hotel_365',
                                            'annonces_par_loueur_(1)','annonces_par_loueur_(2_a_9)','annonces_par_loueur_(10_et_plus)',
                                            'part_annonces_de_multiloueurs_(%)','nbres_commentaires_(0)',
                                            'nbres_commentaires_(0_a_1.75)','nbres_commentaires_(1.75_et_plus)',
                                            'part_de_commentaires_(1.75_et_plus)','nbres_commentaires_total','disponibilite_aucune',
                                            'disponibilite_inf_120_jours','disponibilite_sup_120_jours',
                                            'part_disponibilite_sup_120_jours_(%)','prix_logement_entier_inf_100_euro',
                                            'prix_logement_entier_sup_100_euro','part_logement_entier_inf_100_euro',
                                            'part_logement_entier_sup_100_euro','prix_moyen','prix_moyen_1j_dispo','taux_de_change','date_taux_de_change',
                                            'license_valide','licence_mobilite','licence_vide','licence_hotel','licence_autres'])
            
            table_data_airbnb['date_taux_de_change'] = [date_taux_change]
            table_data_airbnb['taux_de_change'] = [taux_de_change]
            table_data_airbnb['date'] = [data_airbnb_temp['date_tele'].iloc[0]]
            table_data_airbnb['ville'] = [data_airbnb_temp['ville'].iloc[0]]

            table_data_airbnb['nombres_annonces'] = [nbre_logement_entier]
            table_data_airbnb['nbres_chambres_hotels'] = [hotel]
            table_data_airbnb['nombres_annonces_hors_hotels'] = [nbre_logement_entier-hotel]
            table_data_airbnb['nbres_logements_entiers'] = [logement_entier]
            table_data_airbnb['nbres_chambres_privees'] = [chambre_privee]
            table_data_airbnb['nbres_chambres_partagees'] = [chambre_partagee]
            table_data_airbnb['part_de_logements_entiers_(%)'] = [part_logement_entier]
            
            table_data_airbnb['nbres_annonces_dispo_log_365'] = [data_airbnb_logement_entier_dispo_count]
            table_data_airbnb['nbres_annonces_dispo_sauf_hotel_365'] = [data_airbnb_sans_hotel_dispo_count]

            table_data_airbnb['annonces_par_loueur_(1)'] = [count_data_airbnb_mutli_loueur_1_sum]
            table_data_airbnb['annonces_par_loueur_(2_a_9)'] = [count_data_airbnb_mutli_loueur_2_9_sum]
            table_data_airbnb['annonces_par_loueur_(10_et_plus)'] = [count_data_airbnb_mutli_loueur_10_plus_sum]
            table_data_airbnb['part_annonces_de_multiloueurs_(%)'] = [part_annonce_multiloueur]

            table_data_airbnb['nbres_commentaires_(0)'] = [count_data_airbnb_nbr_commentaires_0_sum]
            table_data_airbnb['nbres_commentaires_(0_a_1.75)'] = [count_data_airbnb_nbr_commentaires_0_175_sum]
            table_data_airbnb['nbres_commentaires_(1.75_et_plus)'] = [count_data_airbnb_nbr_commentaires_175_plus_sum]
            table_data_airbnb['part_de_commentaires_(1.75_et_plus)'] = [part_nbres_commentaires_175]
            table_data_airbnb['nbres_commentaires_total'] = ['autre script']

            table_data_airbnb['disponibilite_aucune'] = [count_data_airbnb_availability_0_sum]
            table_data_airbnb['disponibilite_inf_120_jours'] = [count_data_airbnb_availability_120_sum]
            table_data_airbnb['disponibilite_sup_120_jours'] = [count_data_airbnb_availability_120_plus_sum]
            table_data_airbnb['part_disponibilite_sup_120_jours_(%)'] = [perce_jours]

            table_data_airbnb['prix_logement_entier_inf_100_euro'] = [count_data_airbnb_price_100_sum]
            table_data_airbnb['prix_logement_entier_sup_100_euro'] = [count_data_airbnb_price_100_plus_sum]
            table_data_airbnb['part_logement_entier_inf_100_euro'] = [part_logement_entier_inf_100]
            table_data_airbnb['part_logement_entier_sup_100_euro'] = [part_logement_entier_sup_100]
            table_data_airbnb['prix_moyen'] = [count_data_airbnb_price_mean]
            table_data_airbnb['prix_moyen_1j_dispo'] = [data_airbnb_logement_entier_dispo_prix_mean]
            
            table_data_airbnb['license_valide'] = [nbres_licence_valide]
            table_data_airbnb['licence_mobilite'] = [nbres_licence_mobilite]
            table_data_airbnb['licence_vide'] = [nbres_licence_vide]
            table_data_airbnb['licence_hotel'] = [nbres_licence_hotel]
            table_data_airbnb['licence_autres'] = [nbres_licence_autres]
            
            data_airbnb_append.append(table_data_airbnb)
        else:
            next
            
    data_airbnb_sig_concat = pd.concat(data_airbnb_append) # Assemblage des données et dissolution sur une seule entête

    data_airbnb_sig_concat.to_csv(chemin_dossier_sortie + nom_export, index = False, sep=';')


# arrondissement = ['1er', '2e', '3e', '4e', '5e', '6e', '7e', '8e', '9e', '10e', '11e', '12e', '13e','14e', '15e', '16e', '17e', '18e', '19e', '20e']
arrondissement = [ '12e', '13e','14e', '15e', '16e', '17e', '18e', '19e', '20e']

for i in arrondissement:

    arrondissement = i
    champ_shape = 'l_ar' # boro_name # l_ar # nom # Stadsdeel # NOM # Gemeinde_n # NAME
    nom_export = '/new_output/DATA_AIRBNB_PARIS_INDICATEURS_SIG_{}_ARR.csv'.format(i)
    shapefile = r'P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\shapefile_ville\paris.shp'
    nom_fichier_listings = "listings_paris" # début des noms des fichiers avec ville pour récupérer les coordonnées des annonces
    nom_fichier_reviews = "reviews_paris" # début des noms des fichiers avec ville pour les annonces
    chemin_dossier_reviews = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\REVIEWS_AIRBNB' # Chemin du dossier des commentaires
    chemin_dossier_listings = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\LISTINGS_AIRBNB' # Chemin du dossier des annonces avec localisation
    chemin_dossier_sortie = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB' # Chemin du dossier de sortie

    traitement_reviews_data_airbnb_sig(chemin_dossier_reviews,chemin_dossier_listings,chemin_dossier_sortie,shapefile,nom_fichier_listings,nom_fichier_reviews,arrondissement)