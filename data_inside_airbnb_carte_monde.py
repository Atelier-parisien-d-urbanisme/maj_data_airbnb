#coding:utf-8

import pandas as pd
import numpy as np
import requests
import gzip
import os
import matplotlib.pyplot as plt
# from babel.numbers import format_currency
import arcpy
# from currency_converter import CurrencyConverter
import datetime
from datetime import date
from forex_python.converter import CurrencyRates

# Ce script permet d'extraire, traiter et calculer les différents indicateurs sur les données AIRBNB provenant d'INSIDE AIRBNB...

chemin_dossier = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\DATA_CARTE_ANNONCE_MONDE' # Chemin du dossier


def traitement_data_airbnb_monde(nom_fichier):

    arcpy.env.overwriteOutput = True # Attention permet d'écraser fichier du même nom
    nom_gdb = "AIRBNB_MONDE.gdb"
    arcpy.CreateFileGDB_management(chemin_dossier,nom_gdb)
    arcpy.env.workspace = chemin_dossier + "\\" + nom_gdb # Localisation du workspace
    
    arcpy.MakeFeatureLayer_management(r'\\zsfa\ZSF-APUR\SIG\12_BDPPC\maj_pc\script\connexions_sde\utilisateur.sde\apur.diffusion.limite_administrative\apur.diffusion.arrondissement','arrondissement_lyr')
    arcpy.SelectLayerByAttribute_management('arrondissement_lyr','NEW_SELECTION','c_ar IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)')
    arcpy.CopyFeatures_management('arrondissement_lyr','paris')
    
    liste_fichier = os.listdir(chemin_dossier)
    

    for fichier in liste_fichier:  # Lire et stocker les fichiers *.csv dans une seule fichier
        if fichier.startswith(nom_fichier):
            
            print(fichier)
            
            data_airbnb_read_paris = pd.read_csv(chemin_dossier+"\\"+ fichier)
            data_airbnb_paris = data_airbnb_read_paris[['id','latitude','longitude','listing_url','room_type','price','availability_365','calculated_host_listings_count','reviews_per_month','license','last_scraped','ville','date_tele']]
            
            data_airbnb_paris['date_tele'] = pd.to_datetime(data_airbnb_paris['date_tele'])
            data_airbnb_paris['last_scraped'] = pd.to_datetime(data_airbnb_paris['last_scraped'])
            data_airbnb_paris['price'] = data_airbnb_paris['price'].astype(str)
            data_airbnb_paris['price'] = data_airbnb_paris['price'].str.replace('$','',regex=True)
            data_airbnb_paris['price'] = data_airbnb_paris['price'].str.replace(',','',regex=True)
            data_airbnb_paris['price'] = data_airbnb_paris['price'].astype(float)
            
            data_airbnb_paris['prix_euro'] = data_airbnb_paris['price']
            
            data_airbnb_paris = data_airbnb_paris[(data_airbnb_paris['room_type'] != 'Hotel room')]
            
            data_airbnb_paris.to_csv(chemin_dossier + "/sig_{}".format(fichier), index = False, sep=';')
            
            nom = "sig_{}".format(fichier).replace('.csv','')
            print(nom)
            print("Conversion en table...")
            arcpy.TableToTable_conversion(chemin_dossier + "/sig_{}".format(fichier), chemin_dossier + "\\" + nom_gdb, nom)
            
            arcpy.env.workspace = chemin_dossier + "\\" + nom_gdb # Localisation du workspace
            print("Conversion en points...")
            arcpy.management.XYTableToPoint(nom,nom +"_XY",x_field="longitude_D",y_field="latitude_D",z_field=None,coordinate_system='GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision')

            # arcpy.MakeFeatureLayer_management(nom +"_XY",nom +"_XY_lyr")
            # arcpy.SelectLayerByLocation_management(nom +"_XY_lyr",'INTERSECT','arrondissement')
            # arcpy.CopyFeatures_management(nom +"_XY_lyr",nom +"_XY_arr_{}".format(arrondissement))
            
            # arcpy.conversion.ExportTable(nom +"_XY_arr_{}".format(arrondissement),chemin_dossier + "\\" + nom +"_arr_{}.csv".format(arrondissement),"","NOT_USE_ALIAS","",None)
        

nom_fichier = "listings"
traitement_data_airbnb_monde(nom_fichier)


def traitement_data_airbnb_reviews_paris_sig(nom_fichier,nom_fichier_reviews):
        
    liste_fichier = os.listdir(chemin_dossier)
    
    data_airbnb_paris_sig = []

    for fichier in liste_fichier:  # Lire et stocker les fichiers *.csv dans une seule fichier
        if fichier.startswith(nom_fichier):
            
            print(fichier)
            
            data_airbnb_read_paris = pd.read_csv(chemin_dossier+"\\"+ fichier)
            data_airbnb_paris = data_airbnb_read_paris[['id','latitude','longitude','ville']]
            
            data_airbnb_paris_sig.append(data_airbnb_paris)
            
    data_airbnb_paris_sig_concat = pd.concat(data_airbnb_paris_sig)
    data_airbnb_paris_sig_concat_groupby = data_airbnb_paris_sig_concat.groupby(['id','latitude','longitude']).count()
    data_airbnb_paris_sig_concat_groupby_reset = data_airbnb_paris_sig_concat_groupby.reset_index()
    print(data_airbnb_paris_sig_concat_groupby_reset)
            
    data_airbnb_paris_sig_concat_groupby_reset.to_csv(chemin_dossier + "/reviews_sig.csv", index = False, sep=';')

    liste_arrondissements = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    
    for arrondissement in liste_arrondissements:
        
        print('arrondiessment:',arrondissement)
        arcpy.env.overwriteOutput = True # Attention permet d'écraser fichier du même nom
        nom_gdb = "AIRBNB_PARIS_REVIEWS_ARR_{}.gdb".format(arrondissement)
        arcpy.CreateFileGDB_management(chemin_dossier,nom_gdb)
        arcpy.env.workspace = chemin_dossier + "\\" + nom_gdb # Localisation du workspace
        arcpy.MakeFeatureLayer_management(r'\\zsfa\ZSF-APUR\SIG\12_BDPPC\maj_pc\script\connexions_sde\utilisateur.sde\apur.diffusion.limite_administrative\apur.diffusion.arrondissement','arrondissement_lyr')
        # arcpy.SelectLayerByAttribute_management('arrondissement_lyr','NEW_SELECTION','c_ar IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)')
        arcpy.SelectLayerByAttribute_management('arrondissement_lyr','NEW_SELECTION','c_ar IN ({})'.format(arrondissement))
        arcpy.CopyFeatures_management('arrondissement_lyr','arrondissement')
             
        print("Conversion en table...")
        arcpy.TableToTable_conversion(chemin_dossier + "/reviews_sig.csv", chemin_dossier + "\\" + nom_gdb, "reviews_sig")
        
        arcpy.env.workspace = chemin_dossier + "\\" + nom_gdb # Localisation du workspace
        print("Conversion en points...")
        arcpy.management.XYTableToPoint("reviews_sig","reviews_sig_XY",x_field="longitude",y_field="latitude",z_field=None,coordinate_system='GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision')

        arcpy.MakeFeatureLayer_management("reviews_sig_XY","reviews_sig_XY_lyr")
        arcpy.SelectLayerByLocation_management("reviews_sig_XY_lyr",'INTERSECT','arrondissement')
        arcpy.CopyFeatures_management("reviews_sig_XY_lyr","reviews_sig_XY_Paris_{}_arr".format(arrondissement))
        
        arcpy.conversion.TableToTable("reviews_sig_XY_Paris_{}_arr".format(arrondissement),chemin_dossier,"reviews_sig_XY_Paris_{}_arr.csv".format(arrondissement))
        
        data_airbnb_read_paris_reviews_sig = pd.read_csv(chemin_dossier + "\\" + "reviews_sig_XY_Paris_{}_arr.csv".format(arrondissement), sep=';')
        data_airbnb_paris_reveiews = data_airbnb_read_paris_reviews_sig[['id','latitude','longitude']]
        data_airbnb_paris_reveiews['id'] = data_airbnb_paris_reveiews['id'].astype(str)
        data_airbnb_paris_reveiews['id'] = data_airbnb_paris_reveiews['id'].str.replace(',','.',regex=True)
        data_airbnb_paris_reveiews['id'] = data_airbnb_paris_reveiews['id'].astype(float)
        data_airbnb_paris_reveiews['id'] = data_airbnb_paris_reveiews['id'].astype(int)
        print(data_airbnb_paris_reveiews)
    
        data_airbnb_reviews_paris = []
        
        for fichier in liste_fichier:  # Lire et stocker les fichiers *.csv dans une seule fichier
            if fichier.startswith(nom_fichier_reviews):
                print(fichier)
                data_airbnb_read_paris = pd.read_csv(chemin_dossier+"\\"+ fichier,sep=';')
                data_airbnb_paris = data_airbnb_read_paris[['listing_id','id','date','reviewer_id','reviewer_name','ville','date_tele']]
                    
                data_airbnb_reviews_paris.append(data_airbnb_paris)
                
        data_airbnb_reviews_paris_concat = pd.concat(data_airbnb_reviews_paris)
        print(data_airbnb_reviews_paris_concat)
       
        data_airbnb_reviews_paris_concat_dop = data_airbnb_reviews_paris_concat.drop_duplicates()
        data_airbnb_reviews_paris_concat_dop['in_paris'] = data_airbnb_reviews_paris_concat_dop['listing_id'].isin(data_airbnb_paris_reveiews["id"])
        
        data_airbnb_reviews_paris_concat_dop_sel = data_airbnb_reviews_paris_concat_dop[(data_airbnb_reviews_paris_concat_dop['in_paris'] == True)]
        print(data_airbnb_reviews_paris_concat_dop_sel)
        
        data_airbnb_reviews_paris_concat_dop_sel_groupby = data_airbnb_reviews_paris_concat_dop_sel.groupby(['listing_id','id','date']).count()
        print(data_airbnb_reviews_paris_concat_dop_sel_groupby)
        data_airbnb_reviews_paris_concat_dop_groupby_reset = data_airbnb_reviews_paris_concat_dop_sel_groupby.reset_index()
        print(data_airbnb_reviews_paris_concat_dop_groupby_reset)
        
        data_airbnb_reviews_paris_concat_dop_groupby_reset = data_airbnb_reviews_paris_concat_dop_groupby_reset[['listing_id','id','date']]
        print(data_airbnb_reviews_paris_concat_dop_groupby_reset)
        
        data_airbnb_reviews_paris_concat_dop_groupby_reset['date'] = pd.to_datetime(data_airbnb_reviews_paris_concat_dop_groupby_reset['date']) # Conversion du champ date
        data_airbnb_reviews_paris_concat_dop_groupby_reset['mois'] = data_airbnb_reviews_paris_concat_dop_groupby_reset['date'].dt.month
        data_airbnb_reviews_paris_concat_dop_groupby_reset['annee'] = data_airbnb_reviews_paris_concat_dop_groupby_reset['date'].dt.year
        print(data_airbnb_reviews_paris_concat_dop_groupby_reset)
        
        data_airbnb_reviews_paris_concat_dop_groupby_reset_count = data_airbnb_reviews_paris_concat_dop_groupby_reset.groupby(['mois','annee']).count()
        data_airbnb_reviews_paris_concat_dop_groupby_reset_count_reset = data_airbnb_reviews_paris_concat_dop_groupby_reset_count.reset_index()
        data_airbnb_reviews_paris_concat_dop_groupby_reset_count_reset = data_airbnb_reviews_paris_concat_dop_groupby_reset_count_reset[['mois','annee','listing_id']]
        data_airbnb_reviews_paris_concat_dop_groupby_reset_count_reset.rename({'listing_id':'nbres_commentaires'},axis=1, inplace=True)
        print(data_airbnb_reviews_paris_concat_dop_groupby_reset_count_reset)
        
        data_airbnb_reviews_paris_concat_dop_groupby_reset_count_reset.to_csv(chemin_dossier+"/count_all_reviews_paris_sig_{}_arr.csv".format(arrondissement),index = True, sep=';')     
            
nom_fichier = "listings_paris" 
nom_fichier_reviews = "reviews_paris"
# traitement_data_airbnb_reviews_paris_sig(nom_fichier,nom_fichier_reviews)

def traitement_data_airbnb_reviews(chemin_dossier,nom_fichier_reviews):
    
    liste_fichier = os.listdir(chemin_dossier)
    
    data_airbnb_reviews = []
    
    for fichier in liste_fichier:  # Lire et stocker les fichiers *.csv dans une seule fichier
        if fichier.startswith(nom_fichier_reviews):
            
            print(fichier)
            data_airbnb_read = pd.read_csv(chemin_dossier+"\\"+ fichier,sep=';')
            data_airbnb = data_airbnb_read[['listing_id','id','date','reviewer_id','comments','reviewer_name','ville','date_tele']]
                
            data_airbnb_reviews.append(data_airbnb)
            
    data_airbnb_reviews_concat = pd.concat(data_airbnb_reviews)
    print(data_airbnb_reviews_concat)
    
    data_airbnb_reviews_concat['com_auto'] = data_airbnb_reviews_concat['comments'].str.contains('This is an automated posting') 
    print(data_airbnb_reviews_concat)
    
    data_airbnb_reviews_concat_sel = data_airbnb_reviews_concat[(data_airbnb_reviews_concat['com_auto'] == False)]
    print(data_airbnb_reviews_concat_sel)
    
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
    
    data_airbnb_reviews_concat_sel_groupby_count = data_airbnb_reviews_concat_sel_groupby.groupby(['mois','annee']).count()
    data_airbnb_reviews_concat_sel_groupby_count = data_airbnb_reviews_concat_sel_groupby_count.reset_index()
    data_airbnb_reviews_concat_sel_groupby_count = data_airbnb_reviews_concat_sel_groupby_count[['mois','annee','listing_id']]
    data_airbnb_reviews_concat_sel_groupby_count.rename({'listing_id':'nbres_commentaires'},axis=1, inplace=True)
    data_airbnb_reviews_concat_sel_groupby_count_sort = data_airbnb_reviews_concat_sel_groupby_count.sort_values(by=['annee', 'mois'])
    print(data_airbnb_reviews_concat_sel_groupby_count_sort)
    
    data_airbnb_reviews_concat_sel_groupby_count_sort.to_csv(chemin_dossier+"/nbres_commentaires.csv",index = True, sep=';')   
                          
nom_fichier_reviews = "reviews_paris" 
# traitement_data_airbnb_reviews(chemin_dossier,nom_fichier_reviews)



# plt.plot(table_data_airbnb_ville_date['date'],table_data_airbnb_ville_date['nombres_annonces'],'-o',label='ÉVOLUTION DU NOMBRE D ANNONCES PROPOSÉES À LA LOCATION SUR AIRBNB')
# plt.plot(table_data_airbnb_ville_date['date'],table_data_airbnb_ville_date['nbres_commentaires_total'],'-o',label='ÉVOLUTION DU NOMBRE DE COMMENTAIRES LAISSÉS CHAQUE MOIS SUR AIRBNB')
# plt.legend()
# plt.show()


def traitement_data_airbnb(nom_fichier):
    
    liste_fichier = os.listdir(chemin_dossier)
    table_airbnb = []

    for fichier in liste_fichier:  # Lire et stocker les fichiers *.csv dans une seule fichier
        if fichier.startswith(nom_fichier):
            print(fichier)
            data_airbnb_read= pd.read_csv(chemin_dossier+"\\"+ fichier)
            data_airbnb = data_airbnb_read[['id','latitude','longitude','listing_url','room_type','price',
                                            'availability_365','calculated_host_listings_count',
                                            'reviews_per_month','license','last_scraped','ville','date_tele']]
            
            data_airbnb['date_tele'] = pd.to_datetime(data_airbnb['date_tele'])
            data_airbnb['last_scraped'] = pd.to_datetime(data_airbnb['last_scraped'])
            data_airbnb['price'] = data_airbnb['price'].astype(str)
            data_airbnb['price'] = data_airbnb['price'].str.replace('$','',regex=True)
            data_airbnb['price'] = data_airbnb['price'].str.replace(',','',regex=True)
            data_airbnb['price'] = data_airbnb['price'].astype(float)
            
            c = CurrencyRates()
            def get_rate(x):
                print(x)
                try:
                    op = c.get_rate('USD', 'EUR', x)
                    print(op)
                except Exception as re:
                    print(re)
                    op=None 
                return op
            
            date_taux_change = data_airbnb['last_scraped'][0]
            taux_de_change = get_rate(date_taux_change)
            data_airbnb['taux_de_change'] = taux_de_change
            data_airbnb['prix_euro'] = data_airbnb['price']*data_airbnb['taux_de_change']
            
            # print("=====>",datetime.date(data_airbnb.last_scraped.dt.year[0],data_airbnb.last_scraped.dt.month[0],data_airbnb.last_scraped.dt.day[0]))
            # c = CurrencyConverter()
            # taux_change = c.convert(1, 'USD', 'EUR',datetime.date(2010,3,5))
            # print(taux_change)

            ######### license #########
            data_airbnb_logement_entier = data_airbnb[(data_airbnb['room_type'] == 'Entire home/apt')]
            if 'paris' in data_airbnb_logement_entier.values:
                
                data_airbnb_license = data_airbnb_logement_entier[['license']]
                
                data_airbnb_avec_licence_valide = data_airbnb_license['license'].str.contains('75', na = False)
                data_airbnb_avec_licence_mobilite = data_airbnb_license['license'].str.contains('mobility', na = False)
                data_airbnb_avec_licence_vide = data_airbnb_license['license'].str.startswith('', na = False)
                data_airbnb_avec_licence_hotel = data_airbnb_license['license'].str.contains('Exempt', na = False)
                
                count_data_airbnb_avec_licence_valide = data_airbnb_avec_licence_valide.value_counts()
                count_data_airbnb_avec_licence_mobilite = data_airbnb_avec_licence_mobilite.value_counts()
                count_data_airbnb_avec_licence_vide = data_airbnb_avec_licence_vide.value_counts()
                count_data_airbnb_avec_licence_hotel = data_airbnb_avec_licence_hotel.value_counts()

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
            
            ######### type_logement (room_type) #########
            data_airbnb_type_logement = data_airbnb[['room_type','id']]
            count_data_airbnb_type_logement = data_airbnb_type_logement.groupby(by='room_type').count()
            count_data_airbnb_type_logement = count_data_airbnb_type_logement.reset_index()
            count_data_airbnb_type_logement.rename({'id':'nombre','room_type':'type_logement'},axis=1, inplace=True)

            hotel = count_data_airbnb_type_logement[(count_data_airbnb_type_logement['type_logement'] =='Hotel room')]
            logement_entier = count_data_airbnb_type_logement[(count_data_airbnb_type_logement['type_logement'] =='Entire home/apt')]
            logement_entier = logement_entier['nombre'][0]

            chambre_privee = count_data_airbnb_type_logement[(count_data_airbnb_type_logement['type_logement'] =='Private room')]
            chambre_privee = chambre_privee['nombre'][2]

            chambre_partagee = count_data_airbnb_type_logement[(count_data_airbnb_type_logement['type_logement'] =='Shared room')]
            chambre_partagee = chambre_partagee['nombre'][3]

            base_count = count_data_airbnb_type_logement['nombre'].sum()-hotel['nombre'][1]
            part_logement_entier = (logement_entier)/base_count*100

            nbre_logement_entier = len(data_airbnb)

            ######### annonces_par_loueur (calculated_host_listings_count) #########
            data_airbnb_sans_hotel = data_airbnb[(data_airbnb['room_type'] != 'Hotel room')]
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
            
            data_airbnb_nbr_commentaires_hotel = data_airbnb[['reviews_per_month','id']]
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
            data_airbnb_logement_entier = data_airbnb[(data_airbnb['room_type'] == 'Entire home/apt')]
            data_airbnb_price = data_airbnb_logement_entier[['prix_euro','id']]
            count_data_airbnb_price = data_airbnb_price.groupby(by='prix_euro').count()
            count_data_airbnb_price = count_data_airbnb_price.reset_index()

            count_data_airbnb_price_100 = count_data_airbnb_price[(count_data_airbnb_price['prix_euro'] <= 100)]
            count_data_airbnb_price_100_sum = count_data_airbnb_price_100['id'].sum()

            count_data_airbnb_price_100_plus = count_data_airbnb_price[(count_data_airbnb_price['prix_euro'] > 100)]
            count_data_airbnb_price_100_plus_sum = count_data_airbnb_price_100_plus['id'].sum()

            count_data_airbnb_price_total = count_data_airbnb_price_100_sum + count_data_airbnb_price_100_plus_sum

            count_data_airbnb_price_sum = data_airbnb_price['prix_euro'].mean()

            part_logement_entier_inf_100 = count_data_airbnb_price_100_sum/count_data_airbnb_price_total*100
            part_logement_entier_sup_100 = count_data_airbnb_price_100_plus_sum/count_data_airbnb_price_total*100

            
            table_data_airbnb = pd.DataFrame(columns=['ville','date','nombres_annonces','nbres_logements_entiers',
                                            'nbres_chambres_privees','nbres_chambres_partagees',
                                            'part_de_logements_entiers_(%)','annonces_par_loueur_(1)',
                                            'annonces_par_loueur_(2_à_9)','annonces_par_loueur_(10_et_plus)',
                                            'part_annonces_de_multiloueurs_(%)','nbres_commentaires_(0)',
                                            'nbres_commentaires_(0_à_1.75)','nbres_commentaires_(1.75_et_plus)',
                                            'part_de_commentaires_(1.75_et_plus)','nbres_commentaires_total','disponibilite_aucune',
                                            'disponibilite_inf_120_jours','disponibilite_sup_120_jours',
                                            'part_disponibilite_sup_120_jours_(%)','prix_logement_entier_inf_100€',
                                            'prix_logement_entier_sup_100€','part_logement_entier_inf_100€',
                                            'part_logement_entier_sup_100€','prix_moyen','taux_de_change','date_taux_de_change',
                                            'license_valide','licence_mobilite','licence_vide','licence_hotel'])
            
            
            table_data_airbnb['date_taux_de_change'] = [date_taux_change]
            table_data_airbnb['taux_de_change'] = [taux_de_change]
            table_data_airbnb['date'] = [data_airbnb['date_tele'][0]]
            table_data_airbnb['ville'] = [data_airbnb['ville'][0]]

            table_data_airbnb['nombres_annonces'] = [nbre_logement_entier]
            table_data_airbnb['nbres_logements_entiers'] = [logement_entier]
            table_data_airbnb['nbres_chambres_privees'] = [chambre_privee]
            table_data_airbnb['nbres_chambres_partagees'] = [chambre_partagee]
            table_data_airbnb['part_de_logements_entiers_(%)'] = [part_logement_entier]

            table_data_airbnb['annonces_par_loueur_(1)'] = [count_data_airbnb_mutli_loueur_1_sum]
            table_data_airbnb['annonces_par_loueur_(2_à_9)'] = [count_data_airbnb_mutli_loueur_2_9_sum]
            table_data_airbnb['annonces_par_loueur_(10_et_plus)'] = [count_data_airbnb_mutli_loueur_10_plus_sum]
            table_data_airbnb['part_annonces_de_multiloueurs_(%)'] = [part_annonce_multiloueur]

            table_data_airbnb['nbres_commentaires_(0)'] = [count_data_airbnb_nbr_commentaires_0_sum]
            table_data_airbnb['nbres_commentaires_(0_à_1.75)'] = [count_data_airbnb_nbr_commentaires_0_175_sum]
            table_data_airbnb['nbres_commentaires_(1.75_et_plus)'] = [count_data_airbnb_nbr_commentaires_175_plus_sum]
            table_data_airbnb['part_de_commentaires_(1.75_et_plus)'] = [part_nbres_commentaires_175]
            table_data_airbnb['nbres_commentaires_total'] = [count_data_airbnb_nbr_commentaires_total_hotel]

            table_data_airbnb['disponibilite_aucune'] = [count_data_airbnb_availability_0_sum]
            table_data_airbnb['disponibilite_inf_120_jours'] = [count_data_airbnb_availability_120_sum]
            table_data_airbnb['disponibilite_sup_120_jours'] = [count_data_airbnb_availability_120_plus_sum]
            table_data_airbnb['part_disponibilite_sup_120_jours_(%)'] = [perce_jours]

            table_data_airbnb['prix_logement_entier_inf_100€'] = [count_data_airbnb_price_100_sum]
            table_data_airbnb['prix_logement_entier_sup_100€'] = [count_data_airbnb_price_100_plus_sum]
            table_data_airbnb['part_logement_entier_inf_100€'] = [part_logement_entier_inf_100]
            table_data_airbnb['part_logement_entier_sup_100€'] = [part_logement_entier_sup_100]
            table_data_airbnb['prix_moyen'] = [count_data_airbnb_price_sum]
            
            table_data_airbnb['license_valide'] = [nbres_licence_valide]
            table_data_airbnb['licence_mobilite'] = [nbres_licence_mobilite]
            table_data_airbnb['licence_vide'] = [nbres_licence_vide]
            table_data_airbnb['licence_hotel'] = [nbres_licence_hotel]
            
            table_airbnb.append(table_data_airbnb)
            
    table_data_airbnb_ville_date = pd.concat(table_airbnb) # Assemblage des données et dissolution sur une seule entête
    print(table_data_airbnb_ville_date)

    table_data_airbnb_ville_date.to_csv(chemin_dossier +"/DATA_AIRBNB.csv", index = False, sep=';')

nom_fichier = "listings" 
# traitement_data_airbnb(nom_fichier)

def traitement_data_airbnb_paris_arrondissment(nom_fichier):
    
    liste_arrondissements = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    
    arcpy.env.overwriteOutput = True # Attention permet d'écraser fichier du même nom
    nom_gdb = "AIRBNB_PARIS.gdb".format(arrondissement)
    arcpy.CreateFileGDB_management(chemin_dossier,nom_gdb)
    arcpy.env.workspace = chemin_dossier + "\\" + nom_gdb # Localisation du workspace
    
    for arrondissement in liste_arrondissements:
        
        print('arrondiessment:',arrondissement)
        arcpy.MakeFeatureLayer_management(r'\\zsfa\ZSF-APUR\SIG\12_BDPPC\maj_pc\script\connexions_sde\utilisateur.sde\apur.diffusion.limite_administrative\apur.diffusion.arrondissement','arrondissement_lyr')
        # arcpy.SelectLayerByAttribute_management('arrondissement_lyr','NEW_SELECTION','c_ar IN (1, 2, 3, 4, 9, 10, 11)')
        # arcpy.SelectLayerByAttribute_management('arrondissement_lyr','NEW_SELECTION','c_ar IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)')
        arcpy.SelectLayerByAttribute_management('arrondissement_lyr','NEW_SELECTION','c_ar IN ({})'.format(arrondissement))
        arcpy.CopyFeatures_management('arrondissement_lyr','arrondissement')
        
        liste_fichier = os.listdir(chemin_dossier)
        
        data_airbnb_paris_sig = []

        for fichier in liste_fichier:  # Lire et stocker les fichiers *.csv dans une seule fichier
            if fichier.startswith(nom_fichier):
                
                print(fichier)
                
                data_airbnb_read_paris = pd.read_csv(chemin_dossier+"\\"+ fichier)
                data_airbnb_paris = data_airbnb_read_paris[['id','latitude','longitude','listing_url','room_type','price','availability_365','calculated_host_listings_count','reviews_per_month','license','last_scraped','ville','date_tele']]
                
                data_airbnb_paris['date_tele'] = pd.to_datetime(data_airbnb_paris['date_tele'])
                data_airbnb_paris['last_scraped'] = pd.to_datetime(data_airbnb_paris['last_scraped'])
                data_airbnb_paris['price'] = data_airbnb_paris['price'].astype(str)
                data_airbnb_paris['price'] = data_airbnb_paris['price'].str.replace('$','',regex=True)
                data_airbnb_paris['price'] = data_airbnb_paris['price'].str.replace(',','',regex=True)
                data_airbnb_paris['price'] = data_airbnb_paris['price'].astype(float)
                
                # c = CurrencyRates()
                # def get_rate(x):
                #     print(x)
                #     try:
                #         op = c.get_rate('USD', 'EUR', x)
                #         print(op)
                #     except Exception as re:
                #         print(re)
                #         op=None 
                #     return op
                
                # date_taux_change = data_airbnb_paris['last_scraped'][0]
                # data_airbnb_paris['date_taux_de_change'] = date_taux_change
                # taux_de_change = get_rate(date_taux_change)
                # data_airbnb_paris['taux_de_change'] = taux_de_change
                # data_airbnb_paris['prix_euro'] = data_airbnb_paris['price']*data_airbnb_paris['taux_de_change']
                data_airbnb_paris['prix_euro'] = data_airbnb_paris['price']
                
                data_airbnb_paris.to_csv(chemin_dossier + "/sig_{}".format(fichier), index = False, sep=';')
                
                nom = "sig_{}".format(fichier).replace('.csv','')
                print(nom)
                print("Conversion en table...")
                arcpy.TableToTable_conversion(chemin_dossier + "/sig_{}".format(fichier), chemin_dossier + "\\" + nom_gdb, nom)
                
                arcpy.env.workspace = chemin_dossier + "\\" + nom_gdb # Localisation du workspace
                print("Conversion en points...")
                arcpy.management.XYTableToPoint(nom,nom +"_XY",x_field="longitude_D",y_field="latitude_D",z_field=None,coordinate_system='GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision')

                arcpy.MakeFeatureLayer_management(nom +"_XY",nom +"_XY_lyr")
                arcpy.SelectLayerByLocation_management(nom +"_XY_lyr",'INTERSECT','arrondissement')
                arcpy.CopyFeatures_management(nom +"_XY_lyr",nom +"_XY_arr_{}".format(arrondissement))
                
                arcpy.conversion.ExportTable(nom +"_XY_arr_{}".format(arrondissement),chemin_dossier + "\\" + nom +"_arr_{}.csv".format(arrondissement),"","NOT_USE_ALIAS","",None)
                    
                data_airbnb_paris_1_6 = pd.read_csv(chemin_dossier + "\\" + nom +"_arr_{}.csv".format(arrondissement), sep=';')
                # data_airbnb_paris_1_6 = data_airbnb_read_paris_1_6[['id','latitude_D','longitude_D','listing_url','room_type','price',
                #                                 'availability_365','calculated_host_listings_count','reviews_per_month','license','last_scraped','ville','date_tele','prix_euro']]
               
                ######### license #########
                data_airbnb_logement_entier = data_airbnb_paris_1_6[(data_airbnb_paris_1_6['room_type'] == 'Entire home/apt')]
                if 'paris' in data_airbnb_logement_entier.values:
                    
                    data_airbnb_license = data_airbnb_logement_entier[['license']]
                    
                    data_airbnb_avec_licence_valide = data_airbnb_license['license'].str.contains('75', na = False)
                    data_airbnb_avec_licence_mobilite = data_airbnb_license['license'].str.contains('mobility', na = False)
                    data_airbnb_avec_licence_vide = data_airbnb_license['license'].str.startswith('', na = False)
                    data_airbnb_avec_licence_hotel = data_airbnb_license['license'].str.contains('Exempt', na = False)
                    
                    count_data_airbnb_avec_licence_valide = data_airbnb_avec_licence_valide.value_counts()
                    count_data_airbnb_avec_licence_mobilite = data_airbnb_avec_licence_mobilite.value_counts()
                    count_data_airbnb_avec_licence_vide = data_airbnb_avec_licence_vide.value_counts()
                    count_data_airbnb_avec_licence_hotel = data_airbnb_avec_licence_hotel.value_counts()

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
                
                ######### type_logement (room_type) #########
                data_airbnb_type_logement = data_airbnb_paris_1_6[['room_type','id']]
                count_data_airbnb_type_logement = data_airbnb_type_logement.groupby(by='room_type').count()
                count_data_airbnb_type_logement = count_data_airbnb_type_logement.reset_index()
                count_data_airbnb_type_logement.rename({'id':'nombre','room_type':'type_logement'},axis=1, inplace=True)

                hotel = count_data_airbnb_type_logement[(count_data_airbnb_type_logement['type_logement'] =='Hotel room')]
                logement_entier = count_data_airbnb_type_logement[(count_data_airbnb_type_logement['type_logement'] =='Entire home/apt')]
                logement_entier = logement_entier['nombre'][0]

                chambre_privee = count_data_airbnb_type_logement[(count_data_airbnb_type_logement['type_logement'] =='Private room')]
                chambre_privee = chambre_privee['nombre'][2]

                chambre_partagee = count_data_airbnb_type_logement[(count_data_airbnb_type_logement['type_logement'] =='Shared room')]
                chambre_partagee = chambre_partagee['nombre'][3]

                base_count = count_data_airbnb_type_logement['nombre'].sum()-hotel['nombre'][1]
                part_logement_entier = (logement_entier)/base_count*100

                nbre_logement_entier = len(data_airbnb_paris_1_6)

                ######### annonces_par_loueur (calculated_host_listings_count) #########
                data_airbnb_sans_hotel = data_airbnb_paris_1_6[(data_airbnb_paris_1_6['room_type'] != 'Hotel room')]
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
                
                data_airbnb_nbr_commentaires_hotel = data_airbnb_paris_1_6[['reviews_per_month','id']]
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
                data_airbnb_logement_entier = data_airbnb_paris_1_6[(data_airbnb_paris_1_6['room_type'] == 'Entire home/apt')]
                data_airbnb_price = data_airbnb_logement_entier[['prix_euro','id']]
                count_data_airbnb_price = data_airbnb_price.groupby(by='prix_euro').count()
                count_data_airbnb_price = count_data_airbnb_price.reset_index()

                count_data_airbnb_price_100 = count_data_airbnb_price[(count_data_airbnb_price['prix_euro'] <= 100)]
                count_data_airbnb_price_100_sum = count_data_airbnb_price_100['id'].sum()

                count_data_airbnb_price_100_plus = count_data_airbnb_price[(count_data_airbnb_price['prix_euro'] > 100)]
                count_data_airbnb_price_100_plus_sum = count_data_airbnb_price_100_plus['id'].sum()

                count_data_airbnb_price_total = count_data_airbnb_price_100_sum + count_data_airbnb_price_100_plus_sum

                count_data_airbnb_price_sum = data_airbnb_price['prix_euro'].mean()

                part_logement_entier_inf_100 = count_data_airbnb_price_100_sum/count_data_airbnb_price_total*100
                part_logement_entier_sup_100 = count_data_airbnb_price_100_plus_sum/count_data_airbnb_price_total*100

                
                table_data_airbnb = pd.DataFrame(columns=['ville','date','nombres_annonces','nbres_logements_entiers',
                                                'nbres_chambres_privees','nbres_chambres_partagees',
                                                'part_de_logements_entiers_(%)','annonces_par_loueur_(1)',
                                                'annonces_par_loueur_(2_à_9)','annonces_par_loueur_(10_et_plus)',
                                                'part_annonces_de_multiloueurs_(%)','nbres_commentaires_(0)',
                                                'nbres_commentaires_(0_à_1.75)','nbres_commentaires_(1.75_et_plus)',
                                                'part_de_commentaires_(1.75_et_plus)','nbres_commentaires_total','disponibilite_aucune',
                                                'disponibilite_inf_120_jours','disponibilite_sup_120_jours',
                                                'part_disponibilite_sup_120_jours_(%)','prix_logement_entier_inf_100€',
                                                'prix_logement_entier_sup_100€','part_logement_entier_inf_100€',
                                                'part_logement_entier_sup_100€','prix_moyen','taux_de_change','date_taux_de_change',
                                                'license_valide','licence_mobilite','licence_vide','licence_hotel'])
                
                
                # table_data_airbnb['date_taux_de_change'] = [date_taux_change]
                # table_data_airbnb['taux_de_change'] = [taux_de_change]
                table_data_airbnb['date'] = [data_airbnb_paris_1_6['date_tele'][0]]
                # table_data_airbnb['ville'] = [data_airbnb_paris_1_6['ville'][0]]

                table_data_airbnb['nombres_annonces'] = [nbre_logement_entier]
                table_data_airbnb['nbres_logements_entiers'] = [logement_entier]
                table_data_airbnb['nbres_chambres_privees'] = [chambre_privee]
                table_data_airbnb['nbres_chambres_partagees'] = [chambre_partagee]
                table_data_airbnb['part_de_logements_entiers_(%)'] = [part_logement_entier]

                table_data_airbnb['annonces_par_loueur_(1)'] = [count_data_airbnb_mutli_loueur_1_sum]
                table_data_airbnb['annonces_par_loueur_(2_à_9)'] = [count_data_airbnb_mutli_loueur_2_9_sum]
                table_data_airbnb['annonces_par_loueur_(10_et_plus)'] = [count_data_airbnb_mutli_loueur_10_plus_sum]
                table_data_airbnb['part_annonces_de_multiloueurs_(%)'] = [part_annonce_multiloueur]

                table_data_airbnb['nbres_commentaires_(0)'] = [count_data_airbnb_nbr_commentaires_0_sum]
                table_data_airbnb['nbres_commentaires_(0_à_1.75)'] = [count_data_airbnb_nbr_commentaires_0_175_sum]
                table_data_airbnb['nbres_commentaires_(1.75_et_plus)'] = [count_data_airbnb_nbr_commentaires_175_plus_sum]
                table_data_airbnb['part_de_commentaires_(1.75_et_plus)'] = [part_nbres_commentaires_175]
                # table_data_airbnb['nbres_commentaires_total'] = [count_data_airbnb_nbr_commentaires_total_hotel]

                table_data_airbnb['disponibilite_aucune'] = [count_data_airbnb_availability_0_sum]
                table_data_airbnb['disponibilite_inf_120_jours'] = [count_data_airbnb_availability_120_sum]
                table_data_airbnb['disponibilite_sup_120_jours'] = [count_data_airbnb_availability_120_plus_sum]
                table_data_airbnb['part_disponibilite_sup_120_jours_(%)'] = [perce_jours]

                table_data_airbnb['prix_logement_entier_inf_100€'] = [count_data_airbnb_price_100_sum]
                table_data_airbnb['prix_logement_entier_sup_100€'] = [count_data_airbnb_price_100_plus_sum]
                table_data_airbnb['part_logement_entier_inf_100€'] = [part_logement_entier_inf_100]
                table_data_airbnb['part_logement_entier_sup_100€'] = [part_logement_entier_sup_100]
                table_data_airbnb['prix_moyen'] = [count_data_airbnb_price_sum]
                
                table_data_airbnb['license_valide'] = [nbres_licence_valide]
                table_data_airbnb['licence_mobilite'] = [nbres_licence_mobilite]
                table_data_airbnb['licence_vide'] = [nbres_licence_vide]
                table_data_airbnb['licence_hotel'] = [nbres_licence_hotel]
                
                data_airbnb_paris_sig.append(table_data_airbnb)
                
        data_airbnb_paris_sig_concat = pd.concat(data_airbnb_paris_sig) # Assemblage des données et dissolution sur une seule entête
    
        data_airbnb_paris_sig_concat.to_csv(chemin_dossier +"/DATA_AIRBNB_PARIS_{}_ARR.csv".format(arrondissement), index = False, sep=';')

nom_fichier = "listings_paris"
# traitement_data_airbnb_paris_arrondissment(nom_fichier)
