#coding:utf-8

import pandas as pd
import numpy as np
import requests
import gzip
import os

# Ce script permet d'extraire et traiter les données AIRBNB provenant d'INSIDE AIRBNB...

chemin_dossier_listings = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\LISTINGS_AIRBNB' # Chemin du dossier listings
chemin_dossier_reviews = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\REVIEWS_AIRBNB' # Chemin du dossier reviews

ville_url = ['france/ile-de-france/paris','united-states/ny/new-york-city','the-netherlands/north-holland/amsterdam','spain/catalonia/barcelona','germany/be/berlin','united-kingdom/england/london','france/auvergne-rhone-alpes/lyon','france/nouvelle-aquitaine/bordeaux','france/pyr%C3%A9n%C3%A9es-atlantiques/pays-basque']
villes = ['paris','new_york','amsterdam','barcelone','berlin','londres','lyon','bordeaux','pays-basque']
annees = [2022,2023] 
jours = ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31']
mois = ['01','02','03','04','05','06','07','08','09','10','11','12']

#### EXTRACTION DES LISTINGS ####
def extraction_listings_data_airbnb(ville_url,villes,annees,jours,mois,chemin_dossier_listings):
    
    liste_fichier_listings = os.listdir(chemin_dossier_listings)
    print(liste_fichier_listings)

    for i in range(0,np.size(villes),1):
        v = villes[i]
        vu = ville_url[i]
        print("ville:", v)
        for a in annees:
            print("annee:", a)
            for m in mois:
                print("mois:", m)
                for j in jours:
                    
                    URL = "http://data.insideairbnb.com/{}/{}-{}-{}/data/listings.csv.gz".format(vu,a,m,j) # Lien url
                    print(URL)

                    r = requests.get(URL)
                   
                    fichier = "listings_{}_{}_{}_{}.csv".format(v,a,m,j)
                    in_data = np.isin(liste_fichier_listings,fichier)
                    in_data = in_data[(in_data == True)] # voir si il existe deja dans le dossier
                   
                    if (r.status_code == 200) & (in_data != True): 
                        
                        fichier_entree = chemin_dossier_listings + "\\" + "listings_{}_{}_{}_{}.csv.gz".format(v,a,m,j)
                        fichier_sortie = chemin_dossier_listings + "\\" + "listings_{}_{}_{}_{}.csv".format(v,a,m,j)
                        with open(fichier_entree, 'wb') as f:
                            f.write(r.content)
                            
                        with open(fichier_entree, 'rb') as entree, open(fichier_sortie, 'w', encoding='utf8') as sortie:
                            decom_str = gzip.decompress(entree.read()).decode('utf-8')
                            sortie.write(decom_str)
                        
                        data_airbnb_temp = pd.read_csv(fichier_sortie)
                        data_airbnb_temp['ville'] = '{}'.format(v)
                        data_airbnb_temp['date_tele'] = '{}-{}-{}'.format(a,m,j)
                        data_airbnb_temp.to_csv(fichier_sortie,index = True, sep=';')
                        
                        os.remove(fichier_entree)
                    else:
                        next
                        
    print("Extraction data Inside AirBnb terminée...")
                            
extraction_listings_data_airbnb(ville_url,villes,annees,jours,mois,chemin_dossier_listings)

#### EXTRACTION DES REVIEWS ####
def extraction_reviews_data_airbnb(ville_url,villes,annees,jours,mois,chemin_dossier_reviews):
        
    liste_fichier_reviews = os.listdir(chemin_dossier_reviews)
    print(liste_fichier_reviews)
    
    for i in range(0,np.size(villes),1):
        v = villes[i]
        vu = ville_url[i]
        print("ville:", v)
        for a in annees:
            print("annee:", a)
            for m in mois:
                print("mois:", m)
                for j in jours:
                
                    URL = "http://data.insideairbnb.com/{}/{}-{}-{}/data/reviews.csv.gz".format(vu,a,m,j) # Lien url
                    print(URL)
    
                    r = requests.get(URL)
                    
                    fichier = "reviews_{}_{}_{}_{}.csv".format(v,a,m,j)
                    in_data = np.isin(liste_fichier_reviews,fichier)
                    in_data = in_data[(in_data == True)] # voir si il existe deja dans le dossier
                    
                    if (r.status_code == 200) & (in_data != True):

                        fichier_entree = chemin_dossier_reviews + "\\" + "reviews_{}_{}_{}_{}.csv.gz".format(v,a,m,j)
                        fichier_sortie = chemin_dossier_reviews + "\\" + "reviews_{}_{}_{}_{}.csv".format(v,a,m,j)
                        with open(fichier_entree, 'wb') as f:
                            f.write(r.content)
                            
                        with open(fichier_entree, 'rb') as entree, open(fichier_sortie, 'w', encoding='utf8') as sortie:
                            decom_str = gzip.decompress(entree.read()).decode('utf-8')
                            sortie.write(decom_str)
                        
                        data_airbnb_temp = pd.read_csv(fichier_sortie)
                        data_airbnb_temp['ville'] = '{}'.format(v)
                        data_airbnb_temp['date_tele'] = '{}-{}-{}'.format(a,m,j)
                        data_airbnb_temp.to_csv(fichier_sortie,index = True, sep=';')
                        
                        os.remove(fichier_entree)
                    else:
                        next
                        
    print("Extraction data AirBnb terminée...")
                            
extraction_reviews_data_airbnb(ville_url,villes,annees,jours,mois,chemin_dossier_reviews)
