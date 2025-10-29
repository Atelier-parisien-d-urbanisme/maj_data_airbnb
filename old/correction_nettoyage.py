#coding:utf-8

import os
import numpy as np
import pandas as pd
import geopandas as gpd

# champ_1 = 'l_cab' # 'l_cab'
# champ_2 = 'c_cainsee' #'c_cainsee' 
# shapefile = r'P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\shapefile_ville\commune.shp'

# idf = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\LISTINGS_AIRBNB\listings_idf_2024_06_21.csv"
# paris = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\LISTINGS_AIRBNB\listings_paris_2024_06_10.csv"

# def traitement(input):
#     read_data_airbnb_listings = pd.read_csv(input,sep=';')


#     # Selection des champs utiles
#     data_airbnb_read= read_data_airbnb_listings[['id','latitude','longitude','listing_url','room_type','price','availability_365','calculated_host_listings_count','reviews_per_month','license','last_scraped','ville','date_tele']]

#     # Convertire les données aribnb en points shapefile
#     points = gpd.GeoDataFrame(data_airbnb_read, geometry=gpd.points_from_xy(data_airbnb_read.longitude, data_airbnb_read.latitude),crs=4326) # WGS84

#     # Conversion du système de projection
#     points = points.to_crs(2154) # RGF_1993

#     # Lire le fichier shapefile des communes idf
#     zones = gpd.read_file(shapefile)

#     # Conversion du système de projection
#     zones = zones.to_crs(2154)

#     # Vérifier et aligner les CRS
#     if zones.crs != points.crs:
#         points = points.to_crs(zones.crs)

#     # Selection des champs 
#     zones = zones[[champ_1,champ_2, 'geometry']] 

#     # Jointure spatiale par rapport aux communes
#     points_in_zones = gpd.sjoin(points, zones, how="left", predicate="within")

#     # Table de travail
#     data_airbnb_temp = points_in_zones

#     data_airbnb_sans_hotel = data_airbnb_temp[(data_airbnb_temp['room_type'] != 'Hotel room')]

#     data_airbnb_availability = data_airbnb_sans_hotel[['id','availability_365',champ_2]]

#     nan_rows = data_airbnb_availability[data_airbnb_availability['availability_365'].isna()]
#     print(nan_rows)
#     data_airbnb_availability = data_airbnb_availability.fillna(0)


#     test2 = data_airbnb_availability[(data_airbnb_availability[champ_2]== 75000)]

    
#     return test2

# print(traitement(idf))
# print(traitement(paris))
# df1 = traitement(idf)
# df2 = traitement(paris)

# df_merged = pd.merge(df1, df2, on='id', how='outer', indicator=True)
# print(df_merged)

# df_merged.to_csv(r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\diff.csv', index = False, sep=';')



# test1 = df1[(df1[champ_2]== 75000) & (df1['availability_365']==0)]

# test2 = df2[(df2[champ_2]== 75000) & (df2['availability_365']==0)]

# print(test1)
# print(test2)

# test2 = data_airbnb_availability[(data_airbnb_availability[champ_2]== 1)]

# test2.to_csv(r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\test1.csv', index = False, sep=';')              
# print(test2)

# data_airbnb_availability['disponibilite_aucune'] = data_airbnb_availability['availability_365'] == 0
# data_airbnb_availability['disponibilite_inf_120_jours'] = (data_airbnb_availability['availability_365'] > 0) & (data_airbnb_availability['availability_365'] < 120)
# data_airbnb_availability['disponibilite_sup_120_jours'] = data_airbnb_availability['availability_365'] >= 120

# # Compter les vrais et faux pour chaque type de disponibilité
# data_airbnb_availability = data_airbnb_availability.groupby(champ_2).agg({'disponibilite_aucune': ['sum','count'],'disponibilite_inf_120_jours': ['sum','count'],'disponibilite_sup_120_jours': ['sum','count']})

# print(data_airbnb_availability)


import matplotlib.pyplot as plt

idf = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\commune_output\DATA_AIRBNB_PARIS_INDICATEURS_SIG_COMMUNES.csv"

read_data_airbnb_listings = pd.read_csv(idf,sep=';')

diff1 = read_data_airbnb_listings[['date_tele','l_cab',"nombres_annonces"]]

diff1 = diff1[(diff1['l_cab']== 'Paris')]
diff1['date_tele'] = pd.to_datetime(diff1['date_tele'])
diff1['mois'] = diff1['date_tele'].dt.month
diff1['année'] = diff1['date_tele'].dt.year
diff1['mois_années'] = diff1['mois'].astype(str) + '-' + diff1['année'].astype(str)
print(diff1)


paris = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\commune_output\DATA_AIRBNB_PARIS_INDICATEURS_SIG_ARRONDISSEMENTS.csv"

read_data_airbnb_listings = pd.read_csv(paris,sep=';')

diff = read_data_airbnb_listings[['date_tele','c_arinsee',"nombres_annonces"]]

diff = diff[(diff['c_arinsee']== 75000)]
diff['date_tele'] = pd.to_datetime(diff['date_tele'])
diff['mois'] = diff['date_tele'].dt.month
diff['année'] = diff['date_tele'].dt.year
diff['mois_années'] = diff['mois'].astype(str) + '-' + diff['année'].astype(str)
print(diff)


plt.figure(figsize=(10, 6))
plt.plot(diff['mois_années'], diff['nombres_annonces'], marker='o', linestyle='-', color='b')
plt.plot(diff1['mois_années'], diff1['nombres_annonces'], marker='o', linestyle='-', color='r')
plt.title('Comparison of ads by month and year on PARIS')
plt.xlabel('Months and Years')
plt.ylabel('numbers ads')
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()

# Afficher le graphique
plt.show()