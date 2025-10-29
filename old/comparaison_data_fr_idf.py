#coding:utf-8

import pandas as pd
import numpy as np


data_fr = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\commune_output\DATA_AIRBNB_INDICATEURS_IDF_france.csv"
data_idf = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\commune_output\DATA_AIRBNB_INDICATEURS_idf.csv"

df_fr = pd.read_csv(data_fr,sep=';')
df_fr['date_tele'] = pd.to_datetime(df_fr['date_tele'])
df_fr['year'] = df_fr['date_tele'].dt.year
df_fr['month'] = df_fr['date_tele'].dt.month
df_fr['date_tele'] = df_fr['date_tele'].dt.to_period('M')
df_fr = df_fr[['date_tele','l_cab','nombres_annonces']]
print(df_fr)

df_idf = pd.read_csv(data_idf,sep=';')
df_idf['date_tele'] = pd.to_datetime(df_idf['date_tele'])
df_idf['year'] = df_idf['date_tele'].dt.year
df_idf['month'] = df_idf['date_tele'].dt.month
df_idf['date_tele'] = df_idf['date_tele'].dt.to_period('M')
df_idf = df_idf[['date_tele','l_cab','nombres_annonces']]
print(df_idf)


result = pd.merge(df_fr, df_idf, on=['l_cab', 'date_tele'], how='inner')
result = result.rename(columns={'nombres_annonces_x': 'nombres_annonces_fr', 'nombres_annonces_y': 'nombres_annonces_idf'})
result['diff_annonces'] = result['nombres_annonces_fr'] - result['nombres_annonces_idf']
result['diff_annonces_perc'] = (result['nombres_annonces_fr'] - result['nombres_annonces_idf']) / result['nombres_annonces_idf'] * 100
print(result)

result.to_csv(r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\commune_output\comparaison_data_scrap_fr_idf.csv", sep=';')