#coding:utf-8

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from datetime import date
from forex_python.converter import CurrencyRates
import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc
from dash import dash_table
import dash

# Ce script permet de traiter, de géolocaliser et de calculer les différents indicateurs sur les données AIRBNB provenant d'INSIDE AIRBNB...

champ_shape = 'l_ar' # boro_name # l_ar # nom # Stadsdeel # NOM # Gemeinde_n # NAME
shapefile = r'P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\shapefile_ville\paris.shp'
# chemin_fichier_listings = r"\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\LISTINGS_AIRBNB\listings_paris_2023_04_09.csv" # Chemin du dossier des annonces avec localisation
chemin_dossier_listings = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\LISTINGS_AIRBNB' # Chemin du dossier des annonces avec localisation
nom_fichier_listings = "listings_paris_2022" # début des noms des fichiers avec ville pour récupérer les coordonnées des annonces

liste_fichier = os.listdir(chemin_dossier_listings)

data_airbnb_append = []

for fichier in liste_fichier:  # Lire et stocker les fichiers *.csv dans une seule fichier
    if fichier.startswith(nom_fichier_listings):
      print(fichier)


      read_data_airbnb_listings = pd.read_csv(chemin_dossier_listings+"\\"+ fichier,sep=';')
      data_airbnb = read_data_airbnb_listings[['id','latitude','longitude','ville','host_url','room_type','price','reviews_per_month','availability_365','date_tele']]
      
      data_airbnb_append.append(data_airbnb)

data_airbnb_concat = pd.concat(data_airbnb_append) # Assemblage des données et dissolution sur une seule entête


data_airbnb_concat['price'] = data_airbnb_concat['price'].astype(str)
data_airbnb_concat['price'] = data_airbnb_concat['price'].str.replace('$','',regex=True)
data_airbnb_concat['price'] = data_airbnb_concat['price'].str.replace(',','',regex=True)
data_airbnb_concat['price'] = data_airbnb_concat['price'].astype(float) 

ville_shape = gpd.read_file(shapefile) # lecture du shapefile 
ville_shape = ville_shape.to_crs(4326)
data_airbnb_pts = gpd.GeoDataFrame(data_airbnb_concat, geometry=gpd.points_from_xy(data_airbnb_concat.longitude, data_airbnb_concat.latitude),crs=4326) # WGS84
print(data_airbnb_pts)
# points_gdf = points_gdf.to_crs(2154) # RGF_1993

intersect_shape= gpd.sjoin(data_airbnb_pts, ville_shape[[champ_shape, 'geometry']], how='left', predicate='intersects') # l_ar pour les arrondissements à modifier pour les autres villes
intersect_in_shape = intersect_shape[(intersect_shape[champ_shape].notnull())] 


#### TABLEAU DE BORD INSIDE AIRBNB ####
app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "10rem",
    "padding": "2rem 1rem",
    "background-color": "#f8f9fa",
}

CONTENT_STYLE = {
    "margin-left": "1rem",
    "margin-right": "1rem",
    "width": "20rem",
    "height": "100%",
    "padding": "2rem 1rem",
}

app.layout = dbc.Container([

  dbc.Row([
    dbc.Col([
      dcc.Markdown('''**Dataviz Inside Airbnb Paris** '''),
      dcc.Graph(id='map')
      ],style=CONTENT_STYLE),
    
    dbc.Col([
    dcc.Markdown('''**Tableau de bord Inside Airbnb**'''),
      dbc.Badge("date",color="white",text_color="black",className="border me-1"),
      dcc.Checklist(data_airbnb_pts['date_tele'].unique(), data_airbnb_pts['date_tele'].unique(), id="date", inline=True,style={"padding":"10px"}),
      
      ],style=SIDEBAR_STYLE),

  dbc.Row([
    dbc.Col([
    dash_table.DataTable(id='outdata',style_as_list_view=True)
    ],style=CONTENT_STYLE)]),
  
  dbc.Row([
    dbc.Col([
    dcc.Graph(id='line_chart')
    ],style=CONTENT_STYLE)])
    ])
])

@callback(Output('map', 'figure'), Input('date', 'value'))

def date_values(date):

    if date != None:
        data_airbnb_pts_map = data_airbnb_pts[(data_airbnb_pts['date_tele'].isin(date))].copy()

        fig = px.scatter_mapbox(data_airbnb_pts_map, 
                            lat=data_airbnb_pts_map['latitude'], 
                            lon=data_airbnb_pts_map['longitude'], 
                            color="room_type", 
                            size="price",
                            hover_name=data_airbnb_pts_map['id'],
                            hover_data=["id","reviews_per_month", "availability_365","host_url"],
                            size_max=50, 
                            zoom=10,
                            mapbox_style="carto-positron")
        return fig
    else:
        
        data_airbnb_pts_map = data_airbnb_pts[(data_airbnb_pts['date_tele'][0])].copy()

        fig = px.scatter_mapbox(data_airbnb_pts_map, 
                            lat=data_airbnb_pts_map['latitude'], 
                            lon=data_airbnb_pts_map['longitude'], 
                            color="room_type", 
                            size="price",
                            hover_name=data_airbnb_pts_map['id'],
                            hover_data=["id","reviews_per_month", "availability_365","host_url"],
                            size_max=50, 
                            zoom=10,
                            mapbox_style="carto-positron")
        return fig
    
@callback(Output('outdata', 'data'),  Input('map', 'clickData'), Input("date", "value"))

def find_values(clickData,date):

    if clickData != None:
        id = [i["customdata"] for i in clickData['points']]
        collect_id= id[0][0]
        print(collect_id)
        data_airbnb_pts_data = data_airbnb_pts[(data_airbnb_pts['id'] == collect_id) & (data_airbnb_pts['date_tele'].isin(date))].copy()
        
        df1 = pd.DataFrame(data_airbnb_pts_data)
        df1 = pd.DataFrame(df1.drop(columns='geometry'))
        
        print(df1)
        
        return df1.to_dict('records')
    else:
        data_airbnb_pts_data = data_airbnb_pts[(data_airbnb_pts['id'][0]) & (data_airbnb_pts['date_tele'].isin(date))].copy()
        df1 = pd.DataFrame(data_airbnb_pts_data)
        df1 = pd.DataFrame(df1.drop(columns='geometry'))
        
        return df1.to_dict('records')

@callback(Output('line_chart', 'figure'),  Input('map', 'clickData'), Input("date", "value"))

def plot_values(clickData,date):

    if clickData != None:
        id = [i["customdata"] for i in clickData['points']]
        collect_id= id[0][0]
        print(collect_id)
        data_airbnb_pts_data = data_airbnb_pts[(data_airbnb_pts['id'] == collect_id) & (data_airbnb_pts['date_tele'].isin(date))].copy()
        
        df1 = pd.DataFrame(data_airbnb_pts_data)
        df1 = pd.DataFrame(df1.drop(columns='geometry'))
        
        print(df1)
        
        fig = px.scatter(df1, x='date_tele', y='price')
        fig.update_traces(mode='lines+markers')
      
        return fig
    else:
        data_airbnb_pts_data = data_airbnb_pts[(data_airbnb_pts['id'][0]) & (data_airbnb_pts['date_tele'].isin(date))].copy()
        df1 = pd.DataFrame(data_airbnb_pts_data)
        df1 = pd.DataFrame(df1.drop(columns='geometry'))
        
        fig = px.scatter(df1, x='date_tele', y='price')
        fig.update_traces(mode='lines+markers')
        
        return fig
      

if __name__ == '__main__':
  app.run(debug=True)





# logement_entier = points_gdf[(points_gdf['room_type'] == 'Entire home/apt')]
# hotel = points_gdf[(points_gdf['room_type'] == 'Hotel room')]
# chambre = points_gdf[(points_gdf['room_type'] == 'Private room')]
# chambre_partage = points_gdf[(points_gdf['room_type'] == 'Shared room')]

# fig1 = px.choroplethmapbox(ville_shape,geojson=ville_shape.geometry,locations=ville_shape.index, opacity = 0.1,center={"lat": 48.856614, "lon": 2.3522219},mapbox_style="carto-positron", zoom=12) #"open-street-map"
# fig1.add_scattermapbox(chambre['latitude'],lat = chambre['latitude'],lon = chambre['longitude'],name= 'chambre',text = chambre['host_url'],marker_size=5,color='rgb(255, 255, 0)')
# fig1.add_scattermapbox(lat = hotel['latitude'],lon = hotel['longitude'],name= 'hotel',text = hotel['host_url'],marker_size=5,marker_color='rgb(0, 255, 0)')
# fig1.add_scattermapbox(lat = logement_entier['latitude'],lon = logement_entier['longitude'],name= 'logement entier',text = logement_entier['host_url'],marker_size=5,marker_color='rgb(0, 0, 255)')
# fig1.add_scattermapbox(lat = chambre_partage['latitude'],lon = chambre_partage['longitude'],name= 'chambre partage',text = chambre_partage['host_url'],marker_size=5,marker_color='rgb(255, 0, 0)')
    
# fig1 = px.scatter_mapbox(data_airbnb_pts, 
#                         lat=data_airbnb_pts['latitude'], 
#                         lon=data_airbnb_pts['longitude'], 
#                         color="room_type", 
#                         size="price",
#                         hover_name=data_airbnb_pts['host_url'],
#                         hover_data=["reviews_per_month", "availability_365"],
#                         size_max=50, 
#                         zoom=10,
#                         mapbox_style="carto-positron")


# fig1.update_geos(fitbounds="locations", visible=True)
# fig1.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
# fig1.show() 