# coding:utf-8

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import geopandas as gpd


# Ce script permet de traiter, de géolocaliser et de calculer les différents indicateurs sur les données AIRBNB provenant d'INSIDE AIRBNB...


def traitement_reviews_data_airbnb_sig(chemin_dossier_listings,chemin_dossier_sortie,shapefile,nom_fichier_listings,liste_carte):
    
    liste_fichier = os.listdir(chemin_dossier_listings)

    
    liste_fichier_dossier = [fichier for fichier in liste_fichier if fichier.startswith(nom_fichier_listings)]
    
    set1 = set(liste_fichier_dossier)
    set2 = set(liste_carte)
    common_strings = set1.intersection(set2)
    liste_fichier_sel = list(common_strings)
    print(liste_fichier_sel)
    
    for fichier in liste_fichier_sel: 
  
        print(fichier)
        read_data_airbnb = pd.read_csv(chemin_dossier_listings+"\\"+ fichier,sep=';')
        data_airbnb = read_data_airbnb[['id','latitude','longitude','room_type','ville','date_tele']]
        
        data_airbnb = data_airbnb[(data_airbnb['room_type'] == 'Entire home/apt')]
        data_airbnb_sig_concat_groupby = data_airbnb.groupby(['id','latitude','longitude','date_tele']).count()
        data_airbnb_sig_concat_groupby_reset = data_airbnb_sig_concat_groupby.reset_index()
        print(data_airbnb_sig_concat_groupby_reset)
                
        ville_shape = gpd.read_file(shapefile) # lecture du shapefile 
        ville_shape = ville_shape.to_crs(2154)
        points_gdf = gpd.GeoDataFrame(data_airbnb_sig_concat_groupby_reset, geometry=gpd.points_from_xy(data_airbnb_sig_concat_groupby_reset.longitude, data_airbnb_sig_concat_groupby_reset.latitude),crs=4326) # WGS84
        points_gdf = points_gdf.to_crs(2154) # RGF_1993
        
        intersect_shape= gpd.sjoin(points_gdf, ville_shape[[champ_shape, 'geometry']], how='left', predicate='intersects') 
        intersect_in_shape = intersect_shape[(intersect_shape[champ_shape].notnull())] # tous les arrondissements à modifier pour les autres villes
        print(intersect_in_shape)
            
        fig,ax = plt.subplots(figsize=(10,10))
        ville_shape.plot(champ_shape,ax=ax)
        points_gdf.plot(ax=ax)
        intersect_in_shape.plot(ax=ax)
        plt.show()

        intersect_in_shape.to_file(chemin_dossier_sortie + "//"+ fichier[:25] + ".shp")

champ_shape = 'l_ar' 
nom_export = '/OUTPUT/DATA_AIRBNB_PARIS_INDICATEURS_SIG_2024.csv'
shapefile = r'P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\shapefile_ville\paris.shp'
nom_fichier_listings = "listings_paris" # début des noms des fichiers avec ville pour récupérer les coordonnées des annonces
chemin_dossier_listings = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\LISTINGS_AIRBNB' # Chemin du dossier des annonces avec localisation
chemin_dossier_sortie = r'\\Domapur.fr\zsf-apur\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\points' # Chemin du dossier de sortie

liste_carte = ['listings_paris_2015_05_06.csv','listings_paris_2016_04_03.csv','listings_paris_2017_04_04.csv','listings_paris_2018_04_10.csv','listings_paris_2019_04_09.csv','listings_paris_2020_04_15.csv',
              'listings_paris_2021_04_10.csv','listings_paris_2022_04_12.csv','listings_paris_2023_04_09.csv' ]

traitement_reviews_data_airbnb_sig(chemin_dossier_listings,chemin_dossier_sortie,shapefile,nom_fichier_listings,liste_carte)



# chemin_dossier_shapefile = r'P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\points'
# liste_fichier= os.listdir(chemin_dossier_shapefile)
# liste_shapefiles = [fichier for fichier in liste_fichier if fichier.endswith('.shp')]

# for i in liste_shapefiles:
  
#   print(i)
#   read_shp = gpd.read_file(chemin_dossier_shapefile + '\\' + i)
#   count_lines = len(read_shp)
#   print(count_lines)



# import arcpy

# if arcpy.CheckExtension("Spatial") == "Available":
#     print("Kernel Density tool executed successfully.")
# else:
#     print("Kernel Density tool failed to execute.")

# arcpy.CheckOutExtension("Spatial")
# arcpy.env.workspace = chemin_dossier_sortie

# shapefiles = arcpy.ListFeatureClasses("*", "Point")

# for i in shapefiles:
#     print(i)

#     arcpy.env.workspace = r"P:\PROJETS\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\points\carte"
#     output = arcpy.sa.KernelDensity(
#     in_features=chemin_dossier_sortie + "\\" + i,
#     population_field="NONE",
#     cell_size=5,
#     search_radius=None,
#     area_unit_scale_factor="SQUARE_KILOMETERS",
#     out_cell_values="DENSITIES",
#     method="PLANAR",
#     in_barriers=None)
    
#     output.save(i[:25] + ".tif")





# chemin = r"\\Domapur.fr\zsf-apur\PROJETS\OBSERVATOIRE_CONSO_ENERGIE\13_DPE_et_consos_réelles_2024\004_Cartes_Donnees\ADEME_DPE_PARIS\csv2\dpe_complet.csv"
 
# df =  pd.read_csv(chemin, sep=';')
# df = df.drop(0)
# print(df.head(5))


# # chemin_2 = r"\\Domapur.fr\zsf-apur\PROJETS\OBSERVATOIRE_CONSO_ENERGIE\13_DPE_et_consos_réelles_2024\004_Cartes_Donnees\ADEME_DPE_PARIS\csv2\entetes_conso_corrige.csv"

# # new_head = pd.read_csv(chemin_2, sep=';')
# # print(new_head)
# # print(len(new_head.columns))


# new_head = ["n_dpe","date_reception_dpe","date_etablissement_dpe","date_visite_diagnostiqueur","modele_dpe","n_dpe_remplace","date_fin_validite_dpe","version_dpe","n_dpe_immeuble_associe","appartement_non_visite","methode_application_dpe","n_immatriculation_copropriete","invariant_fiscal_logement","n_rpls_logement","etiquette_ges","etiquette_dpe","annee_construction","type_batiment","type_installation_chauffage","type_installation_ecs_general","periode_construction","hauteur_sous_plafond","nombre_appartement","nombre_niveau_immeuble","nombre_niveau_logement","surface_habitable_immeuble","surface_habitable_logement","surface_tertiaire_immeuble","classe_inertie_batiment","typologie_logement","position_logement_dans_immeuble","classe_altitude","zone_climatique","adresse_brute","nom_commune_ban","code_insee_ban","n_voie_ban","identifiant_ban","adresse_ban","code_postal_ban","score_ban","nom_rue_ban","coordonnee_cartographique_x_ban","coordonnee_cartographique_y_ban","code_postal_brut","n_etage_appartement","nom_residence","complement_d_adresse_batiment","cage_d_escalier","complement_d_adresse_logement","statut_geocodage","nom_commune_brut","n_departement_ban","n_region_ban","conso_5_usages_e_finale","conso_5_usages_m2_e_finale","conso_chauffage_e_finale","conso_chauffage_depensier_e_finale","conso_eclairage_e_finale","conso_ecs_e_finale","conso_ecs_depensier_e_finale","conso_refroidissement_e_finale","conso_refroidissement_depensier_e_finale","conso_auxiliaires_e_finale","conso_5_usages_e_primaire","conso_5_usages_par_m2_e_primaire","conso_chauffage_e_primaire","conso_chauffage_depensier_e_primaire","conso_eclairage_e_primaire","conso_ecs_e_primaire","conso_ecs_depensier_e_primaire","conso_refroidissement_e_primaire","conso_refroidissement_depensier_e_primaire","conso_auxiliaires_e_primaire","emission_ges_5_usages","emission_ges_5_usages_par_m2","emission_ges_chauffage","emission_ges_chauffage_depensier","emission_ges_eclairage","emission_ges_ecs","emission_ges_ecs_depensier","emission_ges_refroidissement","emission_ges_refroidissement_depensier","emission_ges_auxiliaires","conso_5_usages_e_finale_energie_n_1","conso_chauffage_e_finale_energie_n_1","conso_ecs_e_finale_energie_n_1","cout_total_5_usages_energie_n_1","cout_chauffage_energie_n_1","cout_ecs_energie_n_1","emission_ges_5_usages_energie_n_1","emission_ges_chauffage_energie_n_1","emission_ges_ecs_energie_n_1","type_energie_n_1","conso_5_usages_e_finale_energie_n_2","conso_chauffage_e_finale_energie_n_2","conso_ecs_e_finale_energie_n_2","cout_total_5_usages_energie_n_2","cout_chauffage_energie_n_2","cout_ecs_energie_n_2","emission_ges_5_usages_energie_n_2","emission_ges_chauffage_energie_n_2","emission_ges_ecs_energie_n_2","type_energie_n_2","conso_5_usages_e_finale_energie_n_3","conso_chauffage_e_finale_energie_n_3","conso_ecs_e_finale_energie_n_3","cout_total_5_usages_energie_n_3","cout_chauffage_energie_n_3","cout_ecs_energie_n_3","emission_ges_5_usages_energie_n_3","emission_ges_chauffage_energie_n_3","emission_ges_ecs_energie_n_3","type_energie_n_3","cout_total_5_usages","cout_chauffage","cout_chauffage_depensier","cout_eclairage","cout_ecs","cout_ecs_depensier","cout_refroidissement","cout_refroidissement_depensier","cout_auxiliaires","logement_traversant_0_1","presence_brasseur_air_0_1","indicateur_confort_ete","isolation_toiture_0_1","protection_solaire_exterieure_0_1","inertie_lourde_0_1","deperditions_baies_vitrees","deperditions_enveloppe","deperditions_murs","deperditions_planchers_bas","deperditions_planchers_hauts","deperditions_ponts_thermiques","deperditions_portes","deperditions_renouvellement_air","qualite_isolation_enveloppe","qualite_isolation_menuiseries","qualite_isolation_murs","qualite_isolation_plancher_bas","ubat_w_m2_k","qualite_isolation_plancher_haut_toit_terrase","qualite_isolation_plancher_haut_comble_amenage","qualite_isolation_plancher_haut_comble_perdu","apports_internes_saison_chauffe","apports_internes_saison_froid","apports_solaires_saison_chauffe","apports_solaires_saison_froid","besoin_chauffage","besoin_ecs","besoin_refroidissement","besoin_refroidissement_depensier","type_energie_principale_chauffage","conso_chauffage_installation_chauffage_n_1","conso_chauffage_depensier_installation_chauffage_n_1","description_installation_chauffage_n_1","configuration_installation_chauffage_n_1","type_installation_chauffage_n_1","facteur_couverture_solaire_installation_chauffage_n_1","facteur_couverture_solaire_saisi_installation_chauffage_n_1","surface_chauffee_installation_chauffage_n_1","type_emetteur_installation_chauffage_n_1","conso_chauffage_generateur_n_1_installation_n_1","conso_chauffage_depensier_generateur_n_1_installation_n_1","description_generateur_chauffage_n_1_installation_n_1","type_energie_generateur_n_1_installation_n_1","type_generateur_n_1_installation_n_1","usage_generateur_n_1_installation_n_1","conso_chauffage_generateur_n_2_installation_n_1","conso_chauffage_depensier_generateur_n_2_installation_n_1","description_generateur_chauffage_n_2_installation_n_1","type_energie_generateur_n_2_installation_n_1","type_generateur_n_2_installation_n_1","usage_generateur_n_2_installation_n_1","conso_chauffage_installation_chauffage_n_2","conso_chauffage_depensier_installation_chauffage_n_2","description_installation_chauffage_n_2","configuration_installation_chauffage_n_2","type_installation_chauffage_n_2","facteur_couverture_solaire_installation_chauffage_n_2","facteur_couverture_solaire_saisi_installation_chauffage_n_2","surface_chauffee_installation_chauffage_n_2","type_emetteur_installation_chauffage_n_2","conso_chauffage_generateur_n_1_installation_n_2","conso_chauffage_depensier_generateur_n_1_installation_n_2","description_generateur_chauffage_n_1_installation_n_2","type_energie_generateur_n_1_installation_n_2","type_generateur_n_1_installation_n_2","usage_generateur_n_1_installation_n_2","conso_chauffage_generateur_n_2_installation_n_2","conso_chauffage_depensier_generateur_n_2_installation_n_2","description_generateur_chauffage_n_2_installation_n_2","type_energie_generateur_n_2_installation_n_2","type_generateur_n_2_installation_n_2","usage_generateur_n_2_installation_n_2","type_energie_principale_ecs","conso_e_finale_installation_ecs","conso_e_finale_depensier_installation_ecs","description_installation_ecs","configuration_installation_ecs","type_installation_ecs","type_installation_solaire","facteur_couverture_solaire","facteur_couverture_solaire_saisi","nombre_logements_desservis_par_installation_ecs","production_ecs_solaire_installation","surface_habitable_desservie_par_installation_ecs","conso_e_finale_generateur_ecs_n_1","conso_e_finale_depensier_generateur_ecs_n_1","cop_generateur_ecs_n_1","description_generateur_ecs_n_1","date_installation_generateur_ecs_n_1","type_energie_generateur_ecs_n_1","type_generateur_ecs_n_1","usage_generateur_ecs_n_1","volume_stockage_generateur_ecs_n_1","conso_e_finale_generateur_ecs_n_2","conso_e_finale_depensier_generateur_ecs_n_2","cop_generateur_ecs_n_2","description_generateur_ecs_n_2","date_installation_generateur_ecs_n_2","type_energie_generateur_ecs_n_2","type_generateur_ecs_n_2","usage_generateur_ecs_n_2","volume_stockage_generateur_ecs_n_2","besoin_ecs_batiment","besoin_ecs_logement","type_ventilation","surface_ventilee","ventilation_posterieure_2012_0_1","conso_refroidissement_annuel","conso_refroidissement_depensier_annuel","periode_installation_generateur_froid","type_generateur_froid","surface_climatisee","type_energie_climatisation","categorie_enr","electricite_pv_autoconsommee","systeme_production_electricite_origine_renouvelable","presence_production_pv_0_1","production_electricite_pv_kwhep_an","surface_totale_capteurs_photovoltaique","nombre_module","X","Y"]


# df.columns = new_head
# print(df)

# df.to_csv(r"\\Domapur.fr\zsf-apur\PROJETS\OBSERVATOIRE_CONSO_ENERGIE\13_DPE_et_consos_réelles_2024\004_Cartes_Donnees\ADEME_DPE_PARIS\csv2\dpe_final.csv")