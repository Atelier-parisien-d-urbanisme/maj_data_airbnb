# coding:utf-8

import pandas as pd
import matplotlib.pyplot as plt

# Chemin vers les fichiers CSV
csv_file_idf = r"\\ZSFB\Projets$\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\commune_output\DATA_AIRBNB_PARIS_INDICATEURS_SIG_COMMUNES_IDF.csv"
csv_file_paris = r"\\ZSFB\Projets$\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\commune_output\DATA_AIRBNB_PARIS_INDICATEURS_SIG_ARRONDISSEMENTS_PARIS.csv"

# Lire les fichiers CSV
data_idf = pd.read_csv(csv_file_idf,sep=';')
data_paris = pd.read_csv(csv_file_paris,sep=';')


data_idf = data_idf[(data_idf['c_cainsee'] == 75000)]

data_paris = data_paris[(data_paris['c_arinsee'] == 75000)]


print(data_idf)
print("------------------------")
print(data_paris)


data_idf['date'] = pd.to_datetime(data_idf['date_tele'])
data_idf['mois'] = data_idf['date'].dt.month
data_idf['année'] = data_idf['date'].dt.year
data_idf['mois_années'] = data_idf['mois'].astype(str) + '-' + data_idf['année'].astype(str)


data_paris['date'] = pd.to_datetime(data_paris['date_tele'])
data_paris['mois'] = data_paris['date'].dt.month
data_paris['année'] = data_paris['date'].dt.year
data_paris['mois_années'] = data_paris['mois'].astype(str) + '-' + data_paris['année'].astype(str)


df_merged = pd.merge(data_idf, data_paris, on='mois_années', suffixes=('_idf', '_paris'))
print(df_merged)

chemin= r"\\ZSFB\Projets$\LOCATIONS_MEUBLEES_TOURISTIQUES\2022-2023_Données\INSIDE_AIRBNB\commune_output\plot"
# Exemple de traçage
# Supposons que votre CSV a deux colonnes nommées 'x' et 'y'
# Adapter les noms des colonnes en fonction de votre fichier CSV
plt.figure(figsize=(10, 6))

plt.plot(data_idf['date'], data_idf['nombres_annonces'], marker='o',linestyle='--',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['nombres_annonces'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur nombres_annonces sur Paris")
plt.legend()
plt.savefig(chemin + "\\" + "nombres_annonces.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['nbres_chambres_hotels'], marker='o',linestyle='--',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['nbres_chambres_hotels'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur nbres_chambres_hotels")
plt.legend()
plt.savefig(chemin + "\\" +"nbres_chambres_hotels.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['nombres_annonces_hors_hotels'], marker='o',linestyle='--',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['nombres_annonces_hors_hotels'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur nombres_annonces_hors_hotels sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"nombres_annonces_hors_hotels.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['nbres_logements_entiers'], marker='o',linestyle='--',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['nbres_logements_entiers'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur nbres_logements_entiers sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"nbres_logements_entiers.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['nbres_chambres_privees'], marker='o',linestyle='--',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['nbres_chambres_privees'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur nbres_chambres_privees sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"nbres_chambres_privees.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['nbres_chambres_partagees'], marker='o',linestyle='--',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['nbres_chambres_partagees'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur nbres_chambres_partagees sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"nbres_chambres_partagees.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['part_de_logements_entiers_(%)'], marker='o',linestyle='--',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['part_de_logements_entiers_(%)'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur part_de_logements_entiers_(%) sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"part_de_logements_entiers_.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['nbres_annonces_dispo_log_365'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['nbres_annonces_dispo_log_365'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur nbres_annonces_dispo_log_365 sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"nbres_annonces_dispo_log_365.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['nbres_annonces_dispo_sauf_hotel_365'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['nbres_annonces_dispo_sauf_hotel_365'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur nbres_annonces_dispo_sauf_hotel_365 sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"nbres_annonces_dispo_sauf_hotel_365.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['annonces_par_loueur_(1)'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['annonces_par_loueur_(1)'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur annonces_par_loueur_(1) sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"annonces_par_loueur_(1).jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['annonces_par_loueur_(2_a_9)'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['annonces_par_loueur_(2_a_9)'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur annonces_par_loueur_(2_a_9) sur Paris")
plt.legend()
plt.savefig("annonces_par_loueur_(2_a_9).jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['annonces_par_loueur_(10_et_plus)'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['annonces_par_loueur_(10_et_plus)'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur annonces_par_loueur_(10_et_plus) sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"annonces_par_loueur_(10_et_plus).jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['part_annonces_de_multiloueurs_(%)'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['part_annonces_de_multiloueurs_(%)'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur part_annonces_de_multiloueurs_(%) sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"part_annonces_de_multiloueurs_.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['nbres_commentaires_(0)'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['nbres_commentaires_(0)'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur nbres_commentaires_(0) sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"nbres_commentaires_(0).jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['nbres_commentaires_(0_a_1.75)'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['nbres_commentaires_(0_a_1.75)'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur nbres_commentaires_(0_a_1.75) sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"nbres_commentaires_(0_a_1.75).jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['nbres_commentaires_(1.75_et_plus)'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['nbres_commentaires_(1.75_et_plus)'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur nbres_commentaires_(1.75_et_plus) sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"nbres_commentaires_(1.75_et_plus).jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['part_de_commentaires_(1.75_et_plus)'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['part_de_commentaires_(1.75_et_plus)'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur part_de_commentaires_(1.75_et_plus) sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"part_de_commentaires_(1.75_et_plus).jpg", format="jpeg", dpi=300)

plt.plot(data_idf['date'], data_idf['nbres_commentaires_total'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['nbres_commentaires_total'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur nbres_commentaires_total sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"nbres_commentaires_total.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['disponibilite_aucune'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['disponibilite_aucune'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur disponibilite_aucune sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"disponibilite_aucune.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['disponibilite_inf_120_jours'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['disponibilite_inf_120_jours'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur disponibilite_inf_120_jours sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"disponibilite_inf_120_jours.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['disponibilite_sup_120_jours'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['disponibilite_sup_120_jours'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur disponibilite_sup_120_jours sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"disponibilite_sup_120_jours.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['part_disponibilite_sup_120_jours_(%)'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['part_disponibilite_sup_120_jours_(%)'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur part_disponibilite_sup_120_jours_(%) sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"part_disponibilite_sup_120_jours_.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['prix_logement_entier_inf_100_euro'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['prix_logement_entier_inf_100_euro'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur prix_logement_entier_inf_100_euro sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"prix_logement_entier_inf_100_euro.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['prix_logement_entier_sup_100_euro'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['prix_logement_entier_sup_100_euro'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur prix_logement_entier_sup_100_euro sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"prix_logement_entier_sup_100_euro.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['part_logement_entier_inf_100_euro'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['part_logement_entier_inf_100_euro'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur part_logement_entier_inf_100_euro sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"part_logement_entier_inf_100_euro.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['part_logement_entier_sup_100_euro'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['part_logement_entier_sup_100_euro'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur part_logement_entier_sup_100_euro sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"part_logement_entier_sup_100_euro.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['prix_moyen'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['prix_moyen'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur prix_moyen sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"prix_moyen.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['prix_moyen_1j_dispo'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['prix_moyen_1j_dispo'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur prix_moyen_1j_dispo sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"prix_moyen_1j_dispo.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['license_valide'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['license_valide'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur license_valide sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"license_valide.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['licence_mobilite'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['licence_mobilite'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur licence_mobilite sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"licence_mobilite.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['licence_vide'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['licence_vide'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur licence_vide sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"licence_vide.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['licence_hotel'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['licence_hotel'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur licence_hotel sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"licence_hotel.jpg", format="jpeg", dpi=300)
plt.close()

plt.plot(data_idf['date'], data_idf['licence_autres'], linestyle='--',marker='o',color='r',label='data idf')
plt.plot(data_paris['date'], data_paris['licence_autres'], marker='x',color='b',label='data site inside paris')
plt.title("Indicateur licence_autres sur Paris")
plt.legend()
plt.savefig(chemin + "\\" +"licence_autres.jpg", format="jpeg", dpi=300)
plt.close()


