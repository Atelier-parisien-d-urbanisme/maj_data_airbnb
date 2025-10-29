#coding:utf-8

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from datetime import date
import geopandas as gpd
import requests
import matplotlib.pyplot as plt

# Liste des différentes fonctions d'indicateurs sur les données AIRBNB provenant d'INSIDE AIRBNB sur l'ensemble des communes d'idf...

# ----------------------------------------------------
# 0️⃣ Nettoyage
# ----------------------------------------------------

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

# ----------------------------------------------------
# 1️⃣ Conversion dates, prix et taux 
# ----------------------------------------------------

def traitement(fichier,chemin_dossier_listings,shapefile_ville,nom_ville,champ_1,champ_2,champ_3,champ_4,champ_5):

    print(fichier)

    # Lire les listings inside airbnb 
    read_data_airbnb_listings = pd.read_csv(
        chemin_dossier_listings + "\\" + fichier,
        sep=';',
        encoding='latin1',        # ou 'ISO-8859-1'
        low_memory=False          # pour éviter le DtypeWarning
    )

    # Selection des champs utiles
    data_airbnb_read= read_data_airbnb_listings[['id','latitude','longitude','listing_url','room_type','price','availability_365','bedrooms','number_of_reviews_ltm','calculated_host_listings_count','reviews_per_month','license','last_scraped','ville','date_tele']].copy()

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

    return points_in_zones,etat_data

def conversion(data, ville_to_currency=None):
    """
    Nettoie les colonnes de dates et de prix d'un DataFrame Airbnb,
    puis convertit les prix dans la devise EUR en fonction de la ville.

    Paramètres :
    ------------
    data : pd.DataFrame
        Le DataFrame contenant les colonnes suivantes :
        - 'date_tele'
        - 'last_scraped'
        - 'price'
        - 'ville'

    ville_to_currency : dict, optionnel
        Dictionnaire associant chaque ville à sa devise.
        Exemple : {'new_york': 'USD', 'londres': 'GBP', 'tokyo': 'JPY'}

    Retour :
    --------
    pd.DataFrame :
        Le DataFrame enrichi avec :
        - 'date_taux_de_change'
        - 'taux_de_change'
        - 'prix_euro'
    """

    # Dictionnaire par défaut si non fourni
    if ville_to_currency is None:
        ville_to_currency = {
            'new_york': 'USD',
            'londres': 'GBP',
            'tokyo': 'JPY'
        }

    # --- 1️⃣ Conversion des dates ---
    data['date_tele'] = pd.to_datetime(data['date_tele'], errors='coerce')
    data['last_scraped'] = pd.to_datetime(data['last_scraped'], format='%Y-%m-%d', errors='coerce')

    # --- 2️⃣ Nettoyage du prix ---
    data['price'] = (
        data['price']
        .astype(str)
        .str.replace(r'[\s$,€]', '', regex=True)
        .str.replace(',', '.', regex=False)
        .astype(float)
    )

    # --- 3️⃣ Fonction interne : obtenir taux de change ---
    def get_rate(base, target, date):
        date_str = date.strftime("%Y-%m-%d") if hasattr(date, "strftime") else str(date)
        url = f"https://api.frankfurter.app/{date_str}?from={base}&to={target}"
        try:
            r = requests.get(url, timeout=10)
            data_json = r.json()
            return data_json.get("rates", {}).get(target, 1.0)
        except Exception as e:
            print(f"Erreur API Frankfurter pour {base}->{target}:", e)
            return 1.0

    # --- 4️⃣ Conversion des prix ---
    data['date_taux_de_change'] = data['date_tele']
    data['taux_de_change'] = 1.0
    data['prix_euro'] = data['price']
    
    for ville, currency in ville_to_currency.items():
        mask = data['ville'].str.lower() == ville
        if mask.any():
            taux = get_rate(currency, "EUR", data.loc[mask, 'date_tele'].iloc[0])
            data.loc[mask, 'taux_de_change'] = taux
            data.loc[mask, 'prix_euro'] = data.loc[mask, 'price'] * taux
    
    # --- 5️⃣ Nettoyage des colonnes numériques ---
    for col in ['price', 'prix_euro', 'bedrooms', 'availability_365']:
        if col in data.columns:
            # 1️⃣ Remplace les chaînes vides ou espaces par NaN
            data[col] = data[col].replace(r'^\s*$', np.nan, regex=True)
            # 2️⃣ Convertit en numérique (texte invalide → NaN)
            data[col] = pd.to_numeric(data[col], errors='coerce')
            # 3️⃣ Remplace les 0 par NaN
            data[col] = data[col].replace(0, np.nan)

    return data

# ----------------------------------------------------
# 2️⃣ Prix des annonces
# ----------------------------------------------------

def prix_moyen_1j_dispo_corr_com(data, champ_2, afficher_graph=True, dossier_sortie=None, nom_fichier=None):

    # Sélection des logements entiers et disponibles avec au moins 1 commentaire récent
    data = data[(data['availability_365'] != 0) & (data['room_type'] == 'Entire home/apt') & (data['number_of_reviews_ltm'] > 0)]

    # Sélection des champs utiles
    data = data[['prix_euro', champ_2]]

    # Suppression des 5 % extrêmes
    low, high = 0.02, 0.98
    numeric_cols = data.select_dtypes(include=['number']).columns

    # Calcul des bornes
    quantiles = {col: (data[col].quantile(low), data[col].quantile(high)) for col in numeric_cols}

    # Masque des lignes conservées
    mask = data[numeric_cols].apply(lambda x: x.between(x.quantile(low), x.quantile(high))).all(axis=1)

    # Données exclues et filtrées
    data_exclues = data[~mask]
    data_filtrees = data[mask]

    # Affichage des valeurs exclues
    print("\n=== VALEURS EXCLUES (2 % extrêmes) ===")
    print(data_exclues)

    # Création du graphique
    plt.figure(figsize=(12, 5))

    # Boxplot avant/après
    plt.subplot(1, 2, 1)
    plt.boxplot(
        [data['prix_euro'], data_filtrees['prix_euro']],
        labels=['Avant filtrage', 'Après filtrage']
    )
    plt.title("Boxplot du prix avant/après suppression des valeurs extrêmes")
    plt.ylabel("Prix (€)")

    # Histogramme avant/après filtrage
    plt.subplot(1, 2, 2)
    plt.hist(data['prix_euro'], bins=30, alpha=0.6, label='Avant filtrage')
    plt.hist(data_filtrees['prix_euro'], bins=30, alpha=0.6, label='Après filtrage')
    plt.axvline(quantiles['prix_euro'][0], color='red', linestyle='--', label='5 %')
    plt.axvline(quantiles['prix_euro'][1], color='green', linestyle='--', label='95 %')
    plt.title("Distribution du prix avant/après filtrage")
    plt.xlabel("Prix (€)")
    plt.ylabel("Fréquence")
    plt.legend()

    plt.tight_layout()

    # 🔹 Sauvegarde du graphique si dossier_sortie est spécifié
    if dossier_sortie:
        os.makedirs(dossier_sortie, exist_ok=True)
        chemin_fichier = os.path.join(dossier_sortie, f"prix_moyen_1j_dispo_corr_com_{nom_fichier}.png")
        plt.savefig(chemin_fichier, dpi=300)
        print(f"\n✅ Graphique sauvegardé dans : {chemin_fichier}")

    # Affichage optionnel à l’écran
    if afficher_graph:
        plt.show()
    else:
        plt.close()

    # Moyenne des prix par champ_2
    result = data_filtrees.groupby(champ_2).mean().reset_index()

    # Renomer le champ
    result = result.rename(columns={'prix_euro': 'prix_moyen_1j_dispo_corr_com'})

    return result, data_exclues

def prix_moyen_1j_dispo_corr(data, champ_2, afficher_graph=True, dossier_sortie=None, nom_fichier=None):

    # Sélection des logements entiers et disponibles
    data = data[(data['availability_365'] != 0) & (data['room_type'] == 'Entire home/apt')]

    # Sélection des champs utiles
    data = data[['prix_euro', champ_2]]

    # Suppression des 2 % de valeurs extrêmes
    low, high = 0.02, 0.98
    numeric_cols = data.select_dtypes(include=['number']).columns

    # Calcul des bornes de quantiles
    quantiles = {col: (data[col].quantile(low), data[col].quantile(high)) for col in numeric_cols}

    # Masque des lignes conservées
    mask = data[numeric_cols].apply(lambda x: x.between(x.quantile(low), x.quantile(high))).all(axis=1)

    # Données exclues et filtrées
    data_exclues = data[~mask]
    data_filtrees = data[mask]

    # Affichage des valeurs exclues
    print("\n=== VALEURS EXCLUES (2 % extrêmes) ===")
    print(data_exclues)

    # Création du graphique
    plt.figure(figsize=(12, 5))

    # Boxplot avant/après filtrage
    plt.subplot(1, 2, 1)
    plt.boxplot(
        [data['prix_euro'], data_filtrees['prix_euro']],
        labels=['Avant filtrage', 'Après filtrage']
    )
    plt.title("Boxplot du prix avant/après suppression des valeurs extrêmes")
    plt.ylabel("Prix (€)")

    # Histogramme avant/après filtrage
    plt.subplot(1, 2, 2)
    plt.hist(data['prix_euro'], bins=30, alpha=0.6, label='Avant filtrage')
    plt.hist(data_filtrees['prix_euro'], bins=30, alpha=0.6, label='Après filtrage')
    plt.axvline(quantiles['prix_euro'][0], color='red', linestyle='--', label='5 %')
    plt.axvline(quantiles['prix_euro'][1], color='green', linestyle='--', label='95 %')
    plt.title("Distribution du prix avant/après filtrage")
    plt.xlabel("Prix (€)")
    plt.ylabel("Fréquence")
    plt.legend()

    plt.tight_layout()

    # 🔹 Sauvegarde de la figure si dossier_sortie est précisé
    if dossier_sortie:
        os.makedirs(dossier_sortie, exist_ok=True)
        chemin_fichier = os.path.join(dossier_sortie, f"prix_moyen_1j_dispo_corr_{nom_fichier}.png")
        plt.savefig(chemin_fichier, dpi=300)
        print(f"\n✅ Graphique sauvegardé dans : {chemin_fichier}")

    # Affichage optionnel
    if afficher_graph:
        plt.show()
    else:
        plt.close()

    # Moyenne des prix par champ_2
    result = data_filtrees.groupby(champ_2).mean().reset_index()

    # Renomer le champ
    result = result.rename(columns={'prix_euro': 'prix_moyen_1j_dispo_corr'})

    return result, data_exclues

def prix_moyen_log(data, champ_2, afficher_graph=True, dossier_sortie=None, nom_fichier=None):

    # Sélection des logements entiers et disponibles
    data_avant = data[(data['room_type'] == 'Entire home/apt')].copy()

    # Comptage des valeurs NaN dans prix_euro
    n_nan_prix = data_avant['prix_euro'].isna().sum()

    # Données avant et après suppression
    data_avant_prix = data_avant['prix_euro'].dropna()
    data_apres = data_avant.dropna(subset=['prix_euro'])
    data_apres_prix = data_apres['prix_euro']

    # Graphiques avant/après suppression des NaN
    if afficher_graph or dossier_sortie:
        plt.figure(figsize=(12, 5))

        # --- Boxplot avant/après ---
        plt.subplot(1, 2, 1)
        plt.boxplot([data_avant_prix, data_apres_prix],
                    labels=['Avant suppression NaN', 'Après suppression NaN'])
        plt.title("Boxplot des prix avant/après suppression des NaN")
        plt.ylabel("Prix (€)")

        # Ajout du nombre de NaN supprimés
        plt.text(1.5, max(data_avant_prix.max(), data_apres_prix.max()) * 0.95,
                 f"{n_nan_prix} valeurs NaN supprimées dans prix_euro",
                 ha='center', color='red', fontsize=10, fontweight='bold')

        # --- Histogramme avant/après ---
        plt.subplot(1, 2, 2)
        plt.hist(data_avant_prix, bins=30, alpha=0.6, label='Avant suppression')
        plt.hist(data_apres_prix, bins=30, alpha=0.6, label='Après suppression')
        plt.title("Distribution des prix avant/après suppression des NaN")
        plt.xlabel("Prix (€)")
        plt.ylabel("Fréquence")
        plt.legend()

        plt.tight_layout()

        # 🔹 Sauvegarde de la figure si dossier_sortie est précisé
        if dossier_sortie:
            os.makedirs(dossier_sortie, exist_ok=True)
            chemin_fichier = os.path.join(dossier_sortie, f"prix_moyen_log_{nom_fichier}.png")
            plt.savefig(chemin_fichier, dpi=300)
            print(f"\n✅ Graphique sauvegardé dans : {chemin_fichier}")

        # Affichage ou fermeture
        if afficher_graph:
            plt.show()
        else:
            plt.close()

    # Moyenne des prix par champ_2 (en ignorant les NaN déjà supprimés)
    result = data_apres.groupby(champ_2)['prix_euro'].mean().reset_index()

    # Renommer le champ
    result = result.rename(columns={'prix_euro': 'prix_moyen_log'})

    # Affichage console
    print(f"\n=== SUPPRESSION DES NaN ===")
    print(f"NaN dans 'prix_euro' : {n_nan_prix}\n")

    return result

def prix_moyen_1j_dispo(data, champ_2, afficher_graph=True, dossier_sortie=None, nom_fichier=None):

    # Sélection des logements entiers et disponibles
    data_avant = data[(data['availability_365'] != 0) & (data['room_type'] == 'Entire home/apt')].copy()

    # Comptage des valeurs NaN dans prix_euro
    n_nan_prix = data_avant['prix_euro'].isna().sum()

    # Données avant et après suppression
    data_avant_prix = data_avant['prix_euro'].dropna()
    data_apres = data_avant.dropna(subset=['prix_euro'])
    data_apres_prix = data_apres['prix_euro']

    # Graphiques avant/après suppression des NaN
    if afficher_graph or dossier_sortie:
        plt.figure(figsize=(12, 5))

        # --- Boxplot avant/après ---
        plt.subplot(1, 2, 1)
        plt.boxplot([data_avant_prix, data_apres_prix],
                    labels=['Avant suppression NaN', 'Après suppression NaN'])
        plt.title("Boxplot des prix avant/après suppression des NaN")
        plt.ylabel("Prix (€)")

        # Ajout du nombre de NaN supprimés
        plt.text(1.5, max(data_avant_prix.max(), data_apres_prix.max()) * 0.95,
                 f"{n_nan_prix} valeurs NaN supprimées dans prix_euro",
                 ha='center', color='red', fontsize=10, fontweight='bold')

        # --- Histogramme avant/après ---
        plt.subplot(1, 2, 2)
        plt.hist(data_avant_prix, bins=30, alpha=0.6, label='Avant suppression')
        plt.hist(data_apres_prix, bins=30, alpha=0.6, label='Après suppression')
        plt.title("Distribution des prix avant/après suppression des NaN")
        plt.xlabel("Prix (€)")
        plt.ylabel("Fréquence")
        plt.legend()

        plt.tight_layout()

        # 🔹 Sauvegarde de la figure si dossier_sortie est précisé
        if dossier_sortie:
            os.makedirs(dossier_sortie, exist_ok=True)
            chemin_fichier = os.path.join(dossier_sortie, f"prix_moyen_1j_dispo_{nom_fichier}.png")
            plt.savefig(chemin_fichier, dpi=300)
            print(f"\n✅ Graphique sauvegardé dans : {chemin_fichier}")

        # Affichage ou fermeture
        if afficher_graph:
            plt.show()
        else:
            plt.close()

    # Moyenne des prix par champ_2 (en ignorant les NaN déjà supprimés)
    result = data_apres.groupby(champ_2)['prix_euro'].mean().reset_index()

    # Renommer le champ
    result = result.rename(columns={'prix_euro': 'prix_moyen_1j_dispo'})

    # Affichage console
    print(f"\n=== SUPPRESSION DES NaN ===")
    print(f"NaN dans 'prix_euro' : {n_nan_prix}\n")

    return result

def prix_median_1j_dispo(data, champ_2):

    # Selection des logements entiers et disponibles
    data = data[(data['availability_365'] != 0) & (data['room_type'] == 'Entire home/apt')]

    # Suppression des lignes avec des valeurs manquantes dans les colonnes clés
    data = data.dropna(subset=['prix_euro', champ_2])

    # Moyenne des prix et réinitialisation de l'index
    result = data.groupby(champ_2)['prix_euro'].median().reset_index()

    # Renomer le champ
    result = result.rename(columns={'prix_euro': 'prix_median_1j_dispo'})

    return result

def prix_chambre(data, champ_2):

    # Sélection seulement des logements entiers
    data = data[data['room_type'] == 'Entire home/apt']
    
    # Suppression des lignes avec des valeurs manquantes dans les colonnes clés
    data = data.dropna(subset=['prix_euro', 'bedrooms', champ_2])

    # On garde uniquement les lignes avec un nombre de chambres valide
    data = data[(data['bedrooms'] > 0) & (data['prix_euro'] > 0)]

    # Création d'une colonne prix par chambre
    data['prix_par_chambre'] = data['prix_euro'] / data['bedrooms']

    # Moyenne du prix par chambre selon champ_2
    result = data.groupby(champ_2)['prix_par_chambre'].mean().reset_index()

    return result

def prix_logement(data, champ_2):

    # Selection seulement des logements entiers
    data = data[(data['room_type'] == 'Entire home/apt')]

    # Selection des champs
    data = data[['prix_euro',champ_2]].copy()

    # Suppression des lignes avec des valeurs manquantes dans les colonnes clés
    data = data.dropna(subset=['prix_euro', champ_2])

    # Chercher les lignes ou les conditions sont vrais ou fausses
    data['prix_logement_entier_inf_100_euro'] = data['prix_euro'] <= 100
    data['prix_logement_entier_sup_100_euro'] = data['prix_euro'] > 100
    data['prix_moyen'] = data['prix_euro']

    # Compter les vrais et faux pour chaque type de prix
    result = data.groupby(champ_2).agg({'prix_logement_entier_inf_100_euro': ['sum'],'prix_logement_entier_sup_100_euro': ['sum'],'prix_moyen': ['mean','count']})

    # Renommer les champs
    result.columns = ['prix_logement_entier_inf_100_euro','prix_logement_entier_sup_100_euro','prix_moyen','prix_total']

    # Calculer la part des prix
    result['part_logement_entier_inf_100_euro'] = result['prix_logement_entier_inf_100_euro'] / result['prix_total'] * 100
    result['part_logement_entier_sup_100_euro'] = result['prix_logement_entier_sup_100_euro'] / result['prix_total'] * 100

    # Réinitialiser l'index de la table
    result = result.reset_index()

    return result

def prix_logement_365(data, champ_2):

    # Selection seulement des logements entiers et des locations disponible ou moins 1 jour
    data = data[(data['room_type'] == 'Entire home/apt') & (data['availability_365'] != 0)]

    # Selection des champs
    data = data[['prix_euro',champ_2]].copy()

    # Suppression des lignes avec des valeurs manquantes dans les colonnes clés
    data = data.dropna(subset=['prix_euro', champ_2])

    # Chercher les lignes ou les conditions sont vrais ou fausses
    data['prix_logement_entier_inf_100_euro_365'] = data['prix_euro'] <= 100
    data['prix_logement_entier_sup_100_euro_365'] = data['prix_euro'] > 100
    
    # Compter les vrais et faux pour chaque type de prix
    result = data.groupby(champ_2).agg({'prix_logement_entier_inf_100_euro_365': ['sum'],'prix_logement_entier_sup_100_euro_365': ['sum'],'prix_euro': ['mean','count']})

    # Renommer les champs
    result.columns = ['prix_logement_entier_inf_100_euro_365','prix_logement_entier_sup_100_euro_365','prix_moyen','prix_total']

    # Calculer la part des prix
    result['part_logement_entier_inf_100_euro_365'] = result['prix_logement_entier_inf_100_euro_365'] / result['prix_total'] * 100
    result['part_logement_entier_sup_100_euro_365'] = result['prix_logement_entier_sup_100_euro_365'] / result['prix_total'] * 100

    # Suppression des colonnes
    result = result.drop(["prix_moyen", "prix_total"], axis=1)

    # Réinitialiser l'index de la table
    result = result.reset_index()

    return result
            
# ----------------------------------------------------
# 3️⃣ Comptage des licences
# ----------------------------------------------------

def licence(data, champ_2):

    # Selection seulement des logements entiers
    data = data[(data['room_type'] == 'Entire home/apt')]

    # Selection des champs
    data = data[['license',champ_2]]

    # Conversion le champ en texte
    data['license'] = data[['license']].astype(str)

    # Chercher les licences valide, mobilité, hotel et vide dans le champ des licence
    data['license_valide'] = data.apply(lambda x: str(x['license']).startswith(str(x[champ_2])) if pd.notnull(x['license']) and pd.notnull(x[champ_2]) else False,axis=1)
    data['licence_mobilite'] = data['license'].str.contains('mobi', na = False)
    data['licence_hotel'] = data['license'].str.contains('Exempt', na = False)                
    data['licence_vide'] = data['license'].isna()

    # Compter les vrais et faux pour chaque licence
    result = data.groupby(champ_2).agg({'license_valide': ['sum'],'licence_mobilite': ['sum'],'licence_hotel': ['sum'],'licence_vide': ['sum','count']})

    # Renommer les champs
    result.columns = ['license_valide', 'licence_mobilite', 'licence_hotel', 'licence_vide','licence_total']

    # Calculer les licences qui restent
    result['licence_autres'] = result['licence_total'] - (result['license_valide'] + result['licence_mobilite'] + result['licence_hotel'] + result['licence_vide'])

    # Réinitialiser l'index de la table
    result = result.reset_index()

    return result

# ----------------------------------------------------
# 4️⃣ Nombres d'annonces
# ----------------------------------------------------

def nbres_annonces(data, champ_2):

    # Selection des champs
    data = data[['room_type',champ_2]].copy()

    # Chercher les hotels, les chambres partagées,priveés et entiers dans le champ des type de chambres
    data['nbres_chambres_hotels'] = data['room_type'] =='Hotel room'
    data['nbres_logements_entiers'] = data['room_type'] =='Entire home/apt'
    data['nbres_chambres_privees'] = data['room_type'] =='Private room'
    data['nbres_chambres_partagees'] = data['room_type'] =='Shared room'

    # Compter les vrais et faux pour chaque type de logement
    result = data.groupby(champ_2).agg({'nbres_chambres_hotels': ['sum'],'nbres_logements_entiers': ['sum'],'nbres_chambres_privees': ['sum'],'nbres_chambres_partagees': ['sum','count']})

    # Renommer les champs
    result.columns = ['nbres_chambres_hotels', 'nbres_logements_entiers', 'nbres_chambres_privees', 'nbres_chambres_partagees','nombres_annonces']

    # Calculer la part de logement entier
    result['part_de_logements_entiers_(%)'] = result['nbres_logements_entiers'] / (result['nombres_annonces'] - result['nbres_chambres_hotels'])*100

    # Calculer le nombres d'annonces hors sans les hotels
    result["nombres_annonces_hors_hotels"] = result['nombres_annonces'] - result['nbres_chambres_hotels']

    # Réinitialiser l'index de la table
    result = result.reset_index()

    return result

def nbres_annonces_dispo_log_365(data, champ_2):

    # Selection des logements entiers et disponibles
    data = data[(data['availability_365'] != 0) & (data['room_type'] == 'Entire home/apt')]

    # Selection des champs utiles
    data = data[['availability_365', champ_2]]

    # Comptage et réinitialisation de l'index
    result = data.groupby(champ_2)['availability_365'].count().reset_index()

    # Renomer le champ
    result = result.rename(columns={'availability_365': 'nbres_annonces_dispo_log_365'})

    return result

def nbres_annonces_dispo_sauf_hotel_365(data, champ_2):

    # Selection des logements entiers et disponibles
    data = data[(data['availability_365'] != 0) & (data['room_type'] != 'Hotel room')]

    # Selection des champs utiles
    data = data[['availability_365', champ_2]]

    # Comptage et réinitialisation de l'index
    result = (data.groupby(champ_2)['availability_365'].count().reset_index())

    # Renomer le champ
    result = result.rename(columns={'availability_365': 'nbres_annonces_dispo_sauf_hotel_365'})

    return result

# ----------------------------------------------------
# 5️⃣ Nombre d’annonces par loueurs
# ----------------------------------------------------

def annonce_loueur(data, champ_2):

    # Enlever les hotels de la selection
    data = data[(data['room_type'] != 'Hotel room')]

    # Selection des champs
    data = data[['calculated_host_listings_count',champ_2]].copy()

    # Chercher les lignes ou les conditions sont vrais ou fausses
    data['annonces_par_loueur_(1)'] = data['calculated_host_listings_count'] == 1
    data['annonces_par_loueur_(2_a_9)'] = (data['calculated_host_listings_count'] >= 2) & (data['calculated_host_listings_count'] <= 9)
    data['annonces_par_loueur_(10_et_plus)'] = data['calculated_host_listings_count'] >= 10

    # Compter les vrais et faux pour chaque type de logement
    result = data.groupby(champ_2).agg({'annonces_par_loueur_(1)': ['sum'],'annonces_par_loueur_(2_a_9)': ['sum'],'annonces_par_loueur_(10_et_plus)': ['sum','count']})

    # Renommer les champs
    result.columns = ['annonces_par_loueur_(1)', 'annonces_par_loueur_(2_a_9)', 'annonces_par_loueur_(10_et_plus)', 'total_annonces']

    # Calculer la part des multiloueurs
    result['part_annonces_de_multiloueurs_(%)'] = (result['annonces_par_loueur_(2_a_9)'] + result['annonces_par_loueur_(10_et_plus)']) / result['total_annonces']*100

    # Réinitialiser l'index de la table
    result = result.reset_index()

    return result

def annonce_loueur_365(data, champ_2):
    
    # Enlever les hotels de la selection et selection  des locations disponible ou moins 1 jour
    data = data[(data['room_type'] != 'Hotel room') & (data['availability_365'] != 0)]

    # Selection des champs
    data = data[['calculated_host_listings_count',champ_2]].copy()

    # Chercher les lignes ou les conditions sont vrais ou fausses
    data['annonces_par_loueur_365_(1)'] = data['calculated_host_listings_count'] == 1
    data['annonces_par_loueur_365_(2_a_9)'] = (data['calculated_host_listings_count'] >= 2) & (data['calculated_host_listings_count'] <= 9)
    data['annonces_par_loueur_365_(10_et_plus)'] = data['calculated_host_listings_count'] >= 10

    # Compter les vrais et faux pour chaque type de logement
    result = data.groupby(champ_2).agg({'annonces_par_loueur_365_(1)': ['sum'],'annonces_par_loueur_365_(2_a_9)': ['sum'],'annonces_par_loueur_365_(10_et_plus)': ['sum','count']})

    # Renommer les champs
    result.columns = ['annonces_par_loueur_365_(1)', 'annonces_par_loueur_365_(2_a_9)', 'annonces_par_loueur_365_(10_et_plus)', 'total_annonces']

    # Calculer la part des multiloueurs
    result['part_annonces_de_multiloueurs_365_(%)'] = (result['annonces_par_loueur_365_(2_a_9)'] + result['annonces_par_loueur_365_(10_et_plus)']) / result['total_annonces']*100

    # Réinitialiser l'index de la table
    result = result.reset_index()

    return result

# ----------------------------------------------------
# 6️⃣ Nombres de commentaires
# ----------------------------------------------------

def nbre_commentaire(data, champ_2):

    # Enlever les hotels de la selection
    data = data[(data['room_type'] != 'Hotel room')]

    # Selection des champs
    data = data[['reviews_per_month',champ_2]].copy()

    # Remplacer les lignes vides par 0
    data = data.fillna(0)

    # Chercher les lignes ou les conditions sont vrais ou fausses
    data['nbres_commentaires_(0)'] = data['reviews_per_month'] == 0
    data['nbres_commentaires_(0_a_1.75)'] = (data['reviews_per_month'] > 0) & (data['reviews_per_month'] < 1.75)
    data['nbres_commentaires_(1.75_et_plus)'] = data['reviews_per_month'] >= 1.75

    # Compter les vrais et faux pour chaque type de commentaires
    result = data.groupby(champ_2).agg({'nbres_commentaires_(0)': ['sum'],'nbres_commentaires_(0_a_1.75)': ['sum'],'nbres_commentaires_(1.75_et_plus)': ['sum','count']})

    # Renommer les champs
    result.columns = ['nbres_commentaires_(0)', 'nbres_commentaires_(0_a_1.75)', 'nbres_commentaires_(1.75_et_plus)', 'nbres_commentaires_total'] 

    # Calculer la part des commentaires
    result['part_de_commentaires_(1.75_et_plus)'] = result['nbres_commentaires_(1.75_et_plus)'] / result['nbres_commentaires_total'] * 100

    # Réinitialiser l'index de la table
    result = result.reset_index()

    return result

def nbre_commentaire_12m(data, champ_2):
    
    # Enlever les hotels de la selection et prendre seulement des logements avec commentaires
    data = data[(data['room_type'] != 'Hotel room') & (data['number_of_reviews_ltm'] > 0 )]

    # Selection des champs
    data = data[['reviews_per_month',champ_2]].copy()

    # Remplacer les lignes vides par 0
    data = data.fillna(0)

    # Compter et réinitialiser l'index de la table
    result = (data.groupby(champ_2).agg(nbres_commentaires_12m=('reviews_per_month', 'count'))).reset_index()

    return result

def nbre_commentaire_12m_365(data, champ_2):    

    # Enlever les hotels de la selection et selectioner seulement des logements avec commentaires et disponibilité
    data = data[(data['room_type'] != 'Hotel room') & (data['number_of_reviews_ltm'] > 0) & (data['availability_365'] > 0)]

    # Selection des champs
    data = data[['reviews_per_month',champ_2]].copy()

    # Remplacer les lignes vides par 0
    data = data.fillna(0)

    # Compter et réinitialiser l'index de la table
    result = (data.groupby(champ_2).agg(nbres_commentaires_12m_365=('reviews_per_month', 'count'))).reset_index()

    return result

# ----------------------------------------------------
# 7️⃣ Comptage des disponibilités 
# ----------------------------------------------------

def disponibilite(data, champ_2):

    # Enlever les hotels de la selection
    data = data[(data['room_type'] != 'Hotel room')]

    # Selection des champs
    data = data[['availability_365',champ_2]].copy()

    # Chercher les lignes ou les conditions sont vrais ou fausses
    data['disponibilite_aucune'] = data['availability_365'] == 0
    data['disponibilite_inf_120_jours'] = (data['availability_365'] > 0) & (data['availability_365'] < 120)
    data['disponibilite_sup_120_jours'] = data['availability_365'] >= 120

    # Compter les vrais et faux pour chaque type de disponibilité
    result = data.groupby(champ_2).agg({'disponibilite_aucune': ['sum'],'disponibilite_inf_120_jours': ['sum'],'disponibilite_sup_120_jours': ['sum','count']})

    # Renommer les champs
    result.columns = ['disponibilite_aucune','disponibilite_inf_120_jours','disponibilite_sup_120_jours','dispo_total']

    # Calculer la part des dispos supérieur à 120 jours
    result['part_disponibilite_sup_120_jours_(%)'] = result['disponibilite_sup_120_jours'] / result['dispo_total'] * 100

    # Réinitialiser l'index de la table
    result = result.reset_index()

    return result