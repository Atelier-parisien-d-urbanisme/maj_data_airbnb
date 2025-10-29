Ce projet vise à centraliser, préparer et analyser les données Airbnb afin de faciliter la compréhension des tendances du marché et d’optimiser la prise de décision. 
Il est composé de plusieurs scripts Python, chacun dédié à une étape précise du traitement des données : extraction, nettoyage, mise en forme et calcul d’indicateurs.

Le script exctraction_data_inside_airbnb.py permet de collecter les données brutes sur le site d'Inside Airbnb, tandis que preparation_data_airbnb.py et mise_en_forme_data_airbnb.py assurent la transformation et la normalisation des données. 
Les scripts listings_indicateurs_airbnb.py et reviews_indicateurs_airbnb.py génèrent des métriques détaillées sur les annonces et les commentaires, donnant un aperçu précis des performances des listings. 
Enfin, main_indicateurs_airbnb.py centralise les fonctions dans listings_indicateurs_airbnb.py pour produire un jeu de données final exploitable avec plusieurs indicateurs.

Ce projet constitue ainsi un outil complet pour l’analyse de données d'Inside Airbnb, de l’extraction initiale jusqu’à la génération d’indicateurs fiables.

╔══════════════════════════════════════════════╗
║      Dictionnaire des variables Airbnb       ║
╚══════════════════════════════════════════════╝
ville : Nom de la ville concernée
date : Date du nom du fichier (+/- date du scrapping)
nombres_annonces : Nombre total d'annonces scrapées
nbres_chambres_hotels : Nombre d'annonces de chambres d'hôtel
nombres_annonces_hors_hotels : Nombre d'annonces, hors chambres d'hôtel
nbres_logements_entiers : Nombre d'annonces de logements entiers
nbres_chambres_privees : Nombre d'annonces de chambres privées
nbres_chambres_partagees : Nombre d'annonces de chambres partagées
part_de_logements_entiers_(%) : Part des annonces de logements entiers, dans l'ensemble des annonces hors chambres d'hôtel
nbres_annonces_dispo_log_365 : Nombre d'annonces de logements entiers disponibles au moins 1 jour dans les 365 prochains jours
nbres_annonces_dispo_sauf_hotel_365 : Nombre d'annonces (hors chambres d'hôtels) disponibles au moins 1 jour dans les 365 prochains jours
annonces_par_loueur_(1) : Nombre d'annonces (hors chambres d'hôtels) rattachées à un loueur qui n'a que cette annonce sur Airbnb
annonces_par_loueur_(2_a_9) : Nombre d'annonces (hors chambres d'hôtels) rattachées à un loueur qui a entre 2 et 9 annonces sur Airbnb
annonces_par_loueur_(10_et_plus) : Nombre d'annonces (hors chambres d'hôtels) rattachées à un loueur qui a 10 annonces ou plus sur Airbnb
part_annonces_de_multiloueurs_(%) : Part des annonces de multiloueurs, dans l'ensemble des annonces hors chambres d'hôtels
annonces_par_loueur_365_(1) : Nombre d'annonces (hors chambres d'hôtels) rattachées à un loueur qui n'a que cette annonce sur Airbnb parmi les annonces disponibles au moins 1 jour dans les 365 prochains jours
annonces_par_loueur_365_(2_a_9) : Nombre d'annonces (hors chambres d'hôtels) rattachées à un loueur qui a entre 2 et 9 annonces sur Airbnb parmi les annonces disponibles au moins 1 jour dans les 365 prochains jours
annonces_par_loueur_365_(10_et_plus) : Nombre d'annonces (hors chambres d'hôtels) rattachées à un loueur qui a 10 annonces ou plus sur Airbnb parmi les annonces disponibles au moins 1 jour dans les 365 prochains jours
part_annonces_de_multiloueurs_365_(%) : Part des annonces de multiloueurs parmi les annonces disponibles au moins 1 jour dans les 365 prochains jours
nbres_commentaires_(0) : Nombre d'annonces (hors chambres d'hôtels) avec un nombre de commentaires mensuel moyen de 0
nbres_commentaires_(0_a_1.75) : Nombre d'annonces (hors chambres d'hôtels) avec un nombre de commentaires mensuel entre 0 et 1,75
nbres_commentaires_(1.75_et_plus) : Nombre d'annonces (hors chambres d'hôtels) avec un nombre de commentaires mensuel moyen supérieur ou égal à 1,75
part_de_commentaires_(1.75_et_plus) : Part des annonces avec un nombre de commentaires mensuel moyen supérieur ou égal à 1,75, dans l'ensemble des annonces hors chambres d'hôtels
nbres_commentaires_total : À voir dans le fichier commentaires
annonces_min_1_commentaire : Nombre total d'annonces (hors chambres d'hôtels) avec au moins un commentaire au cours des 12 derniers mois
annonces_365_min_1_commentaire : Nombre d'annonces (hors chambres d'hôtels) disponibles au moins 1 jour dans les 365 prochains jours avec au moins un commentaire au cours des 12 derniers mois
disponibilite_aucune : Nombre d'annonces dont la disponibilité dans les 365 prochains jours est de 0
disponibilite_inf_120_jours : Nombre d'annonces dont la disponibilité dans les 365 prochains jours est supérieure à 0 mais inférieure à 120 jours
disponibilite_sup_120_jours : Nombre d'annonces dont la disponibilité dans les 365 prochains jours est supérieure ou égale à 120 jours
part_disponibilite_sup_120_jours_(%) : Part des annonces dont la disponibilité dans les 365 prochains jours est supérieure ou égale à 120 jours
prix_logement_entier_inf_100_euro : Nombre d'annonces de logements entiers dont le prix de la nuitée est inférieur ou égal à 100 €
prix_logement_entier_sup_100_euro : Nombre d'annonces de logements entiers dont le prix de la nuitée est strictement supérieur à 100 €
part_logement_entier_inf_100_euro : Part des annonces de logements entiers dont le prix de la nuitée est inférieur ou égal à 100 €
part_logement_entier_sup_100_euro : Part des annonces de logements entiers dont le prix de la nuitée est strictement supérieur à 100 €
prix_moyen : Prix moyen en euros des annonces de logements entiers
prix_moyen_1j_dispo : Prix moyen des annonces de logements entiers disponibles au moins 1 jour dans les 365 prochains jours
prix_moyen_1j_dispo_cor : Prix moyen des annonces disponibles au moins 1 jour, en enlevant les 5% plus élevés et les 5% plus bas
prix_moyen_1j_dispo_cor_comm : Prix moyen des annonces disponibles au moins 1 jour et ayant au moins 1 commentaire, en enlevant les 5% plus élevés et les 5% plus bas
date_taux_de_change : Date du taux de change
license_valide : Nombre d'annonces dont le numéro d'enregistrement commence par le code Insee de la commune
licence_mobilite : Nombre d'annonces mentionnant "mobi" (bail mobilité)
licence_vide : Nombre d'annonces sans indication du numéro d'enregistrement
licence_hotel : Nombre d'annonces mentionnant "Exempt" (exempt - hôtel)
licence_autres : Nombre d'annonces n'entrant pas dans les cas précédents
