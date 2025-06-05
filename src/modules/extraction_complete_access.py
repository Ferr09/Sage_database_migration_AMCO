#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import pyodbc
from pathlib import Path
import os

# --------------------------------------------------------------------
# Importation des chemins absolus depuis outils.chemins
# --------------------------------------------------------------------
from outils.chemins import racine_projet, dossier_csv_extraits

# --------------------------------------------------------------------
# Paramètres utilisateur basés sur des chemins absolus
# --------------------------------------------------------------------
chemin_fichier_access = racine_projet / "db_sage_access" / "tables_sage_hyperix.accdb"
dossier_sortie_csv   = dossier_csv_extraits

# --------------------------------------------------------------------
# Création du dossier de sortie s'il n'existe pas
# --------------------------------------------------------------------
dossier_sortie_csv.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------
# Connexion à la base Access
# --------------------------------------------------------------------
connexion = pyodbc.connect(
    fr"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={chemin_fichier_access};"
)
curseur = connexion.cursor()

# --------------------------------------------------------------------
# Récupération de toutes les tables utilisateur
# --------------------------------------------------------------------
tables = curseur.tables(tableType='TABLE')
noms_tables = [table.table_name for table in tables]

print(f"Nombre total de tables détectées : {len(noms_tables)}")

# --------------------------------------------------------------------
# Fonction d'exportation d'une table vers un fichier CSV
# --------------------------------------------------------------------
def exporter_table_vers_csv(nom_table):
    """
    Exporte la table Access nom_table dans un fichier CSV dans dossier_sortie_csv.
    """
    chemin_csv = dossier_sortie_csv / f"{nom_table}.csv"

    try:
        # Exécuter la requête pour récupérer toutes les lignes
        curseur.execute(f"SELECT * FROM [{nom_table}]")
        colonnes = [col[0] for col in curseur.description]
        donnees = curseur.fetchall()

        # Écriture dans le CSV avec BOM pour l'encodage UTF-8
        with open(chemin_csv, "w", newline="", encoding="utf-8-sig") as f_csv:
            writer = csv.writer(f_csv)
            writer.writerow(colonnes)
            writer.writerows(donnees)

        print(f"Exporté : {nom_table} ({len(donnees)} lignes)")
    except Exception as e:
        print(f"Erreur lors de l’exportation de la table {nom_table} : {e}")

# --------------------------------------------------------------------
# Exportation de toutes les tables détectées
# --------------------------------------------------------------------
for table in noms_tables:
    exporter_table_vers_csv(table)

# --------------------------------------------------------------------
# Fermeture de la connexion Access
# --------------------------------------------------------------------
curseur.close()
connexion.close()

print("\nExtraction complète terminée.")
