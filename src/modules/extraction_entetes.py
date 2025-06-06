#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
from pathlib import Path
import os

# --------------------------------------------------------------------
# Importation des chemins absolus depuis chemins.py
# --------------------------------------------------------------------
from src.outils.chemins import dossier_csv_extraits, dossier_xlsx_propres, racine_projet

# --------------------------------------------------------------------
# Définition des dossiers source et sortie en chemins absolus
# --------------------------------------------------------------------
dossier_source_csv = dossier_csv_extraits
dossier_sortie_txt = racine_projet / "statistiques" / "entetes_csv"

# --------------------------------------------------------------------
# Création du dossier de sortie s’il n’existe pas
# --------------------------------------------------------------------
dossier_sortie_txt.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------
# Parcourir tous les fichiers CSV du dossier source
# --------------------------------------------------------------------
for fichier in os.listdir(dossier_source_csv):
    if fichier.lower().endswith(".csv"):
        chemin_csv = dossier_source_csv / fichier
        nom_table = Path(fichier).stem
        chemin_txt = dossier_sortie_txt / f"{nom_table}_entetes.txt"

        try:
            # Lecture de la première ligne pour obtenir les en-têtes
            with open(chemin_csv, "r", encoding="utf-8-sig") as f:
                lecteur = csv.reader(f)
                entetes = next(lecteur)  # Première ligne : en-têtes

            # Écriture des en-têtes dans le fichier .txt
            with open(chemin_txt, "w", encoding="utf-8") as f_txt:
                for colonne in entetes:
                    f_txt.write(colonne + "\n")

            print(f"En-têtes extraites : {nom_table} ({len(entetes)} colonnes)")

        except Exception as e:
            print(f"Erreur pour {nom_table} : {e}")
