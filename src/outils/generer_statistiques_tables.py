import os
import pandas as pd

# === Dossiers ===
from src.outils.chemins import (
    dossier_tables_statistiques,
    dossier_datalake_raw_sage
) 

# Création du dossier pour les statistiques
os.makedirs(dossier_tables_statistiques, exist_ok=True)

# Listes des résultats
tables_vides = []
tables_non_vides = []
tables_plus_de_10 = []
tables_plus_de_100 = []

# Analyse brute sans nettoyage
fichiers = [f for f in os.listdir(dossier_datalake_raw_sage) if f.endswith(".csv")]
print(f"{len(fichiers)} fichier(s) détecté(s) dans le dossier : {dossier_datalake_raw_sage}\n")

for fichier in fichiers:
    nom_table = os.path.splitext(fichier)[0]
    chemin_csv = os.path.join(dossier_datalake_raw_sage, fichier)

    try:
        df = pd.read_csv(chemin_csv, encoding="utf-8-sig")
        n_lignes = len(df)

        if n_lignes == 0:
            tables_vides.append(nom_table)
        else:
            tables_non_vides.append(nom_table)
            if n_lignes > 10:
                tables_plus_de_10.append(nom_table)
            if n_lignes > 100:
                tables_plus_de_100.append(nom_table)

        print(f"{nom_table} : {n_lignes} ligne(s) brutes")

    except Exception as e:
        print(f"Erreur pour {nom_table} : {e}")

# === Sauvegarde des fichiers de statistiques ===
def enregistrer_liste(nom_fichier, liste):
    with open(os.path.join(dossier_tables_statistiques, nom_fichier), "w", encoding="utf-8") as f:
        for t in sorted(liste):
            f.write(t + "\n")

enregistrer_liste("tables_vides.txt", tables_vides)
enregistrer_liste("tables_non_vides.txt", tables_non_vides)
enregistrer_liste("tables_plus_de_10_lignes.txt", tables_plus_de_10)
enregistrer_liste("tables_plus_de_100_lignes.txt", tables_plus_de_100)

print(f"\nStatistiques enregistrées dans : {dossier_tables_statistiques}")
