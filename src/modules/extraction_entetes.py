import os
import csv

# === Paramètres ===
dossier_source_csv = r"extraits\csv_extraits"        # Dossier contenant les .csv
dossier_sortie_txt = r"extraits\entetes_csv"    # Dossier pour stocker les .txt

# === Création du dossier de sortie s’il n’existe pas ===
os.makedirs(dossier_sortie_txt, exist_ok=True)

# === Parcourir tous les fichiers CSV ===
for fichier in os.listdir(dossier_source_csv):
    if fichier.lower().endswith(".csv"):
        chemin_csv = os.path.join(dossier_source_csv, fichier)
        nom_table = os.path.splitext(fichier)[0]
        chemin_txt = os.path.join(dossier_sortie_txt, f"{nom_table}_entetes.txt")

        try:
            with open(chemin_csv, "r", encoding="utf-8-sig") as f:
                lecteur = csv.reader(f)
                entetes = next(lecteur)  # Première ligne : en-têtes

            with open(chemin_txt, "w", encoding="utf-8") as f_txt:
                for colonne in entetes:
                    f_txt.write(colonne + "\n")

            print(f"En-têtes extraites : {nom_table} ({len(entetes)} colonnes)")

        except Exception as e:
            print(f"Erreur pour {nom_table} : {e}")
