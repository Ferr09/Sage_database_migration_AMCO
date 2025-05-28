import os
import pyodbc
import csv

# === Paramètres utilisateur ===
chemin_fichier_access = r"db_sage_access\tables_sage_hyperix.accdb"  
dossier_sortie_csv = r"extraits\csv_extraits"      

# === Création du dossier de sortie s'il n'existe pas ===
os.makedirs(dossier_sortie_csv, exist_ok=True)

# === Connexion à la base Access ===
connexion = pyodbc.connect(
    fr"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={chemin_fichier_access};"
)
curseur = connexion.cursor()

# === Récupération de toutes les tables utilisateur ===
tables = curseur.tables(tableType='TABLE')
noms_tables = [table.table_name for table in tables]

print(f"Nombre total de tables détectées : {len(noms_tables)}")

# === Fonction d'exportation CSV ===
def exporter_table_vers_csv(nom_table):
    chemin_csv = os.path.join(dossier_sortie_csv, f"{nom_table}.csv")

    try:
        curseur.execute(f"SELECT * FROM [{nom_table}]")
        colonnes = [col[0] for col in curseur.description]
        donnees = curseur.fetchall()

        with open(chemin_csv, "w", newline="", encoding="utf-8-sig") as f_csv:
            writer = csv.writer(f_csv)
            writer.writerow(colonnes)
            writer.writerows(donnees)

        print(f"Exporté : {nom_table} ({len(donnees)} lignes)")
    except Exception as e:
        print(f"Erreur lors de l’exportation de la table {nom_table} : {e}")

# === Exportation de toutes les tables ===
for table in noms_tables:
    exporter_table_vers_csv(table)

# === Fermeture de la connexion ===
curseur.close()
connexion.close()

print("\nExtraction complète terminée.")
