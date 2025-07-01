# -*- coding: utf-8 -*-
"""
Analyse de la qualité des données hors-connexion,
script situé dans src/outils/analyse_qualite_offline.py

Génère un rapport TXT en deux sections :
1. Statistiques générales sur le nombre et le taux de tables vides.
2. Taux de complétude par colonne des tables représentatives,
   en utilisant tables_vides.txt, tables_non_vides.txt et les fichiers XLSX filtrés.
"""

import sys
from pathlib import Path
import pandas as pd

# 1. Déterminer la racine du projet (2 niveaux au-dessus de src/outils)
chemin_script = Path(__file__).resolve()
racine_projet = chemin_script.parents[2]
sys.path.append(str(racine_projet / "src"))

# 2. Importer les chemins configurés
from src.outils.chemins import (
    dossier_tables_statistiques,
    dossier_datalake_staging_sage,
    dossier_analyse_structure
)

# 3. Charger les listes de tables vides et non-vides
f_vides     = dossier_tables_statistiques / "tables_vides.txt"
f_non_vides = dossier_tables_statistiques / "tables_non_vides.txt"

tables_vides     = [t.strip().upper() for t in f_vides.read_text(encoding="utf-8").splitlines() if t.strip()]
tables_non_vides = [t.strip().upper() for t in f_non_vides.read_text(encoding="utf-8").splitlines() if t.strip()]

total_tables = len(tables_vides) + len(tables_non_vides)
nb_vides     = len(tables_vides)
taux_vides   = (nb_vides / total_tables * 100) if total_tables else 0.0

# 4. Définir les tables et colonnes représentatives
colonnes_representatives = {
    "VENTES": {
        "F_DOCLIGNE_staging": [
            "DL_NO", "AC_REFCLIENT", "AR_REF", "CT_NUM",
            "DL_PIECEBL", "DL_DATEBL", "DL_DESIGN",
            "DL_QTE", "DL_PRIXUNITAIRE", "DL_MONTANTHT", "DO_DATE"
        ],
        "F_FAMILLE_staging": ["FA_CODEFAMILLE", "FA_CENTRAL", "FA_INTITULE"],
        "F_COMPTET_staging": ["CT_NUM", "CT_INTITULE"],
        "F_ARTICLE_staging": ["AR_REF", "FA_CODEFAMILLE"]
    },
    "ACHATS": {
        "F_DOCLIGNE_staging": [
            "DL_NO", "AF_REFFOURNISS", "AR_REF", "CT_NUM",
            "DL_PIECEBC", "DL_DATEBL", "AC_REFCLIENT",
            "DL_DESIGN", "DL_QTE", "DL_PRIXUNITAIRE", "DL_MONTANTHT", "DO_DATE"
        ],
        "F_ARTFOURNISS_staging": ["AF_REFFOURNISS", "AR_REF"],
        "F_FAMILLE_staging": ["FA_CODEFAMILLE", "FA_CENTRAL", "FA_INTITULE"],
        "F_COMPTET_staging": ["CT_NUM", "CT_INTITULE"],
        "F_ARTICLE_staging": ["AR_REF", "FA_CODEFAMILLE"]
    }
}

# 5. Calculer la complétude
resultats = []

# A. Tables vides → taux 0 % pour toutes les colonnes
for sch, tbls in colonnes_representatives.items():
    for table, cols in tbls.items():
        if table in tables_vides:
            for col in cols:
                resultats.append({
                    "schéma": sch,
                    "table": table,
                    "colonne": col,
                    "total_lignes": 0,
                    "valeurs_non_nulles": 0,
                    "taux_completude_%": 0.0
                })

# B. Tables non-vides → lecture des XLSX
for xlsx in dossier_datalake_staging_sage.glob("*.xlsx"):
    nom_table = xlsx.stem.upper()
    # déterminer le schéma
    schema = next((s for s, tbls in colonnes_representatives.items() if nom_table in tbls), None)
    if not schema or nom_table not in tables_non_vides:
        continue
    df = pd.read_excel(xlsx)
    total_lignes = len(df)
    for col in colonnes_representatives[schema][nom_table]:
        if total_lignes > 0 and col in df.columns:
            non_nuls = int(df[col].notna().sum())
            taux_col = non_nuls / total_lignes * 100
        else:
            non_nuls = 0
            taux_col = 0.0
        resultats.append({
            "schéma": schema,
            "table": nom_table,
            "colonne": col,
            "total_lignes": total_lignes,
            "valeurs_non_nulles": non_nuls,
            "taux_completude_%": round(taux_col, 2)
        })

# 6. Écriture du rapport TXT dans statistiques/analyse_structure_db
chemin_txt = dossier_analyse_structure / "rapport_completude_offline.txt"
chemin_txt.parent.mkdir(parents=True, exist_ok=True)

with chemin_txt.open("w", encoding="utf-8") as txt:
    txt.write("=== Statistiques générales ===\n")
    txt.write(f"Total des tables       : {total_tables}\n")
    txt.write(f"Nombre de tables vides : {nb_vides}\n")
    txt.write(f"Taux de tables vides   : {taux_vides:.2f} %\n\n")
    txt.write("=== Complétude par colonne ===\n")
    txt.write("Schéma | Table | Colonne | Total lignes | Non nuls | Taux complétude (%)\n")
    txt.write("-" * 90 + "\n")
    for r in resultats:
        txt.write(
            f"{r['schéma']} | {r['table']} | {r['colonne']} | "
            f"{r['total_lignes']} | {r['valeurs_non_nulles']} | {r['taux_completude_%']:.2f}\n"
        )

print(f"✔ Rapport généré dans : {chemin_txt}")
