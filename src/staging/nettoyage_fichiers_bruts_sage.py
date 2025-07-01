#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Module de nettoyage des CSV bruts Sage et export vers CSV staging.
Au lieu d’Excel, on génère <nom_table>_staging.csv dans data_lake/staging/sage/.
"""

import re
import pandas as pd
from pathlib import Path
import os

# --------------------------------------------------------------------
# Importation des chemins absolus depuis chemins.py
# --------------------------------------------------------------------
try:
    from src.outils.chemins import (
        dossier_datalake_raw_sage,
        dossier_datalake_staging_sage
    )
except ImportError:
    # Fallback si exécuté hors du contexte src/
    projet_root = Path(__file__).resolve().parents[2]
    dossier_datalake_raw_sage     = projet_root / "data_lake" / "raw"     / "sage"
    dossier_datalake_staging_sage = projet_root / "data_lake" / "staging" / "sage"

# --------------------------------------------------------------------
# Vérification du dossier source et création du dossier de sortie
# --------------------------------------------------------------------
if not dossier_datalake_raw_sage.is_dir():
    raise FileNotFoundError(f"Le dossier source n'existe pas : {dossier_datalake_raw_sage}")

dossier_datalake_staging_sage.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------
# Configuration des conversions de types et colonnes spécifiques
# --------------------------------------------------------------------
types_tables = {
    "F_DOCENTETE": {
        "DO_DATE": "datetime",
        "DO_DATELIVR": "datetime",
        "FNT_TOTALHT": "float"
    },
    "F_DOCLIGNE": {
        "DL_DATEBL": "datetime",
        "DL_DATEBC": "datetime",
        "DL_DATEPL": "datetime",
        "DL_QTE": "float",
        "DL_PRIXUNITAIRE": "float",
        "DL_MONTANTHT": "float"
    }
}

colonnes_monnaie_tables = {
    "F_DOCENTETE": ["FNT_TOTALHT"],
    "F_DOCLIGNE": ["DL_PRIXUNITAIRE", "DL_MONTANTHT"]
}

dtype_tables = {
    "F_DOCLIGNE": {
        "AC_REFCLIENT": str,
        "AF_REFFOURNISS": str
    }
}

# --------------------------------------------------------------------
# Fonction principale de nettoyage et export vers CSV
# --------------------------------------------------------------------
def nettoyer_et_exporter_csv(chemin_csv: Path, nom_table: str):
    try:
        # Lecture du fichier CSV brut
        dtype = dtype_tables.get(nom_table, None)
        df = pd.read_csv(chemin_csv, encoding="utf-8-sig", dtype=dtype, low_memory=False)

        # Suppression des lignes vides ou nulles
        df_clean = df.dropna(how='all')
        df_clean = df_clean.loc[~(df_clean.isna() | (df_clean == 0)).all(axis=1)]

        # Cas particulier : F_ARTFOURNISS sans AF_REFFOURNISS
        if nom_table == "F_ARTFOURNISS" and "AF_REFFOURNISS" in df_clean.columns:
            avant = len(df_clean)
            df_clean = df_clean[df_clean["AF_REFFOURNISS"].notna()]
            print(f"{nom_table} : {avant - len(df_clean)} ligne(s) sans AF_REFFOURNISS supprimée(s)")

        # Cas particulier : extraction de BL pour F_DOCLIGNE
        if nom_table == "F_DOCLIGNE":
            if "DL_PIECEBL" in df_clean.columns and "DL_DESIGN" in df_clean.columns:
                def extraire_bl(row):
                    val = row["DL_PIECEBL"]
                    texte = str(row["DL_DESIGN"]).replace('\xa0', ' ')
                    texte = re.sub(r"\s+", " ", texte).strip()
                    if pd.isna(val) or str(val).strip() == "":
                        match = re.search(r"LIVREE?S?\s+PAR\s+BL\s*(?:N°?|N)?\s*(\d+)",
                                          texte, re.IGNORECASE)
                        if match:
                            return match.group(1)
                    return val

                ancienne = df_clean["DL_PIECEBL"].copy()
                df_clean["DL_PIECEBL"] = df_clean.apply(extraire_bl, axis=1)
                modif = ((ancienne.isna() | (ancienne == "")) &
                         (ancienne != df_clean["DL_PIECEBL"])).sum()
                print(f"{nom_table} : {modif} ligne(s) mise(s) à jour dans DL_PIECEBL")

        # Conversion des types définis
        conversions = types_tables.get(nom_table, {})
        for col, dtype in conversions.items():
            if col in df_clean.columns:
                try:
                    if dtype == "datetime":
                        df_clean[col] = pd.to_datetime(df_clean[col], errors="coerce")
                    else:
                        df_clean[col] = df_clean[col].astype(dtype)
                except Exception as e:
                    print(f"Erreur conversion {nom_table}.{col} : {e}")

        # Si vide après nettoyage, on ignore
        if df_clean.empty:
            print(f"Ignoré : {nom_table} (aucune ligne après nettoyage)")
            return

        # Export vers CSV staging
        fichier_sortie = dossier_datalake_staging_sage / f"{nom_table}_staging.csv"
        df_clean.to_csv(fichier_sortie, index=False, encoding="utf-8-sig")
        print(f"Exporté : {nom_table} → {fichier_sortie} ({len(df_clean)} lignes)")

    except Exception as e:
        print(f"Erreur pour {nom_table} : {e}")

# --------------------------------------------------------------------
# Exécution pour tous les CSV bruts du dossier raw/sage
# --------------------------------------------------------------------
def main():
    fichiers = [f for f in dossier_datalake_raw_sage.iterdir() if f.suffix.lower() == ".csv"]
    print(f"Détection de {len(fichiers)} fichiers CSV bruts dans {dossier_datalake_raw_sage}")
    for fichier in fichiers:
        nom_table = fichier.stem
        nettoyer_et_exporter_csv(fichier, nom_table)

if __name__ == "__main__":
    main()
