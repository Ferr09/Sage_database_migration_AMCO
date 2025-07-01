#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Module pour générer les CSV simplifiés des tables générales Ventes et Achats,
en conservant la plupart des champs en varchar et en ajoutant des colonnes
nulles pour la structure de la table générale.
"""

import os
import sys
from pathlib import Path
import pandas as pd

try:
    from src.outils.chemins import dossier_datalake_staging_sage, dossier_datalake_processed
    from src.outils.logger import get_logger
except ImportError:
    projet_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(projet_root / "src"))
    from outils.chemins import dossier_datalake_staging_sage, dossier_datalake_processed
    from outils.logger import get_logger

logger = get_logger(__name__)

def _load_staging(table_name: str) -> pd.DataFrame:
    filename = f"F_{table_name}_staging.csv"
    path = dossier_datalake_staging_sage / filename
    if not path.exists():
        raise FileNotFoundError(f"{path} non trouvé")
    # Lecture sans conversion automatique de types
    df = pd.read_csv(path, encoding="utf-8-sig", dtype=str, low_memory=False).fillna('')
    logger.info("Chargé %s (%d lignes)", filename, len(df))
    return df

def generer_ventes_simplifie():
    d = _load_staging("DOCLIGNE")
    a = _load_staging("ARTICLE")
    f = _load_staging("FAMILLE")
    c = _load_staging("COMPTET")

    # Joins
    df = (
        d
        .merge(
            a[["AR_REF","FA_CODEFAMILLE"]],
            how="left", left_on="AR_REF", right_on="AR_REF", validate="many_to_one"
        )
        .merge(
            f[["FA_CODEFAMILLE","FA_CENTRAL","FA_INTITULE"]],
            how="left", left_on="FA_CODEFAMILLE", right_on="FA_CODEFAMILLE", validate="many_to_one"
        )
        .merge(
            c[["CT_NUM","CT_INTITULE"]],
            how="left", left_on="CT_NUM", right_on="CT_NUM", validate="many_to_one"
        )
    )

    # Sélection des colonnes et ajout des colonnes nulles
    df_out = pd.DataFrame({
        "N° Ligne doc":               df["DL_NO"].astype(int),
        "Famille du client":          df["FA_CODEFAMILLE"],           # varchar        
        "Code client":                df["CT_NUM"],
        "Raison sociale":             df["CT_INTITULE"],
        "N° BL":                      df["DL_PIECEBL"],
        "Date BL":                    df["DL_DATEBL"],
        "Ref cde client":             df["AC_REFCLIENT"],
        "code article":               df["AR_REF"],
        "N° Cde":                     df["DL_NO"].astype(int),
        "Désignation":                df["DL_DESIGN"],
        "famille article libellé":    df["FA_CENTRAL"],
        "sous-famille article libellé": df["FA_INTITULE"],
        "Qté fact":                   pd.to_numeric(df["DL_QTE"], errors="coerce"),
        "Prix Unitaire":              pd.to_numeric(df["DL_PRIXUNITAIRE"], errors="coerce"),
        "Tot HT":                     pd.to_numeric(df["DL_MONTANTHT"], errors="coerce"),
        "Année":                      pd.to_datetime(df["DO_DATE"], errors="coerce").dt.year.astype("Int64"),
        "Mois":                       pd.to_datetime(df["DO_DATE"], errors="coerce").dt.month.astype("Int64"),
        "responsable du dossier":     pd.NA,                          # colonne structurelle, null
        "représentant":               pd.NA,
        "N° facture":                 pd.NA,
        "date facture":               pd.NA,
        "Date demandée client":       pd.NA,
        "Date accusée AMCO":          pd.NA,
        "Numéro de plan":             pd.NA
    })

    os.makedirs(dossier_datalake_processed, exist_ok=True)
    sortie = dossier_datalake_processed / "tabla_generale_ventes.csv"
    df_out.to_csv(sortie, index=False, encoding="utf-8-sig")
    logger.info("CSV Ventes écrit : %s (%d lignes × %d colonnes)", sortie, *df_out.shape)

def generer_achats_simplifie():
    d  = _load_staging("DOCLIGNE")
    af = _load_staging("ARTFOURNISS").drop_duplicates(subset=["AF_REFFOURNISS"])
    a  = _load_staging("ARTICLE")
    f  = _load_staging("FAMILLE")
    c  = _load_staging("COMPTET")

    # Joins en partant de DOCLIGNE
    df = (
        d
        .merge(
            af[["AF_REFFOURNISS"]],
            how="left", left_on="AF_REFFOURNISS", right_on="AF_REFFOURNISS", validate="many_to_one"
        )
        .merge(
            a[["AR_REF","FA_CODEFAMILLE"]],
            how="left", left_on="AR_REF", right_on="AR_REF", validate="many_to_one"
        )
        .merge(
            f[["FA_CODEFAMILLE","FA_CENTRAL","FA_INTITULE"]],
            how="left", left_on="FA_CODEFAMILLE", right_on="FA_CODEFAMILLE", validate="many_to_one"
        )
        .merge(
            c[["CT_NUM","CT_INTITULE"]],
            how="left", left_on="CT_NUM", right_on="CT_NUM", validate="many_to_one"
        )
    )

    df_out = pd.DataFrame({
        "Famille du client":           df["FA_CODEFAMILLE"],
        "Ref cde fournisseur":         df["AF_REFFOURNISS"],
        "Code client":                 df["CT_NUM"],
        "Raison sociale":              df["CT_INTITULE"],
        "N° BC":                       df["DL_PIECEBC"],
        "Date BL":                     df["DL_DATEBL"],
        "Ref cde client":              df["AC_REFCLIENT"],
        "code article":                df["AR_REF"],
        "N° Cde":                      df["DL_NO"].astype(int),
        "Désignation":                 df["DL_DESIGN"],
        "famille article libellé":     df["FA_CENTRAL"],
        "sous-famille article libellé":df["FA_INTITULE"],
        "Qté fact":                    pd.to_numeric(df["DL_QTE"], errors="coerce"),
        "Prix Unitaire":               pd.to_numeric(df["DL_PRIXUNITAIRE"], errors="coerce"),
        "Tot HT":                      pd.to_numeric(df["DL_MONTANTHT"], errors="coerce"),
        "Année":                       pd.to_datetime(df["DO_DATE"], errors="coerce").dt.year.astype("Int64"),
        "Mois":                        pd.to_datetime(df["DO_DATE"], errors="coerce").dt.month.astype("Int64"),
        "responsable du dossier":      pd.NA,
        "représentant":                pd.NA,
        "N° facture":                  pd.NA,
        "date facture":                pd.NA,
        "Date demandée client":        pd.NA,
        "Date accusée AMCO":           pd.NA,
        "Numéro de plan":              pd.NA
    })

    os.makedirs(dossier_datalake_processed, exist_ok=True)
    sortie = dossier_datalake_processed / "tabla_generale_achats.csv"
    df_out.to_csv(sortie, index=False, encoding="utf-8-sig")
    logger.info("CSV Achats écrit : %s (%d lignes × %d colonnes)", sortie, *df_out.shape)

if __name__ == "__main__":
    generer_ventes_simplifie()
    generer_achats_simplifie()
