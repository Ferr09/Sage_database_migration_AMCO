#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
    return pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna('')

def generer_ventes_simplifie():
    d = _load_staging("DOCLIGNE")
    a = _load_staging("ARTICLE")
    f = _load_staging("FAMILLE")
    c = _load_staging("COMPTET")

    df = (d
        .merge(a[["AR_REF","FA_CODEFAMILLE"]], on="AR_REF", how="left", validate="many_to_one")
        .merge(f[["FA_CODEFAMILLE","FA_CENTRAL","FA_INTITULE"]], on="FA_CODEFAMILLE", how="left", validate="many_to_one")
        .merge(c[["CT_NUM","CT_INTITULE"]], on="CT_NUM", how="left", validate="many_to_one")
    )

    # Liste de tous les champs attendus dans la table générale
    champs = [
        "N° Ligne doc","Famille du client","Code client","Raison sociale",
        "N° BL","Date BL","condition_livraison","Ref cde client","code article",
        "N° Cde","Désignation","famille article libellé",
        "sous-famille article libellé","Qté fact","Prix Unitaire","Tot HT",
        "Année","Mois","responsable du dossier","représentant","N° facture",
        "date facture","Date demandée client","Date accusée AMCO","Numéro de plan"
    ]

    data = {}
    for name in champs:
        if name == "N° Ligne doc":
            data[name] = df["DL_NO"].astype(int)
        elif name == "N° Cde":
            data[name] = df["DL_NO"].astype(int)
        elif name in ("Qté fact","Prix Unitaire","Tot HT"):
            clé = {"Qté fact":"DL_QTE","Prix Unitaire":"DL_PRIXUNITAIRE","Tot HT":"DL_MONTANTHT"}[name]
            data[name] = pd.to_numeric(df[clé], errors="coerce")
        elif name == "Année":
            data[name] = pd.to_datetime(df["DO_DATE"], errors="coerce").dt.year.astype("Int64")
        elif name == "Mois":
            data[name] = pd.to_datetime(df["DO_DATE"], errors="coerce").dt.month.astype("Int64")
        else:
            # columnas directas de df o nulas
            mapping = {
                "Famille du client":"FA_CODEFAMILLE","Code client":"CT_NUM","Raison sociale":"CT_INTITULE",
                "N° BL":"DL_PIECEBL","Date BL":"DL_DATEBL","Ref cde client":"AC_REFCLIENT",
                "code article":"AR_REF","Désignation":"DL_DESIGN",
                "famille article libellé":"FA_CENTRAL","sous-famille article libellé":"FA_INTITULE"
            }
            if name in mapping and mapping[name] in df.columns:
                data[name] = df[mapping[name]]
            else:
                data[name] = pd.NA

    df_out = pd.DataFrame(data)
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

    # 1) On élimine les lignes sans Ref cde fournisseur (AF_REFFOURNISS vide ou null)
    mask_fourn = d["AF_REFFOURNISS"].notna() & (d["AF_REFFOURNISS"].astype(str).str.strip() != "")
    d_filtree = d.loc[mask_fourn].copy()
    logger.info("F_DOCLIGNE filtré pour achats : %d lignes restantes", len(d_filtree))

    # 2) Joins
    df = (
        d_filtree
        .merge(af[["AF_REFFOURNISS"]], on="AF_REFFOURNISS", how="left", validate="many_to_one")
        .merge(a[["AR_REF","FA_CODEFAMILLE"]], on="AR_REF", how="left", validate="many_to_one")
        .merge(f[["FA_CODEFAMILLE","FA_CENTRAL","FA_INTITULE"]], on="FA_CODEFAMILLE", how="left", validate="many_to_one")
        .merge(c[["CT_NUM","CT_INTITULE"]], on="CT_NUM", how="left", validate="many_to_one")
    )

    # 3) Liste des champs à conserver (sans "Ref cde client")
    champs = [
        "Famille du client","Ref cde fournisseur","Code client","Raison sociale",
        "N° BC","Date BL","condition_livraison","code article",
        "N° Cde","Désignation","famille article libellé",
        "sous-famille article libellé","Qté fact","Prix Unitaire","Tot HT",
        "Année","Mois","responsable du dossier","représentant","N° facture",
        "date facture","Date demandée client","Date accusée AMCO","Numéro de plan"
    ]

    # 4) Construction du DataFrame de sortie
    data = {}
    for name in champs:
        if name == "N° Cde":
            data[name] = df["DL_NO"].astype(int)
        elif name in ("Qté fact","Prix Unitaire","Tot HT"):
            clé = {"Qté fact":"DL_QTE","Prix Unitaire":"DL_PRIXUNITAIRE","Tot HT":"DL_MONTANTHT"}[name]
            data[name] = pd.to_numeric(df[clé], errors="coerce")
        elif name == "Année":
            data[name] = pd.to_datetime(df["DO_DATE"], errors="coerce").dt.year.astype("Int64")
        elif name == "Mois":
            data[name] = pd.to_datetime(df["DO_DATE"], errors="coerce").dt.month.astype("Int64")        
        else:
            mapping = {
                "Famille du client":"FA_CODEFAMILLE",
                "Ref cde fournisseur":"AF_REFFOURNISS",
                "Code client":"CT_NUM",
                "Raison sociale":"CT_INTITULE",
                "N° BC":"DL_PIECEBC",
                "Date BL":"DL_DATEBL",
                "condition_livraison":"condition_livraison",
                "code article":"AR_REF",
                "Désignation":"DL_DESIGN",
                "famille article libellé":"FA_CENTRAL",
                "sous-famille article libellé":"FA_INTITULE"
            }
            if name in mapping and mapping[name] in df.columns:
                data[name] = df[mapping[name]]
            else:
                data[name] = pd.NA

    df_out = pd.DataFrame(data)

    # 5) Export CSV
    os.makedirs(dossier_datalake_processed, exist_ok=True)
    sortie = dossier_datalake_processed / "tabla_generale_achats.csv"
    df_out.to_csv(sortie, index=False, encoding="utf-8-sig")
    logger.info("CSV Achats écrit : %s (%d lignes × %d colonnes)", sortie, *df_out.shape)

if __name__ == "__main__":
    generer_ventes_simplifie()
    generer_achats_simplifie()
