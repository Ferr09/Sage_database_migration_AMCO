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
    """Charge une table de staging en CSV et renvoie un DataFrame Pandas."""
    filename = f"F_{table_name}_staging.csv"
    path = dossier_datalake_staging_sage / filename
    if not path.exists():
        raise FileNotFoundError(f"{path} non trouvé")
    return pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna('')


def generer_ventes_simplifie():
    """Génère le CSV de la table générale des ventes simplifiées."""
    d = _load_staging("DOCLIGNE")
    a = _load_staging("ARTICLE")
    f = _load_staging("FAMILLE")
    c = _load_staging("COMPTET")

    df = (
        d
        .merge(a[["AR_REF","FA_CODEFAMILLE"]], on="AR_REF", how="left", validate="many_to_one")
        .merge(f[["FA_CODEFAMILLE","FA_CENTRAL","FA_INTITULE"]], on="FA_CODEFAMILLE", how="left", validate="many_to_one")
        .merge(c[["CT_NUM","CT_INTITULE"]], on="CT_NUM", how="left", validate="many_to_one")
    )

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
            mapping = {
                "Famille du client":"FA_CODEFAMILLE","Code client":"CT_NUM","Raison sociale":"CT_INTITULE",
                "N° BL":"DL_PIECEBL","Date BL":"DL_DATEBL","Ref cde client":"AC_REFCLIENT",
                "code article":"AR_REF","Désignation":"DL_DESIGN",
                "famille article libellé":"FA_CENTRAL","sous-famille article libellé":"FA_INTITULE"
            }
            data[name] = df[mapping[name]] if name in mapping and mapping[name] in df.columns else pd.NA

    df_out = pd.DataFrame(data)
    os.makedirs(dossier_datalake_processed, exist_ok=True)
    sortie = dossier_datalake_processed / "tabla_generale_ventes.csv"
    df_out.to_csv(sortie, index=False, encoding="utf-8-sig")
    logger.info("CSV Ventes écrit : %s (%d lignes × %d colonnes)", sortie, *df_out.shape)


def generer_achats_simplifie():
    """Génère le CSV de la table générale des achats simplifiés."""
    # 1) Chargement et filtrage de DOCENTETE
    d_entete = _load_staging("DOCENTETE")
    c = _load_staging("COMPTET")
    mask_achats = d_entete["INT_CATCOMPTA"].astype(str).str.startswith("Achats")
    d_achats = d_entete.loc[mask_achats].copy()
    logger.info("DOCENTETE filtré (catégorie Achats) : %d lignes", len(d_achats))

    # 2) Jointure avec COMPTET
    df_entete = (
        d_achats.merge(
            c[[
                "CT_NUMPAYEUR","CT_INTITULE","CT_CONTACT","CT_ADRESSE",
                "CT_COMPLEMENT","CT_CODEPOSTAL","CT_VILLE",
                "CT_TELEPHONE","CT_TELECOPIE"
            ]],
            on="CT_NUMPAYEUR",
            how="left",
            validate="many_to_one"
        )
    )
    logger.info("Après merge avec COMPTET : %d lignes, %d colonnes", df_entete.shape[0], df_entete.shape[1])

    # 3) Préparation de la table partielle DOCLIGNE + ARTICLE + FAMILLE
    d_ligne = _load_staging("DOCLIGNE")
    a = _load_staging("ARTICLE")
    f = _load_staging("FAMILLE")
    df_ligne_par = (
        d_ligne
        .merge(a[["AR_REF","FA_CODEFAMILLE"]], on="AR_REF", how="left", validate="many_to_one")
        .merge(f[["FA_CODEFAMILLE","FA_CENTRAL","FA_INTITULE"]], on="FA_CODEFAMILLE", how="left", validate="many_to_one")
    )

    # 4) Détection de la colonne de pièce
    if "DL_PIECE" in df_ligne_par.columns:
        piece_col = "DL_PIECE"
    elif "DO_PIECE" in df_ligne_par.columns:
        piece_col = "DO_PIECE"
    else:
        raise KeyError("Aucune colonne de pièce ('DL_PIECE' ou 'DO_PIECE') trouvée dans DOCLIGNE")

    # 5) Jointure entre ENTETE et table partielle
    df_merge = (
        df_entete
        .merge(
            df_ligne_par[[piece_col,"AR_REF","DL_DESIGN","FA_CENTRAL","FA_INTITULE"]],
            left_on="DO_PIECE", right_on=piece_col,
            how="left", validate="many_to_many"
        )
    )

    # 6) Extraction année et mois de DO_DATE
    df_merge["Année"] = pd.to_datetime(df_merge["DO_DATE"], errors="coerce").dt.year.astype("Int64")
    df_merge["Mois"] = pd.to_datetime(df_merge["DO_DATE"], errors="coerce").dt.month.astype("Int64")

    # 7) Sélection et renommage des colonnes finales
    colonnes_a_garder = {
        "DO_REF":"Reference achat","CT_NUMPAYEUR":"Code fournisseur",
        "DO_DATE":"date achat","DO_PIECE":"Bon de commande",
        "FNT_QUANTITES":"Qté fact","FNT_MONTANTTOTALTAXES":"Total TVA",
        "FNT_TOTALHTNET":"Total HT","FNT_TOTALTTC":"Total TTC",
        "FNT_NETAPAYER":"NET A PAYER","INT_EXPEDIT":"Mode d'expedition",
        "CT_INTITULE":"Raison sociale","CT_CONTACT":"Contact","CT_ADRESSE":"Adresse",
        "CT_COMPLEMENT":"Complement adresse","CT_CODEPOSTAL":"Code postal",
        "CT_VILLE":"Ville","CT_TELEPHONE":"N° telephone","CT_TELECOPIE":"N° fax",
        "AR_REF":"code article","DL_DESIGN":"Désignation",
        "FA_CENTRAL":"famille article libellé","FA_INTITULE":"sous-famille article libellé",
        "Année":"Année","Mois":"Mois"
    }
    df_out = df_merge[list(colonnes_a_garder.keys())].rename(columns=colonnes_a_garder)

    # 8) Export CSV
    os.makedirs(dossier_datalake_processed, exist_ok=True)
    sortie = dossier_datalake_processed / "tabla_generale_achats.csv"
    df_out.to_csv(sortie, index=False, encoding="utf-8-sig")
    logger.info("CSV Achats écrit : %s (%d lignes × %d colonnes)", sortie, *df_out.shape)


if __name__ == "__main__":
    generer_ventes_simplifie()
    generer_achats_simplifie()
