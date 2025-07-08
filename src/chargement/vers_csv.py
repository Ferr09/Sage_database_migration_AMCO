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
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig").fillna('')
    df.columns = df.columns.str.strip() # Normalisation des noms de colonnes
    return df

def generer_ventes_simplifie():
    """Génère le CSV de la table générale des ventes simplifiées, incluant le code famille."""
    logger.info("Début de la génération de la table générale des VENTES...")
    d = _load_staging("DOCLIGNE")
    a = _load_staging("ARTICLE")
    f = _load_staging("FAMILLE")
    c = _load_staging("COMPTET")

    df = (
        d
        .merge(a[["AR_REF","FA_CODEFAMILLE"]], on="AR_REF", how="left")
        .merge(f[["FA_CODEFAMILLE","FA_CENTRAL","FA_INTITULE"]], on="FA_CODEFAMILLE", how="left")
        .merge(c[["CT_NUM","CT_INTITULE"]], on="CT_NUM", how="left")
    )

    champs = [
        "N° Ligne doc","Famille du client","Code client","Raison sociale",
        "N° BL","Date BL","condition_livraison","Ref cde client","code article", "Code Famille",
        "N° Cde","Désignation","famille article libellé",
        "sous-famille article libellé","Qté fact","Prix Unitaire","Tot HT",
        "Année","Mois","responsable du dossier","représentant","N° facture",
        "date facture","Date demandée client","Date accusée AMCO","Numéro de plan"
    ]

    data = {}
    for name in champs:
        if name == "N° Ligne doc":
            data[name] = pd.to_numeric(df.get("DL_NO"), errors='coerce').astype('Int64')
        elif name == "N° Cde":
            data[name] = df.get("DO_PIECE")
        elif name in ("Qté fact", "Prix Unitaire", "Tot HT"):
            clé = {"Qté fact": "DL_QTE", "Prix Unitaire": "DL_PRIXUNITAIRE", "Tot HT": "DL_MONTANTHT"}[name]
            data[name] = pd.to_numeric(df.get(clé), errors="coerce")
        elif name == "Année":
            data[name] = pd.to_datetime(df.get("DO_DATE"), errors="coerce").dt.year.astype("Int64")
        elif name == "Mois":
            data[name] = pd.to_datetime(df.get("DO_DATE"), errors="coerce").dt.month.astype("Int64")
        else:
            mapping = {
                "Famille du client": "FA_CODEFAMILLE", 
                "Code client": "CT_NUM",
                "Raison sociale": "CT_INTITULE",
                "N° BL": "DL_PIECEBL",
                "Date BL": "DL_DATEBL",
                "Ref cde client": "AC_REFCLIENT",
                "code article": "AR_REF",
                "Code Famille": "FA_CODEFAMILLE", 
                "Désignation": "DL_DESIGN",
                "famille article libellé": "FA_CENTRAL",
                "sous-famille article libellé": "FA_INTITULE",
            }
            if name in mapping:
                data[name] = df.get(mapping[name])
            else:
                 data[name] = pd.NA

    df_out = pd.DataFrame(data)
    os.makedirs(dossier_datalake_processed, exist_ok=True)
    sortie = dossier_datalake_processed / "tabla_generale_ventes.csv"
    df_out.to_csv(sortie, index=False, encoding="utf-8-sig")
    logger.info("CSV Ventes écrit : %s (%d lignes × %d colonnes)", sortie, *df_out.shape)


def generer_achats_simplifie():
    """
    Génère la table générale des achats, incluant le code famille.
    Chaque ligne représente un en-tête de document d'achat (DO_PIECE),
    enrichi avec les informations de la première ligne d'article trouvée.
    """
    logger.info("Début de la génération de la table générale des ACHATS...")

    d_entete = _load_staging("DOCENTETE")
    c = _load_staging("COMPTET")
    
    mask_achats = d_entete["INT_CATCOMPTA"].astype(str).str.match(r"^Achats\b", case=False, na=False)
    d_achats = d_entete.loc[mask_achats].copy()
    d_achats['DO_PIECE'] = d_achats['DO_PIECE'].str.strip()
    
    df_entete = d_achats.merge(c.drop_duplicates(subset=["CT_NUMPAYEUR"]), on="CT_NUMPAYEUR", how="left")
    df_entete_unique = df_entete.drop_duplicates(subset=['DO_PIECE'], keep='first').copy()
    logger.info("En-têtes d'achat uniques à traiter : %d lignes", len(df_entete_unique))

    d_ligne = _load_staging("DOCLIGNE")
    a = _load_staging("ARTICLE")
    f = _load_staging("FAMILLE")
    
    d_ligne_enrichie = d_ligne.merge(
        a[["AR_REF","FA_CODEFAMILLE"]], on="AR_REF", how="left"
    ).merge(
        f[["FA_CODEFAMILLE","FA_CENTRAL","FA_INTITULE"]], on="FA_CODEFAMILLE", how="left"
    )
    d_ligne_enrichie['DO_PIECE'] = d_ligne_enrichie['DO_PIECE'].str.strip()
    
    df_ligne_premier = d_ligne_enrichie.drop_duplicates(subset=['DO_PIECE'], keep='first')
    logger.info("Première ligne de détail extraite pour %d pièces uniques", len(df_ligne_premier))
    
    # --- CORRECTION: Utilisation de suffixes pour gérer les colonnes dupliquées ---
    df_final = df_entete_unique.merge(
        df_ligne_premier,
        on='DO_PIECE',
        how='left',
        suffixes=('_entete', '_ligne')
    )
    logger.info("Taille de la table finale après jointure 'left' : %d lignes", len(df_final))

    # --- CORRECTION: Utiliser les noms de colonnes avec suffixes ---
    # La date de l'achat vient de l'en-tête, donc on utilise 'DO_DATE_entete'
    df_final["Année"] = pd.to_datetime(df_final.get("DO_DATE_entete"), errors="coerce").dt.year.astype("Int64")
    df_final["Mois"] = pd.to_datetime(df_final.get("DO_DATE_entete"), errors="coerce").dt.month.astype("Int64")

    data_export = {
        "Reference achat": df_final.get("DO_REF_entete"),
        "Code fournisseur": df_final.get("CT_NUMPAYEUR"),
        "date achat": df_final.get("DO_DATE_entete"),
        "Bon de commande": df_final.get("DO_PIECE"),
        "Qté fact": pd.to_numeric(df_final.get("DL_QTE"), errors="coerce"),
        "Total TVA": pd.to_numeric(df_final.get("FNT_MONTANTTOTALTAXES"), errors="coerce"),
        "Total HT": pd.to_numeric(df_final.get("FNT_TOTALHTNET"), errors="coerce"),
        "Total TTC": pd.to_numeric(df_final.get("FNT_TOTALTTC"), errors="coerce"),
        "NET A PAYER": pd.to_numeric(df_final.get("FNT_NETAPAYER"), errors="coerce"),
        "Mode d'expedition": df_final.get("INT_EXPEDIT"),
        "Raison sociale": df_final.get("CT_INTITULE"),
        "Contact": df_final.get("CT_CONTACT"),
        "Adresse": df_final.get("CT_ADRESSE"),
        "Complement adresse": df_final.get("CT_COMPLEMENT"),
        "Code postal": df_final.get("CT_CODEPOSTAL"),
        "Ville": df_final.get("CT_VILLE"),
        "N° telephone": df_final.get("CT_TELEPHONE"),
        "N° fax": df_final.get("CT_TELECOPIE"),
        # Les infos de l'article viennent de la ligne, donc pas de suffixe si le nom est unique
        "code article": df_final.get("AR_REF"), 
        "Désignation": df_final.get("DL_DESIGN"),
        "Code Famille": df_final.get("FA_CODEFAMILLE"),
        "famille article libellé": df_final.get("FA_CENTRAL"),
        "sous-famille article libellé": df_final.get("FA_INTITULE"),
        "Année": df_final.get("Année"),
        "Mois": df_final.get("Mois")
    }

    df_export = pd.DataFrame(data_export)
    
    os.makedirs(dossier_datalake_processed, exist_ok=True)
    sortie = dossier_datalake_processed / "tabla_generale_achats.csv"
    df_export.to_csv(sortie, index=False, encoding="utf-8-sig")
    logger.info("CSV Achats (avec première ligne) écrit : %s (%d lignes × %d colonnes)", sortie, *df_export.shape)


if __name__ == "__main__":
    try:
        generer_ventes_simplifie()
    except Exception as e:
        logger.error(f"Erreur lors de la génération des ventes : {e}", exc_info=True)
    
    try:
        generer_achats_simplifie()
    except Exception as e:
        logger.error(f"Erreur lors de la génération des achats : {e}", exc_info=True)