#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import pandas as pd
import os
import sys

# Charger les chemins du projet
try:
    from src.outils.chemins import dossier_datalake_processed
except ImportError:
    projet_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    sys.path.insert(0, os.path.join(projet_root, "src"))
    from outils.chemins import dossier_datalake_processed

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

# Répertoires de sortie basés sur dossier_datalake_processed
VENTES_DIR = os.path.join(dossier_datalake_processed, "ventes")
ACHATS_DIR = os.path.join(dossier_datalake_processed, "achats")

os.makedirs(VENTES_DIR, exist_ok=True)
os.makedirs(ACHATS_DIR, exist_ok=True)

# Utilitaire pour extraire colonne ou NA

def get_col(df, col_name):
    return df[col_name] if col_name in df.columns else pd.Series(pd.NA, index=df.index)

# Génération des CSV en modèle étoile pour Ventes

def generer_csv_ventes_star():
    df = pd.read_csv(
        os.path.join(dossier_datalake_processed, 'tabla_generale_ventes.csv'),
        parse_dates=['Date BL', 'date facture', 'Date demandée client', 'Date accusée AMCO'],
        encoding='utf-8-sig',
        low_memory=False
    )
    # --- Dimensions ---
    # Dimension clients
    dim_client = pd.DataFrame({
        'code_client': get_col(df, 'Code client'),
        'raison_sociale': get_col(df, 'Raison sociale')
    }).drop_duplicates().reset_index(drop=True)
    dim_client['dim_client_id'] = dim_client.index + 1
    dim_client = dim_client[['dim_client_id', 'code_client', 'raison_sociale']]
    dim_client.to_csv(os.path.join(VENTES_DIR, 'dim_client.csv'), index=False)
    logging.info(f"dim_client.csv : {len(dim_client)} lignes")

    # Dimension articles
    dim_article = pd.DataFrame({
        'code_article': get_col(df, 'code article'),
        'designation': get_col(df, 'Désignation'),
        'code_famille': get_col(df, 'famille article libellé'),
        'libelle_famille': get_col(df, 'famille article libellé'),
        'libelle_sous_famille': get_col(df, 'sous-famille article libellé')
    }).drop_duplicates().reset_index(drop=True)
    dim_article['dim_article_id'] = dim_article.index + 1
    dim_article = dim_article[['dim_article_id','code_article','designation','code_famille','libelle_famille','libelle_sous_famille']]
    dim_article.to_csv(os.path.join(VENTES_DIR, 'dim_article.csv'), index=False)
    logging.info(f"dim_article.csv : {len(dim_article)} lignes")

    # Dimension temps
    dates = pd.concat([
        get_col(df, 'Date BL'),
        get_col(df, 'date facture')
    ]).dropna().drop_duplicates().reset_index(drop=True)
    dim_temps = pd.DataFrame({'date_cle': dates.sort_values().unique()})
    dim_temps['annee'] = pd.to_datetime(dim_temps['date_cle']).dt.year
    dim_temps['mois'] = pd.to_datetime(dim_temps['date_cle']).dt.month
    dim_temps['jour'] = pd.to_datetime(dim_temps['date_cle']).dt.day
    dim_temps['dim_temps_id'] = dim_temps.index + 1
    dim_temps = dim_temps[['dim_temps_id','date_cle','annee','mois','jour']]
    dim_temps.to_csv(os.path.join(VENTES_DIR, 'dim_temps.csv'), index=False)
    logging.info(f"dim_temps.csv : {len(dim_temps)} lignes")

    # --- Mappings pour faits ---
    map_client  = dict(zip(dim_client['code_client'],  dim_client['dim_client_id']))
    map_article = dict(zip(dim_article['code_article'], dim_article['dim_article_id']))
    map_temps   = dict(zip(dim_temps['date_cle'],      dim_temps['dim_temps_id']))

    # Construction de la table de faits ventes
    fact = pd.DataFrame({
        'date_bl':       get_col(df, 'Date BL'),
        'num_bl':        get_col(df, 'N° BL'),
        'date_facture':  get_col(df, 'date facture'),
        'num_facture':   get_col(df, 'N° facture'),
        'quantite':      get_col(df, 'Qté fact'),
        'prix_unitaire': get_col(df, 'Prix Unitaire'),
        'montant_ht':    get_col(df, 'Tot HT'),
        'code_client':   get_col(df, 'Code client'),
        'code_article':  get_col(df, 'code article')
    })
    # Appliquer map pour créer les clés étrangères (remplacer NaN par 0)
    fact['dim_client_id']  = fact['code_client'].map(map_client).fillna(0).astype(int)
    fact['dim_article_id'] = fact['code_article'].map(map_article).fillna(0).astype(int)
    fact['dim_temps_id']   = fact['date_bl'].map(map_temps).fillna(0).astype(int)

    # Sélection des colonnes finales
    fact_ventes = fact[[
        'date_bl','num_bl','date_facture','num_facture',
        'quantite','prix_unitaire','montant_ht',
        'dim_client_id','dim_article_id','dim_temps_id'
    ]]

    # Export CSV
    fact_ventes.to_csv(os.path.join(VENTES_DIR, 'fact_ventes.csv'), index=False)
    logging.info(f"fact_ventes.csv : {len(fact_ventes)} lignes")

# Génération des CSV en modèle étoile pour Achats

def generer_csv_achats_star():
    df = pd.read_csv(
        os.path.join(dossier_datalake_processed, 'tabla_generale_achats.csv'),
        parse_dates=['Date BL','date facture','Date demandée client','Date accusée AMCO'],
        encoding='utf-8-sig',
        low_memory=False
    )
    # Dimension fournisseurs
    dim_fourn = pd.DataFrame({
        'code_fournisseur': get_col(df, 'Ref cde fournisseur'),
        'raison_sociale':   get_col(df, 'Raison sociale')
    }).drop_duplicates().reset_index(drop=True)
    dim_fourn['dim_fournisseur_id'] = dim_fourn.index + 1
    dim_fourn = dim_fourn[['dim_fournisseur_id','code_fournisseur','raison_sociale']]
    dim_fourn.to_csv(os.path.join(ACHATS_DIR, 'dim_fournisseur.csv'), index=False)
    logging.info(f"dim_fournisseur.csv : {len(dim_fourn)} lignes")

    # Dimension articles (isolation)
    dim_article = pd.DataFrame({
        'code_article': get_col(df, 'code article'),
        'designation':  get_col(df, 'Désignation'),
        'code_famille': get_col(df, 'famille article libellé'),
        'libelle_famille':      get_col(df, 'famille article libellé'),
        'libelle_sous_famille': get_col(df, 'sous-famille article libellé')
    }).drop_duplicates().reset_index(drop=True)
    dim_article['dim_article_id'] = dim_article.index + 1
    dim_article = dim_article[['dim_article_id','code_article','designation','code_famille','libelle_famille','libelle_sous_famille']]
    dim_article.to_csv(os.path.join(ACHATS_DIR, 'dim_article.csv'), index=False)
    logging.info(f"dim_article.csv : {len(dim_article)} lignes")

    # Dimension temps (isolation)
    dim_temps = pd.DataFrame({'date_cle': get_col(df, 'Date BL').dropna().drop_duplicates()})
    dim_temps['annee'] = pd.to_datetime(dim_temps['date_cle']).dt.year
    dim_temps['mois']  = pd.to_datetime(dim_temps['date_cle']).dt.month
    dim_temps['jour']  = pd.to_datetime(dim_temps['date_cle']).dt.day
    dim_temps['dim_temps_id'] = dim_temps.index + 1
    dim_temps = dim_temps[['dim_temps_id','date_cle','annee','mois','jour']]
    dim_temps.to_csv(os.path.join(ACHATS_DIR, 'dim_temps.csv'), index=False)
    logging.info(f"dim_temps.csv : {len(dim_temps)} lignes")

    # Mappings
    map_fournisseur = dict(zip(dim_fourn['code_fournisseur'], dim_fourn['dim_fournisseur_id']))
    map_article     = dict(zip(dim_article['code_article'],     dim_article['dim_article_id']))
    map_temps       = dict(zip(dim_temps['date_cle'],           dim_temps['dim_temps_id']))

    # Table de faits Achats
    fact_a = pd.DataFrame({
        'date_bl':          get_col(df, 'Date BL'),
        'num_bl':           get_col(df, 'N° BC'),
        'quantite':         get_col(df, 'Qté fact'),
        'prix_unitaire':    get_col(df, 'Prix Unitaire'),
        'code_fournisseur': get_col(df, 'Ref cde fournisseur'),
        'code_article':     get_col(df, 'code article')
    })
    fact_a['dim_fournisseur_id'] = fact_a['code_fournisseur'].map(map_fournisseur).fillna(0).astype(int)
    fact_a['dim_article_id']     = fact_a['code_article'].map(map_article).fillna(0).astype(int)
    fact_a['dim_temps_id']       = fact_a['date_bl'].map(map_temps).fillna(0).astype(int)

    fact_achats = fact_a[[
        'date_bl','num_bl','quantite','prix_unitaire',
        'dim_fournisseur_id','dim_article_id','dim_temps_id'
    ]]
    fact_achats.to_csv(os.path.join(ACHATS_DIR, 'fact_achats.csv'), index=False)
    logging.info(f"fact_achats.csv : {len(fact_achats)} lignes")

# Main

def main():
    generer_csv_ventes_star()
    generer_csv_achats_star()
    logging.info("Modèle étoile CSV générés pour ventes et achats")

if __name__ == "__main__":
    main()
