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
        parse_dates=['Date BL', 'Date demandée client', 'Date accusée AMCO', 'date facture'],
        encoding='utf-8-sig',
        low_memory=False
    )

    # Dimension clients
    dim_client = pd.DataFrame({
        'code_client': get_col(df, 'Code client'),
        'raison_sociale': get_col(df, 'Raison sociale'),
        'famille_client': get_col(df, 'Famille du client'),
        'responsable_dossier': get_col(df, 'responsable du dossier'),
        'representant': get_col(df, 'représentant')
    }).drop_duplicates().reset_index(drop=True)
    dim_client['dim_client_id'] = dim_client.index + 1
    dim_client = dim_client[['dim_client_id','code_client','raison_sociale','famille_client','responsable_dossier','representant']]
    dim_client.to_csv(os.path.join(VENTES_DIR, 'dim_client.csv'), index=False)
    logging.info(f"dim_client.csv : {len(dim_client)} lignes")

    # Dimension familles d'articles
    dim_fam = pd.DataFrame({
        'code_famille': get_col(df, 'famille article libellé'),
        'libelle_famille': get_col(df, 'famille article libellé'),
        'libelle_sous_famille': get_col(df, 'sous-famille article libellé')
    }).drop_duplicates().reset_index(drop=True)
    dim_fam['id_famille'] = dim_fam.index + 1
    dim_fam = dim_fam[['id_famille','code_famille','libelle_famille','libelle_sous_famille']]
    dim_fam.to_csv(os.path.join(VENTES_DIR, 'dim_famillesarticles.csv'), index=False)
    logging.info(f"dim_famillesarticles.csv : {len(dim_fam)} lignes")

    # Dimension articles
    dim_article = pd.DataFrame({
        'code_article': get_col(df, 'code article'),
        'designation': get_col(df, 'Désignation'),
        'numero_plan': get_col(df, 'Numéro de plan'),
        'ref_article_client': get_col(df, 'Ref cde client'),
        'id_famille_fk': get_col(df, 'famille article libellé')
    })
    # Autoriser multiples designations, enregistrer toutes combinaisons
    dim_article = dim_article.drop_duplicates(subset=['code_article','designation','id_famille_fk'])
    dim_article['dim_article_id'] = dim_article.index + 1
    dim_article = dim_article[['dim_article_id','code_article','designation','numero_plan','ref_article_client','id_famille_fk']]
    dim_article.to_csv(os.path.join(VENTES_DIR, 'dim_article.csv'), index=False)
    logging.info(f"dim_article.csv : {len(dim_article)} lignes")

    # Dimension temps journalier
    dates = pd.concat([df['Date BL'], df['date facture'], df['Date demandée client'], df['Date accusée AMCO']])
    dim_temps = pd.DataFrame({'date_cle': dates.dropna().drop_duplicates().reset_index(drop=True)})
    dim_temps['annee'] = dim_temps['date_cle'].dt.year
    dim_temps['mois'] = dim_temps['date_cle'].dt.month
    dim_temps['jour'] = dim_temps['date_cle'].dt.day
    dim_temps['dim_temps_id'] = dim_temps.index + 1
    dim_temps = dim_temps[['dim_temps_id','date_cle','annee','mois','jour']]
    dim_temps.to_csv(os.path.join(VENTES_DIR, 'dim_temps.csv'), index=False)
    logging.info(f"dim_temps.csv : {len(dim_temps)} lignes")


    # 1) Construction du DataFrame fact
    fact = pd.DataFrame({
        'dl_no':                 get_col(df, 'N° Ligne doc'),
        'date_bl':               get_col(df, 'Date BL'),
        'num_bl':                get_col(df, 'N° BL'),
        'condition_livraison':   get_col(df, 'condition_livraison'),
        'date_demandee_client':  get_col(df, 'Date demandée client'),
        'date_accusee_amco':     get_col(df, 'Date accusée AMCO'),
        'num_facture':           get_col(df, 'N° facture'),
        'date_facture':          get_col(df, 'date facture'),
        'qte_vendue':            get_col(df, 'Qté fact'),
        'prix_unitaire':         get_col(df, 'Prix Unitaire'),
        'montant_ht':            get_col(df, 'Tot HT'),
        'code_client':           get_col(df, 'Code client'),
        'code_article':          get_col(df, 'code article'),
        'designation':           get_col(df, 'Désignation')
    })

    # 2) Merge pour la FK client
    fact = fact.merge(
        dim_client[['code_client','dim_client_id']],
        on='code_client',
        how='left'
    )
    if fact['dim_client_id'].isna().any():
        bad = fact.loc[fact['dim_client_id'].isna(), 'code_client'].drop_duplicates()
        raise ValueError(f"Échecs de mapping dim_client pour : {list(bad)}")

    # 3) Merge pour la FK article (code_article + designation)
    fact = fact.merge(
        dim_article[['code_article','designation','dim_article_id']],
        on=['code_article','designation'],
        how='left'
    )
    if fact['dim_article_id'].isna().any():
        bad = fact.loc[fact['dim_article_id'].isna(), ['code_article','designation']].drop_duplicates()
        raise ValueError(f"Échecs de mapping dim_article pour ces paires :\n{bad}")

    # 4) Merge pour la FK temps (ajusté pour ignorer les NaT)
    fact = fact.merge(
        dim_temps[['date_cle','dim_temps_id']],
        left_on='date_bl', right_on='date_cle',
        how='left'
    ).drop(columns=['date_cle'])

    # On ne contrôle que les lignes où date_bl est non nulle
    mask_date = fact['date_bl'].notna()
    bad_dates = fact.loc[mask_date & fact['dim_temps_id'].isna(), 'date_bl'].drop_duplicates()
    if not bad_dates.empty:
        raise ValueError(f"Échecs de mapping dim_temps pour ces dates : {list(bad_dates)}")


    # 5) Sélection et export
    fact_ventes = fact[[
        'dl_no','date_bl','num_bl','condition_livraison',
        'date_demandee_client','date_accusee_amco',
        'num_facture','date_facture','qte_vendue',
        'prix_unitaire','montant_ht',
        'dim_client_id','dim_article_id','dim_temps_id'
    ]]
    fact_ventes.to_csv(os.path.join(VENTES_DIR, 'fact_ventes.csv'), index=False)
    logging.info(f"fact_ventes.csv : {len(fact_ventes)} lignes")

# Génération des CSV en modèle étoile pour Achats


def generer_csv_achats_star():
    # 0) Lecture de la table générale achats
    df = pd.read_csv(
        os.path.join(dossier_datalake_processed, 'tabla_generale_achats.csv'),
        encoding='utf-8-sig',
        parse_dates=['date achat'],
        dayfirst=True,
        low_memory=False
    )

    # 1) Dimension fournisseurs (dim_fournisseur)
    # - code_fournisseur, raison_sociale, famille_fournisseur
    # - clés UNKNOWN gérées par Supabase
    dim_fourn = pd.DataFrame({
        'code_fournisseur': get_col(df, 'Code fournisseur'),
        'raison_sociale':   get_col(df, 'Raison sociale'),
        'famille_fournisseur': get_col(df, 'Famille du client')
    }).drop_duplicates().reset_index(drop=True)
    dim_fourn['dim_fournisseur_id'] = dim_fourn.index + 1
    dim_fourn = dim_fourn[['dim_fournisseur_id', 'code_fournisseur', 'raison_sociale', 'famille_fournisseur']]
    dim_fourn.to_csv(os.path.join(ACHATS_DIR, 'dim_fournisseur.csv'), index=False)

    # 2) Dimension familles articles (dim_famille_article)
    dim_fam_a = pd.DataFrame({
        'fa_codef':       get_col(df, 'famille article libellé'),
        'fa_central':     get_col(df, 'famille article libellé'),
        'fa_intitule':    get_col(df, 'sous-famille article libellé')
    }).drop_duplicates().reset_index(drop=True)
    dim_fam_a['famille_id'] = dim_fam_a.index + 1
    dim_fam_a = dim_fam_a[['famille_id', 'fa_codef', 'fa_central', 'fa_intitule']]
    dim_fam_a.to_csv(os.path.join(ACHATS_DIR, 'dim_famille_article.csv'), index=False)

    # 3) Dimension articles (dim_article)
    dim_article = pd.DataFrame({
        'ar_ref':       get_col(df, 'code article'),
        'famille_id':   get_col(df, 'famille article libellé')
    }).drop_duplicates().reset_index(drop=True)
    dim_article = dim_article.merge(
        dim_fam_a[['fa_codef', 'famille_id']],
        left_on='famille_id', right_on='fa_codef', how='left'
    )
    dim_article = dim_article[['ar_ref', 'famille_id_y']]
    dim_article.columns = ['ar_ref', 'famille_id']
    dim_article['article_id'] = dim_article.index + 1
    dim_article = dim_article[['article_id', 'ar_ref', 'famille_id']]
    dim_article.to_csv(os.path.join(ACHATS_DIR, 'dim_article.csv'), index=False)

    # 4) Dimension temps (dim_date)
    # Utilise uniquement la date achat
    dates = df['date achat'].dropna().drop_duplicates().reset_index(drop=True)
    dim_date = pd.DataFrame({'date_full': dates})
    dim_date['annee'] = dim_date['date_full'].dt.year
    dim_date['mois'] = dim_date['date_full'].dt.month
    dim_date['jour'] = dim_date['date_full'].dt.day
    dim_date['date_id'] = dim_date.index + 1
    dim_date = dim_date[['date_id', 'date_full', 'annee', 'mois', 'jour']]
    dim_date.to_csv(os.path.join(ACHATS_DIR, 'dim_date.csv'), index=False)

    # 5) Construction de la table de faits fact_achats
    fact_a = pd.DataFrame({
        'date_achat':      get_col(df, 'date achat'),
        'code_fournisseur': get_col(df, 'Code fournisseur'),
        'ar_ref':          get_col(df, 'code article'),
        'bon_de_commande': get_col(df, 'Bon de commande'),
        'qte_fact':        get_col(df, 'Qté fact'),
        'total_ht':        get_col(df, 'Total HT'),
        'total_ttc':       get_col(df, 'Total TTC'),
        'net_a_payer':     get_col(df, 'NET A PAYER')
    })

    # 5a) Jointure dim_date
    fact_a = fact_a.merge(
        dim_date[['date_full', 'date_id']],
        left_on='date_achat', right_on='date_full', how='left'
    ).drop(columns=['date_full'])

    # 5b) Jointure dim_fournisseur
    fact_a = fact_a.merge(
        dim_fourn[['code_fournisseur', 'dim_fournisseur_id']],
        on='code_fournisseur', how='left'
    )

    # 5c) Jointure dim_article
    fact_a = fact_a.merge(
        dim_article[['ar_ref', 'article_id']],
        on='ar_ref', how='left'
    )

    # 6) Sélection colonnes finales
    fact_achats = fact_a[[
        'date_id', 'dim_fournisseur_id', 'article_id',
        'bon_de_commande', 'qte_fact', 'total_ht', 'total_ttc', 'net_a_payer'
    ]]
    fact_achats.to_csv(os.path.join(ACHATS_DIR, 'fact_achats.csv'), index=False)

# Main
def main():
    generer_csv_ventes_star()
    generer_csv_achats_star()
    logging.info("Modèle étoile CSV générés pour ventes et achats")

if __name__ == "__main__":
    main()
