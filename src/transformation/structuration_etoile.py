#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import pandas as pd
import os
import sys

# Assurez-vous que le chemin vers src est correct
try:
    from src.outils.chemins import dossier_datalake_processed
except ImportError:
    # Chemin de repli si le script est exécuté depuis un autre répertoire
    projet_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    sys.path.insert(0, projet_root)
    from src.outils.chemins import dossier_datalake_processed

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Répertoires de sortie
VENTES_DIR = os.path.join(dossier_datalake_processed, "ventes")
ACHATS_DIR = os.path.join(dossier_datalake_processed, "achats")

os.makedirs(VENTES_DIR, exist_ok=True)
os.makedirs(ACHATS_DIR, exist_ok=True)

# --- Fonctions Utilitaires ---
def get_col(df, col_name):
    """Retourne la colonne du DataFrame ou une série vide si elle n'existe pas."""
    # Les noms de colonnes sont déjà nettoyés lors du chargement
    return df[col_name] if col_name in df.columns else pd.Series(pd.NA, index=df.index)

def charger_et_nettoyer_csv(chemin_fichier, dates_a_parser=None, dayfirst_format=False):
    """Charge un CSV, normalise les noms de colonnes et gère les erreurs."""
    try:
        df = pd.read_csv(
            chemin_fichier,
            parse_dates=dates_a_parser,
            encoding='utf-8-sig',
            low_memory=False,
            dayfirst=dayfirst_format
        )
        df.columns = df.columns.str.strip() # Normalisation des noms
        logging.info(f"Fichier '{os.path.basename(chemin_fichier)}' chargé avec {len(df)} lignes.")
        return df
    except FileNotFoundError:
        logging.error(f"Le fichier '{os.path.basename(chemin_fichier)}' n'a pas été trouvé. Processus interrompu.")
        return None

# =============================================================================
# MODÈLE EN ÉTOILE POUR LES VENTES
# =============================================================================
def generer_csv_ventes_star():
    """Génère les fichiers CSV pour le modèle en étoile des ventes."""
    logging.info("Début de la génération du modèle en étoile pour les VENTES.")
    
    df = charger_et_nettoyer_csv(
        os.path.join(dossier_datalake_processed, 'tabla_generale_ventes.csv'),
        dates_a_parser=['Date BL', 'Date demandée client', 'Date accusée AMCO', 'date facture']
    )
    if df is None: return

    # --- Dimension Clients ---
    logging.info("Création de dim_client...")
    dim_client_cols = {
        'code_client': get_col(df, 'Code client'),
        'raison_sociale': get_col(df, 'Raison sociale'),
        'famille_client': get_col(df, 'Famille du client'),
        'responsable_dossier': get_col(df, 'responsable du dossier'),
        'representant': get_col(df, 'représentant')
    }
    dim_client = pd.DataFrame(dim_client_cols).dropna(subset=['code_client'])
    dim_client = dim_client.drop_duplicates(subset=['code_client']).reset_index(drop=True)
    dim_client['dim_client_id'] = dim_client.index + 1
    dim_client = dim_client[['dim_client_id', 'code_client', 'raison_sociale', 'famille_client', 'responsable_dossier', 'representant']]
    dim_client.to_csv(os.path.join(VENTES_DIR, 'dim_client.csv'), index=False, encoding='utf-8-sig')

    # --- Dimension Familles d'Articles ---
    logging.info("Création de dim_famillesarticles...")
    # --- CORRECTION : Utiliser 'Code Famille' comme clé naturelle ---
    dim_fam_cols = {
        'code_famille': get_col(df, 'Code Famille'),
        'libelle_famille': get_col(df, 'famille article libellé'),
        'libelle_sous_famille': get_col(df, 'sous-famille article libellé')
    }
    dim_fam = pd.DataFrame(dim_fam_cols).fillna('UNKNOWN').dropna(subset=['code_famille'])
    dim_fam = dim_fam.drop_duplicates(subset=['code_famille']).reset_index(drop=True)
    if 'UNKNOWN' not in dim_fam['code_famille'].values:
        unknown_row = pd.DataFrame([{'code_famille': 'UNKNOWN', 'libelle_famille': 'UNKNOWN', 'libelle_sous_famille': 'UNKNOWN'}])
        dim_fam = pd.concat([dim_fam, unknown_row], ignore_index=True)
    dim_fam['id_famille'] = dim_fam.index + 1
    dim_fam = dim_fam[['id_famille', 'code_famille', 'libelle_famille', 'libelle_sous_famille']]
    dim_fam.to_csv(os.path.join(VENTES_DIR, 'dim_famillesarticles.csv'), index=False, encoding='utf-8-sig')

    # --- Dimension Articles ---
    logging.info("Création de dim_article...")
    dim_article_cols = {
        'code_article': get_col(df, 'code article').str.strip(),
        'designation': get_col(df, 'Désignation'),
        'numero_plan': get_col(df, 'Numéro de plan'),
        'ref_article_client': get_col(df, 'Ref cde client'),
        'code_famille': get_col(df, 'Code Famille') # Utiliser le code pour la jointure
    }
    dim_article = pd.DataFrame(dim_article_cols).dropna(subset=['code_article'])
    dim_article = dim_article.drop_duplicates(subset=['code_article']).reset_index(drop=True)
    
    # --- CORRECTION : Jointure pour obtenir l'ID de la famille via 'code_famille' ---
    dim_article['code_famille'].fillna('UNKNOWN', inplace=True)
    unknown_fam_id = dim_fam.loc[dim_fam['code_famille'] == 'UNKNOWN', 'id_famille'].iloc[0]
    dim_article = dim_article.merge(dim_fam[['code_famille', 'id_famille']], on='code_famille', how='left')
    dim_article['id_famille'].fillna(unknown_fam_id, inplace=True)
    dim_article['id_famille'] = dim_article['id_famille'].astype(int)

    if 'UNKNOWN' not in dim_article['code_article'].values:
        unknown_row = pd.DataFrame([{'code_article': 'UNKNOWN', 'designation': 'UNKNOWN', 'numero_plan': 'UNKNOWN', 'ref_article_client': 'UNKNOWN', 'id_famille': unknown_fam_id}])
        dim_article = pd.concat([dim_article, unknown_row], ignore_index=True)
    dim_article['dim_article_id'] = dim_article.index + 1
    dim_article = dim_article[['dim_article_id', 'code_article', 'designation', 'numero_plan', 'ref_article_client', 'id_famille']]
    dim_article.to_csv(os.path.join(VENTES_DIR, 'dim_article.csv'), index=False, encoding='utf-8-sig')

    # --- Dimension Temps ---
    logging.info("Création de dim_temps...")
    dates = pd.concat([df['Date BL'], df['date facture'], df['Date demandée client'], df['Date accusée AMCO']]).dropna().unique()
    dim_temps = pd.DataFrame({'date_cle': dates})
    unknown_date = pd.to_datetime('1900-01-01')
    if unknown_date not in dim_temps['date_cle'].values:
        dim_temps = pd.concat([dim_temps, pd.DataFrame([{'date_cle': unknown_date}])], ignore_index=True)
    dim_temps['annee'] = dim_temps['date_cle'].dt.year
    dim_temps['mois'] = dim_temps['date_cle'].dt.month
    dim_temps['jour'] = dim_temps['date_cle'].dt.day
    dim_temps['dim_temps_id'] = dim_temps.index + 1
    dim_temps = dim_temps[['dim_temps_id', 'date_cle', 'annee', 'mois', 'jour']]
    dim_temps.to_csv(os.path.join(VENTES_DIR, 'dim_temps.csv'), index=False)

    # --- Table des Faits : Ventes ---
    logging.info("Construction de fact_ventes...")
    fact_cols = {
        'dl_no': get_col(df, 'N° Ligne doc'), 'date_bl': get_col(df, 'Date BL'), 'num_bl': get_col(df, 'N° BL'),
        'condition_livraison': get_col(df, 'condition_livraison'), 'date_demandee_client': get_col(df, 'Date demandée client'),
        'date_accusee_amco': get_col(df, 'Date accusée AMCO'), 'num_facture': get_col(df, 'N° facture'),
        'date_facture': get_col(df, 'date facture'), 'qte_vendue': get_col(df, 'Qté fact'),
        'prix_unitaire': get_col(df, 'Prix Unitaire'), 'montant_ht': get_col(df, 'Tot HT'),
        'code_client': get_col(df, 'Code client'), 'code_article': get_col(df, 'code article').str.strip()
    }
    fact = pd.DataFrame(fact_cols)
    fact = fact.merge(dim_client[['code_client', 'dim_client_id']], on='code_client', how='left')
    fact = fact.merge(dim_article[['code_article', 'dim_article_id']], on='code_article', how='left')
    unknown_article_id = dim_article.loc[dim_article['code_article'] == 'UNKNOWN', 'dim_article_id'].iloc[0]
    fact['dim_article_id'].fillna(unknown_article_id, inplace=True)
    fact['dim_article_id'] = fact['dim_article_id'].astype(int)
    fact = fact.merge(dim_temps[['date_cle', 'dim_temps_id']], left_on='date_bl', right_on='date_cle', how='left')
    unknown_date_id = dim_temps.loc[dim_temps['date_cle'] == unknown_date, 'dim_temps_id'].iloc[0]
    fact['dim_temps_id'].fillna(unknown_date_id, inplace=True)
    fact['dim_temps_id'] = fact['dim_temps_id'].astype(int)
    fact_ventes = fact[['dl_no', 'date_bl', 'num_bl', 'condition_livraison', 'date_demandee_client', 'date_accusee_amco', 'num_facture', 'date_facture', 'qte_vendue', 'prix_unitaire', 'montant_ht', 'dim_client_id', 'dim_article_id', 'dim_temps_id']]
    fact_ventes.to_csv(os.path.join(VENTES_DIR, 'fact_ventes.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"fact_ventes.csv généré avec {len(fact_ventes)} lignes. Processus VENTES terminé.")

# =============================================================================
# MODÈLE EN ÉTOILE POUR LES ACHATS
# =============================================================================
def generer_csv_achats_star():
    """Génère les fichiers CSV pour le modèle en étoile des achats."""
    logging.info("Début de la génération du modèle en étoile pour les ACHATS.")
    
    df = charger_et_nettoyer_csv(
        os.path.join(dossier_datalake_processed, 'tabla_generale_achats.csv'),
        dates_a_parser=['date achat'],
        dayfirst_format=True
    )
    if df is None: return

    # --- Dimension Fournisseurs ---
    logging.info("Création de dim_fournisseur...")
    dim_fourn_cols = {
        'code_fournisseur': get_col(df, 'Code fournisseur'),
        'raison_sociale': get_col(df, 'Raison sociale')
    }
    dim_fourn = pd.DataFrame(dim_fourn_cols).dropna(subset=['code_fournisseur'])
    dim_fourn = dim_fourn.drop_duplicates(subset=['code_fournisseur']).reset_index(drop=True)
    dim_fourn['dim_fournisseur_id'] = dim_fourn.index + 1
    dim_fourn = dim_fourn[['dim_fournisseur_id', 'code_fournisseur', 'raison_sociale']]
    dim_fourn.to_csv(os.path.join(ACHATS_DIR, 'dim_fournisseur.csv'), index=False, encoding='utf-8-sig')

    # --- Dimension Familles d'Articles (Achats) ---
    logging.info("Création de dim_famille_article...")
    # --- CORRECTION : Utiliser 'Code Famille' comme clé naturelle ---
    dim_fam_achats_cols = {
        'code_famille': get_col(df, 'Code Famille'),
        'libelle_famille': get_col(df, 'famille article libellé'),
        'libelle_sous_famille': get_col(df, 'sous-famille article libellé')
    }
    dim_fam_achats = pd.DataFrame(dim_fam_achats_cols).fillna('UNKNOWN').dropna(subset=['code_famille'])
    dim_fam_achats = dim_fam_achats.drop_duplicates(subset=['code_famille']).reset_index(drop=True)
    if 'UNKNOWN' not in dim_fam_achats['code_famille'].values:
        unknown_row = pd.DataFrame([{'code_famille': 'UNKNOWN', 'libelle_famille': 'UNKNOWN', 'libelle_sous_famille': 'UNKNOWN'}])
        dim_fam_achats = pd.concat([dim_fam_achats, unknown_row], ignore_index=True)
    dim_fam_achats['famille_id'] = dim_fam_achats.index + 1
    dim_fam_achats = dim_fam_achats[['famille_id', 'code_famille', 'libelle_famille', 'libelle_sous_famille']]
    dim_fam_achats.to_csv(os.path.join(ACHATS_DIR, 'dim_famille_article.csv'), index=False, encoding='utf-8-sig')

    # --- Dimension Articles (Achats) ---
    logging.info("Création de dim_article pour les achats...")
    dim_article_cols = {
        'ar_ref': get_col(df, 'code article').str.strip(),
        'designation': get_col(df, 'Désignation'),
        'code_famille': get_col(df, 'Code Famille') # Utiliser le code pour la jointure
    }
    dim_article = pd.DataFrame(dim_article_cols).dropna(subset=['ar_ref'])
    dim_article = dim_article.drop_duplicates(subset=['ar_ref']).reset_index(drop=True)
    
    # --- CORRECTION : Jointure avec dim_fam_achats sur 'code_famille' ---
    dim_article['code_famille'].fillna('UNKNOWN', inplace=True)
    unknown_fam_id_achats = dim_fam_achats.loc[dim_fam_achats['code_famille'] == 'UNKNOWN', 'famille_id'].iloc[0]
    dim_article = dim_article.merge(dim_fam_achats[['code_famille', 'famille_id']], on='code_famille', how='left')
    dim_article['famille_id'].fillna(unknown_fam_id_achats, inplace=True)
    dim_article['famille_id'] = dim_article['famille_id'].astype(int)

    if 'UNKNOWN' not in dim_article['ar_ref'].values:
        unknown_row = pd.DataFrame([{'ar_ref': 'UNKNOWN', 'designation': 'UNKNOWN', 'code_famille': 'UNKNOWN', 'famille_id': unknown_fam_id_achats}])
        dim_article = pd.concat([dim_article, unknown_row], ignore_index=True)
    dim_article['article_id'] = dim_article.index + 1
    dim_article = dim_article[['article_id', 'ar_ref', 'designation', 'famille_id']]
    dim_article.to_csv(os.path.join(ACHATS_DIR, 'dim_article.csv'), index=False, encoding='utf-8-sig')

    # --- Dimension Date (Achats) ---
    logging.info("Création de dim_date...")
    dates = df['date achat'].dropna().unique()
    dim_date = pd.DataFrame({'date_full': dates})
    unknown_date = pd.to_datetime('1900-01-01')
    if unknown_date not in dim_date['date_full'].values:
        dim_date = pd.concat([dim_date, pd.DataFrame([{'date_full': unknown_date}])], ignore_index=True)
    dim_date['annee'] = dim_date['date_full'].dt.year
    dim_date['mois'] = dim_date['date_full'].dt.month
    dim_date['jour'] = dim_date['date_full'].dt.day
    dim_date['date_id'] = dim_date.index + 1
    dim_date = dim_date[['date_id', 'date_full', 'annee', 'mois', 'jour']]
    dim_date.to_csv(os.path.join(ACHATS_DIR, 'dim_date.csv'), index=False, encoding='utf-8-sig')

    # --- Table des Faits : Achats ---
    logging.info("Construction de fact_achats...")
    fact_cols = {
        'date_achat': get_col(df, 'date achat'), 'code_fournisseur': get_col(df, 'Code fournisseur'),
        'ar_ref': get_col(df, 'code article').str.strip(), 'bon_de_commande': get_col(df, 'Bon de commande'),
        'qte_fact': get_col(df, 'Qté fact'), 'total_tva': get_col(df, 'Total TVA'),
        'total_ht': get_col(df, 'Total HT'), 'total_ttc': get_col(df, 'Total TTC'),
        'net_a_payer': get_col(df, 'NET A PAYER')
    }
    fact = pd.DataFrame(fact_cols)
    fact = fact.merge(dim_date[['date_full', 'date_id']], left_on='date_achat', right_on='date_full', how='left')
    unknown_date_id = dim_date.loc[dim_date['date_full'] == unknown_date, 'date_id'].iloc[0]
    fact['date_id'].fillna(unknown_date_id, inplace=True)
    fact['date_id'] = fact['date_id'].astype(int)
    fact = fact.merge(dim_fourn[['code_fournisseur', 'dim_fournisseur_id']], on='code_fournisseur', how='left')
    fact = fact.merge(dim_article[['ar_ref', 'article_id']], on='ar_ref', how='left')
    unknown_article_id = dim_article.loc[dim_article['ar_ref'] == 'UNKNOWN', 'article_id'].iloc[0]
    fact['article_id'].fillna(unknown_article_id, inplace=True)
    fact['article_id'] = fact['article_id'].astype(int)
    fact_achats = fact[['date_id', 'dim_fournisseur_id', 'article_id', 'bon_de_commande', 'qte_fact', 'total_tva', 'total_ht', 'total_ttc', 'net_a_payer']]
    fact_achats.to_csv(os.path.join(ACHATS_DIR, 'fact_achats.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"fact_achats.csv généré avec {len(fact_achats)} lignes. Processus ACHATS terminé.")

# --- Point d'entrée principal ---
def main():
    """Exécute la génération des modèles en étoile pour les ventes et les achats."""
    generer_csv_ventes_star()
    print("-" * 60) # Séparateur visuel
    generer_csv_achats_star()
    logging.info("Toutes les opérations sont terminées.")

if __name__ == "__main__":
    main()