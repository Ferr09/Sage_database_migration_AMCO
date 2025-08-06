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

# --- CONSTANTES POUR LA GESTION DES VALEURS INCONNUES (en français) ---
ID_INCONNU = 1
VALEUR_TEXTE_INCONNU = "Inconnu"
VALEUR_CODE_INCONNU = "INC" # Pour "Inconnu"
DATE_INCONNUE = pd.Timestamp('1900-01-01')

# Répertoires de sortie
VENTES_DIR = os.path.join(dossier_datalake_processed, "ventes")
ACHATS_DIR = os.path.join(dossier_datalake_processed, "achats")

os.makedirs(VENTES_DIR, exist_ok=True)
os.makedirs(ACHATS_DIR, exist_ok=True)

# --- Fonctions Utilitaires ---
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
        df.columns = df.columns.str.strip()
        logging.info(f"Fichier '{os.path.basename(chemin_fichier)}' chargé avec {len(df)} lignes.")
        return df
    except FileNotFoundError:
        logging.error(f"Le fichier '{os.path.basename(chemin_fichier)}' n'a pas été trouvé. Processus interrompu.")
        return None

def creer_dimension_avec_inconnu(df_base, nom_id, valeurs_inconnues):
    """
    Ajoute une ligne 'Inconnu' à un DataFrame de dimension et lui assigne un ID.
    """
    df_inconnu = pd.DataFrame([valeurs_inconnues])
    df_final = pd.concat([df_inconnu, df_base], ignore_index=True)
    df_final[nom_id] = df_final.index + 1
    return df_final

# =============================================================================
# MODÈLE EN ÉTOILE POUR LES VENTES
# =============================================================================
def generer_csv_ventes_star():
    logging.info("Génération du modèle en étoile pour les VENTES...")
    df = charger_et_nettoyer_csv(os.path.join(dossier_datalake_processed, 'tabla_generale_ventes.csv'), dates_a_parser=['Date BL', 'date facture'])
    if df is None: return

    # --- 1. Dimension: dim_famillesarticles ---
    logging.info("Création de ventes/dim_famillesarticles.csv")
    dim_fam_base = df[['Code Famille', 'famille article libellé', 'sous-famille article libellé']].rename(columns={'Code Famille': 'code_famille', 'famille article libellé': 'libelle_famille', 'sous-famille article libellé': 'libelle_sous_famille'}).dropna(subset=['code_famille']).drop_duplicates(subset=['code_famille']).reset_index(drop=True)
    valeurs_inconnues_fam = {'code_famille': VALEUR_CODE_INCONNU, 'libelle_famille': VALEUR_TEXTE_INCONNU, 'libelle_sous_famille': VALEUR_TEXTE_INCONNU}
    dim_fam = creer_dimension_avec_inconnu(dim_fam_base, 'id_famille', valeurs_inconnues_fam)
    dim_fam.to_csv(os.path.join(VENTES_DIR, 'dim_famillesarticles.csv'), index=False, encoding='utf-8-sig')

    # --- 2. Dimension: dim_article (dépend de dim_famillesarticles) ---
    logging.info("Création de ventes/dim_article.csv")
    dim_article_base = df[['code article', 'Désignation', 'Code Famille']].rename(columns={'code article': 'code_article', 'Désignation': 'designation', 'Code Famille': 'code_famille'}).dropna(subset=['code_article']).drop_duplicates(subset=['code_article']).reset_index(drop=True)
    dim_article_base = dim_article_base.merge(dim_fam[['code_famille', 'id_famille']], on='code_famille', how='left')
    dim_article_base['id_famille'].fillna(ID_INCONNU, inplace=True) # Gérer les articles sans famille
    
    valeurs_inconnues_art = {'code_article': VALEUR_CODE_INCONNU, 'designation': VALEUR_TEXTE_INCONNU, 'id_famille': ID_INCONNU}
    dim_article = creer_dimension_avec_inconnu(dim_article_base.drop(columns=['code_famille']), 'dim_article_id', valeurs_inconnues_art)
    dim_article['id_famille'] = dim_article['id_famille'].astype('Int64')
    dim_article[['dim_article_id', 'code_article', 'designation', 'id_famille']].to_csv(os.path.join(VENTES_DIR, 'dim_article.csv'), index=False, encoding='utf-8-sig')

    # --- 3. Dimension: dim_client ---
    logging.info("Création de ventes/dim_client.csv")
    dim_client_base = df[['Code client', 'Raison sociale']].rename(columns={'Code client': 'code_client', 'Raison sociale': 'raison_sociale'}).dropna(subset=['code_client']).drop_duplicates(subset=['code_client']).reset_index(drop=True)
    valeurs_inconnues_cli = {'code_client': VALEUR_CODE_INCONNU, 'raison_sociale': VALEUR_TEXTE_INCONNU, 'famille_client': VALEUR_TEXTE_INCONNU, 'responsable_dossier': VALEUR_TEXTE_INCONNU, 'representant': VALEUR_TEXTE_INCONNU}
    for col in valeurs_inconnues_cli.keys():
        if col not in dim_client_base.columns:
            dim_client_base[col] = pd.NA
    dim_client = creer_dimension_avec_inconnu(dim_client_base, 'dim_client_id', valeurs_inconnues_cli)
    dim_client.to_csv(os.path.join(VENTES_DIR, 'dim_client.csv'), index=False, encoding='utf-8-sig')

    # --- 4. Dimension: dim_temps ---
    logging.info("Création de ventes/dim_temps.csv")
    dates = pd.to_datetime(df['Date BL'], errors='coerce').dropna().unique()
    dim_temps_base = pd.DataFrame({'date_cle': dates}).sort_values('date_cle').reset_index(drop=True)
    valeurs_inconnues_tps = {'date_cle': DATE_INCONNUE}
    dim_temps = creer_dimension_avec_inconnu(dim_temps_base, 'dim_temps_id', valeurs_inconnues_tps)
    dim_temps['annee'] = dim_temps['date_cle'].dt.year
    dim_temps['mois'] = dim_temps['date_cle'].dt.month
    dim_temps['jour'] = dim_temps['date_cle'].dt.day
    dim_temps.to_csv(os.path.join(VENTES_DIR, 'dim_temps.csv'), index=False, encoding='utf-8-sig')

    # --- Table des Faits : Ventes ---
    logging.info("Construction de fact_ventes...")
    fact = df.copy()
    fact = fact.merge(dim_client[['code_client', 'dim_client_id']], left_on='Code client', right_on='code_client', how='left')
    fact = fact.merge(dim_article[['code_article', 'dim_article_id']], left_on='code article', right_on='code_article', how='left')
    fact = fact.merge(dim_temps[['date_cle', 'dim_temps_id']], left_on='Date BL', right_on='date_cle', how='left')
    
    fact['dim_client_id'].fillna(ID_INCONNU, inplace=True)
    fact['dim_article_id'].fillna(ID_INCONNU, inplace=True)
    fact['dim_temps_id'].fillna(ID_INCONNU, inplace=True)
    
    fact_ventes = fact.rename(columns={
        'N° Ligne doc': 'dl_no', 'N° Cde': 'num_cde', 'Date BL': 'date_bl',
        'N° BL': 'num_bl', 'Qté fact': 'qte_vendue', 'Prix Unitaire': 'prix_unitaire',
        'Tot HT': 'montant_ht'
    })
    
    colonnes_int = ['dl_no', 'num_cde', 'dim_client_id', 'dim_article_id', 'dim_temps_id']
    for col in colonnes_int:
        if col in fact_ventes.columns:
            fact_ventes[col] = pd.to_numeric(fact_ventes[col], errors='coerce').astype('Int64')

    final_cols = ['dl_no', 'num_cde', 'date_bl', 'num_bl', 'qte_vendue', 'prix_unitaire', 'montant_ht', 'dim_client_id', 'dim_article_id', 'dim_temps_id']
    fact_ventes_final = fact_ventes[[col for col in final_cols if col in fact_ventes.columns]]
    
    fact_ventes_final.to_csv(os.path.join(VENTES_DIR, 'fact_ventes.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"fact_ventes.csv généré avec {len(fact_ventes_final)} lignes.")

# =============================================================================
# MODÈLE EN ÉTOILE POUR LES ACHATS
# =============================================================================
def generer_csv_achats_star():
    logging.info("Début de la génération du modèle en étoile pour les ACHATS.")
    df = charger_et_nettoyer_csv(os.path.join(dossier_datalake_processed, 'tabla_generale_achats.csv'), dates_a_parser=['date achat'], dayfirst_format=True)
    if df is None: return

    # --- 1. Dimension: dim_famille_article (Achats) ---
    logging.info("Création de achats/dim_famille_article.csv")
    dim_fam_achats_base = df[['Code Famille', 'famille article libellé', 'sous-famille article libellé']].rename(columns={'Code Famille': 'fa_codef', 'famille article libellé': 'fa_central', 'sous-famille article libellé': 'fa_intitule'}).dropna(subset=['fa_codef']).drop_duplicates(subset=['fa_codef']).reset_index(drop=True)
    valeurs_inconnues_fam_a = {'fa_codef': VALEUR_CODE_INCONNU, 'fa_central': VALEUR_TEXTE_INCONNU, 'fa_intitule': VALEUR_TEXTE_INCONNU}
    dim_fam_achats = creer_dimension_avec_inconnu(dim_fam_achats_base, 'famille_id', valeurs_inconnues_fam_a)
    dim_fam_achats.to_csv(os.path.join(ACHATS_DIR, 'dim_famille_article.csv'), index=False, encoding='utf-8-sig')

    # --- 2. Dimension: dim_article (Achats) ---
    logging.info("Création de achats/dim_article.csv")
    dim_art_achats_base = df[['code article', 'Code Famille']].rename(columns={'code article': 'ar_ref', 'Code Famille': 'fa_codef'}).dropna(subset=['ar_ref']).drop_duplicates(subset=['ar_ref']).reset_index(drop=True)
    dim_art_achats_base = dim_art_achats_base.merge(dim_fam_achats[['fa_codef', 'famille_id']], on='fa_codef', how='left')
    dim_art_achats_base['famille_id'].fillna(ID_INCONNU, inplace=True)
    
    valeurs_inconnues_art_a = {'ar_ref': VALEUR_CODE_INCONNU, 'famille_id': ID_INCONNU}
    dim_article_achats = creer_dimension_avec_inconnu(dim_art_achats_base.drop(columns=['fa_codef']), 'article_id', valeurs_inconnues_art_a)
    dim_article_achats['famille_id'] = dim_article_achats['famille_id'].astype('Int64')
    dim_article_achats_final = dim_article_achats[['article_id', 'ar_ref', 'famille_id']]
    dim_article_achats_final.to_csv(os.path.join(ACHATS_DIR, 'dim_article.csv'), index=False, encoding='utf-8-sig')

    # --- 3. Dimension: dim_fournisseur ---
    logging.info("Création de achats/dim_fournisseur.csv")
    dim_fourn_cols = ['Code fournisseur', 'Raison sociale', 'Contact', 'Adresse', 'Complement adresse', 'Code postal', 'Ville', 'N° telephone', 'N° fax']
    dim_fourn_base = df[dim_fourn_cols].rename(columns={'Code fournisseur': 'ct_numpayeur', 'Raison sociale': 'raison_sociale', 'Contact': 'contact', 'Adresse': 'adresse', 'Complement adresse': 'complement', 'Code postal': 'code_postal', 'Ville': 'ville', 'N° telephone': 'telephone', 'N° fax': 'fax'}).dropna(subset=['ct_numpayeur']).drop_duplicates(subset=['ct_numpayeur']).reset_index(drop=True)
    valeurs_inconnues_fourn = {'ct_numpayeur': VALEUR_CODE_INCONNU, 'raison_sociale': VALEUR_TEXTE_INCONNU, 'contact': VALEUR_TEXTE_INCONNU, 'adresse': VALEUR_TEXTE_INCONNU, 'complement': '', 'code_postal': '', 'ville': VALEUR_TEXTE_INCONNU, 'telephone': '', 'fax': ''}
    dim_fourn = creer_dimension_avec_inconnu(dim_fourn_base, 'fournisseur_id', valeurs_inconnues_fourn)
    dim_fourn.to_csv(os.path.join(ACHATS_DIR, 'dim_fournisseur.csv'), index=False, encoding='utf-8-sig')

    # --- 4. Dimension: dim_date (Achats) ---
    logging.info("Création de achats/dim_date.csv")
    dates_achats = pd.to_datetime(df['date achat'], errors='coerce').dropna().unique()
    dim_date_base = pd.DataFrame({'date_full': dates_achats}).sort_values('date_full').reset_index(drop=True)
    valeurs_inconnues_date_a = {'date_full': DATE_INCONNUE}
    dim_date = creer_dimension_avec_inconnu(dim_date_base, 'date_id', valeurs_inconnues_date_a)
    dim_date['annee'] = dim_date['date_full'].dt.year
    dim_date['mois'] = dim_date['date_full'].dt.month
    dim_date['jour'] = dim_date['date_full'].dt.day
    dim_date['trimestre'] = dim_date['date_full'].dt.quarter
    dim_date.to_csv(os.path.join(ACHATS_DIR, 'dim_date.csv'), index=False, encoding='utf-8-sig')
    
    # --- 5. Dimension: dim_mode_expedition ---
    logging.info("Création de achats/dim_mode_expedition.csv")
    dim_mode_base = df[['Mode d\'expedition']].rename(columns={'Mode d\'expedition': 'libelle'}).dropna(subset=['libelle']).drop_duplicates(subset=['libelle']).reset_index(drop=True)
    valeurs_inconnues_mode = {'libelle': VALEUR_TEXTE_INCONNU}
    dim_mode = creer_dimension_avec_inconnu(dim_mode_base, 'mode_id', valeurs_inconnues_mode)
    dim_mode['code_expedit'] = dim_mode['libelle'].str.upper().str.replace(' ', '_').str.slice(0, 20)
    dim_mode_final = dim_mode[['mode_id', 'code_expedit', 'libelle']]
    dim_mode_final.to_csv(os.path.join(ACHATS_DIR, 'dim_mode_expedition.csv'), index=False, encoding='utf-8-sig')

    # --- Table des Faits : Achats ---
    logging.info("Construction de fact_achats...")
    fact = df.copy()
    fact = fact.merge(dim_date[['date_full', 'date_id']], left_on='date achat', right_on='date_full', how='left')
    fact = fact.merge(dim_fourn[['ct_numpayeur', 'fournisseur_id']], left_on='Code fournisseur', right_on='ct_numpayeur', how='left')
    fact = fact.merge(dim_article_achats[['ar_ref', 'article_id']], left_on='code article', right_on='ar_ref', how='left')
    fact = fact.merge(dim_mode[['libelle', 'mode_id']], left_on='Mode d\'expedition', right_on='libelle', how='left')

    fact['date_id'].fillna(ID_INCONNU, inplace=True)
    fact['fournisseur_id'].fillna(ID_INCONNU, inplace=True)
    fact['article_id'].fillna(ID_INCONNU, inplace=True)
    fact['mode_id'].fillna(ID_INCONNU, inplace=True)

    for col in ['date_id', 'fournisseur_id', 'article_id', 'mode_id']:
        fact[col] = fact[col].astype('Int64')

    fact_achats = fact.rename(columns={'Bon de commande': 'bon_de_commande', 'Qté fact': 'qte_fact', 'Total TVA': 'total_tva', 'Total HT': 'total_ht', 'Total TTC': 'total_ttc', 'NET A PAYER': 'net_a_payer', 'Reference achat': 'do_ref'})
    
    final_cols = ['date_id', 'fournisseur_id', 'article_id', 'mode_id', 'do_ref', 'bon_de_commande', 'qte_fact', 'total_tva', 'total_ht', 'total_ttc', 'net_a_payer']
    fact_achats_final = fact_achats[[col for col in final_cols if col in fact_achats.columns]]
    
    fact_achats_final.to_csv(os.path.join(ACHATS_DIR, 'fact_achats.csv'), index=False, encoding='utf-8-sig')
    logging.info("Processus ACHATS terminé.")

# --- Point d'entrée principal ---
def main():
    """Exécute la génération des modèles en étoile pour les ventes et les achats."""
    generer_csv_ventes_star()
    print("-" * 60)
    generer_csv_achats_star()
    logging.info("Toutes les opérations sont terminées.")

if __name__ == "__main__":
    main()