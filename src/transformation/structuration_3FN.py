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

# Fonction utilitaire pour extraire une colonne ou NA si absente

def get_col(df, col_name):
    return df[col_name] if col_name in df.columns else pd.Series(pd.NA, index=df.index)


def generer_csv_ventes():
    df = pd.read_csv(
        os.path.join(dossier_datalake_processed, 'tabla_generale_ventes.csv'),
        parse_dates=['Date BL','date facture','Date demandée client','Date accusée AMCO'],
        encoding='utf-8-sig',
        low_memory=False
    )
    # 2.1 Clients
    clients = pd.DataFrame({
        'code_client': get_col(df, 'Code client'),
        'raison_sociale': get_col(df, 'Raison sociale'),
        'famille_client': get_col(df, 'Famille du client'),
        'responsable_dossier': get_col(df, 'responsable du dossier'),
        'representant': get_col(df, 'représentant')
    }).drop_duplicates()
    clients.to_csv(os.path.join(VENTES_DIR, 'clients.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"clients.csv généré : {len(clients)} lignes")
    
    # 2.2 Familles d’articles
    familles = pd.DataFrame({
        'libelle_famille': get_col(df, 'famille article libellé'),
        'libelle_sous_famille': get_col(df, 'sous-famille article libellé')
    }).drop_duplicates()
    familles['code_famille'] = (
        familles['libelle_famille']
        .fillna('')
        .str.replace(r'\s+', '_', regex=True)
        .str.lower()
        .replace('', pd.NA)
    )
    familles = familles[['code_famille','libelle_famille','libelle_sous_famille']]
    familles.to_csv(os.path.join(VENTES_DIR, 'famillesarticles.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"famillesarticles.csv généré : {len(familles)} lignes")
    
    # 2.3 Articles
    articles = pd.DataFrame({
        'code_article': get_col(df, 'code article'),
        'designation': get_col(df, 'Désignation'),
        'numero_plan': get_col(df, 'Numéro de plan'),
        'ref_article_client': get_col(df, 'ref cde client'),
        'libelle_famille': get_col(df, 'famille article libellé')
    }).drop_duplicates()
    # Jointure code_famille
    map_fam = familles.set_index('libelle_famille')['code_famille'].to_dict()
    articles['id_famille_fk'] = articles['libelle_famille'].map(map_fam)
    articles = articles[['code_article','designation','numero_plan','ref_article_client','id_famille_fk']]
    articles.to_csv(os.path.join(VENTES_DIR, 'articles.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"articles.csv généré : {len(articles)} lignes")
    
    # 2.4 Commandes clients
    commandes = pd.DataFrame({
        'num_commande': get_col(df, 'N° Cde'),
        'ref_commande_client': get_col(df, 'Ref cde client'),
        'date_demandee': get_col(df, 'Date demandée client'),
        'date_accusee': get_col(df, 'Date accusée AMCO'),
        'code_client': get_col(df, 'Code client')
    }).drop_duplicates()
    commandes.to_csv(os.path.join(VENTES_DIR, 'commandesclients.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"commandesclients.csv généré : {len(commandes)} lignes")
    
    # 2.5 Factures de vente
    factures = pd.DataFrame({
        'num_facture': get_col(df, 'N° facture'),
        'date_facture': get_col(df, 'date facture'),
        'num_bl': get_col(df, 'N° BL'),
        'date_bl': get_col(df, 'Date BL'),
        'condition_livraison': get_col(df, 'condition_livraison'),
        'code_client': get_col(df, 'Code client'),
        'id_commande_client_fk': get_col(df, 'N° Cde')
    }).drop_duplicates()
    factures.to_csv(os.path.join(VENTES_DIR, 'factures.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"factures.csv généré : {len(factures)} lignes")
    
    # 2.6 Lignes de facture
    lignes = pd.DataFrame({
        'qte_vendue': get_col(df, 'Qté fact'),
        'prix_unitaire_vente': get_col(df, 'Prix Unitaire'),
        'id_facture_fk': get_col(df, 'N° facture'),
        'code_article': get_col(df, 'code article')
    })
    lignes.to_csv(os.path.join(VENTES_DIR, 'lignesfacture.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"lignesfacture.csv généré : {len(lignes)} lignes")


def generer_csv_achats():
    df = pd.read_csv(
        os.path.join(dossier_datalake_processed, 'tabla_generale_achats.csv'),
        parse_dates=['Date BL','date facture','Date demandée client','Date accusée AMCO'],
        encoding='utf-8-sig',
        low_memory=False
    )
    # 3.1 Fournisseurs
    fourn = pd.DataFrame({
        'code_fournisseur': get_col(df, 'Code client'),
        'raison_sociale': get_col(df, 'Raison sociale'),
        'famille_fournisseur': get_col(df, 'Famille du client'),
        'responsable_dossier': get_col(df, 'responsable du dossier'),
        'representant': get_col(df, 'représentant')
    }).drop_duplicates()
    fourn.to_csv(os.path.join(ACHATS_DIR, 'fournisseurs.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"fournisseurs.csv généré : {len(fourn)} lignes")
    
    # 3.2 Familles d’articles achats
    familles = pd.DataFrame({
        'libelle_famille': get_col(df, 'famille article libellé'),
        'libelle_sous_famille': get_col(df, 'sous-famille article libellé')
    }).drop_duplicates()
    familles['code_famille'] = (
        familles['libelle_famille']
        .fillna('')
        .str.replace(r'\s+', '_', regex=True)
        .str.lower()
        .replace('', pd.NA)
    )
    familles.to_csv(os.path.join(ACHATS_DIR, 'famillesarticles.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"famillesarticles.csv généré : {len(familles)} lignes")
    
    # 3.3 Articles achats
    articles = pd.DataFrame({
        'code_article': get_col(df, 'code article'),
        'designation': get_col(df, 'Désignation'),
        'numero_plan': get_col(df, 'Numéro de plan'),
        'libelle_famille': get_col(df, 'famille article libellé')
    }).drop_duplicates()
    map_fam = familles.set_index('libelle_famille')['code_famille'].to_dict()
    articles['id_famille_fk'] = articles['libelle_famille'].map(map_fam)
    articles = articles[['code_article','designation','numero_plan','id_famille_fk']]
    articles.to_csv(os.path.join(ACHATS_DIR, 'articles.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"articles.csv généré : {len(articles)} lignes")
    
    # 3.4 Articles par fournisseur
    art_fourn = pd.DataFrame({
        'code_article': get_col(df, 'code article'),
        'ref_article_fournisseur': get_col(df, 'Ref cde fournisseur')
    }).drop_duplicates()
    art_fourn.to_csv(os.path.join(ACHATS_DIR, 'articlesfournisseurs.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"articlesfournisseurs.csv généré : {len(art_fourn)} lignes")
    
    # 3.5 Commandes fournisseurs
    commandes = pd.DataFrame({
        'num_commande': get_col(df, 'N° BC'),
        'ref_commande_fourn': get_col(df, 'Ref cde fournisseur'),
        'date_commande': get_col(df, 'Date demandée client'),
        'date_livraison_prevue': get_col(df, 'Date accusée AMCO')
    }).drop_duplicates()
    commandes.to_csv(os.path.join(ACHATS_DIR, 'commandesfournisseurs.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"commandesfournisseurs.csv généré : {len(commandes)} lignes")
    
    # 3.6 Factures fournisseurs
    fact = pd.DataFrame({
        'num_facture': get_col(df, 'N° facture'),
        'date_facture': get_col(df, 'date facture'),
        'num_bl': get_col(df, 'N° BC'),
        'date_bl': get_col(df, 'Date BL')
    }).drop_duplicates()
    fact.to_csv(os.path.join(ACHATS_DIR, 'facturesfournisseurs.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"facturesfournisseurs.csv généré : {len(fact)} lignes")
    
    # 3.7 Lignes facture fournisseur
    lignes = pd.DataFrame({
        'qte_achetee': get_col(df, 'Qté fact'),
        'prix_unitaire_achat': get_col(df, 'Prix Unitaire'),
        'id_facture_fourn_fk': get_col(df, 'N° facture'),
        'code_article': get_col(df, 'code article')
    })
    lignes.to_csv(os.path.join(ACHATS_DIR, 'lignesfacturefournisseur.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"lignesfacturefournisseur.csv généré : {len(lignes)} lignes")


def main():
    generer_csv_ventes()
    generer_csv_achats()
    logging.info("Tous les CSV 3FN ont été générés dans les dossiers 'ventes' et 'achats' sous processed")

if __name__ == "__main__":
    main()
