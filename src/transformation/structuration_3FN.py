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

def generer_csv_ventes():
    df = pd.read_csv(os.path.join(dossier_datalake_processed, 'tabla_generale_ventes.csv'),
                     parse_dates=['Date BL','date facture','Date demandée client','Date accusée AMCO'],
                     encoding='utf-8-sig')
    # 2.1 Clients
    clients = df[['Code client','Raison sociale','Famille du client','responsable du dossier','représentant']].drop_duplicates()
    clients.columns = ['code_client','raison_sociale','famille_client','responsable_dossier','representant']
    clients.to_csv(os.path.join(VENTES_DIR, 'clients.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"clients.csv généré : {len(clients)} lignes")
    # 2.2 Familles d’articles
    familles = df[['famille article libellé','sous-famille article libellé']].drop_duplicates()
    familles.columns = ['libelle_famille','libelle_sous_famille']
    familles['code_famille'] = (familles['libelle_famille']
                                .str.replace(r'\s+', '_', regex=True)
                                .str.lower())
    familles = familles[['code_famille','libelle_famille','libelle_sous_famille']]
    familles.to_csv(os.path.join(VENTES_DIR, 'famillesarticles.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"famillesarticles.csv généré : {len(familles)} lignes")
    # 2.3 Articles
    articles = df[['code article','Désignation','Numéro de plan','ref cde client','famille article libellé']].drop_duplicates()
    articles.columns = ['code_article','designation','numero_plan','ref_article_client','libelle_famille']
    map_fam = familles.set_index('libelle_famille')['code_famille']
    articles['id_famille_fk'] = articles['libelle_famille'].map(map_fam)
    articles = articles[['code_article','designation','numero_plan','ref_article_client','id_famille_fk']]
    articles.to_csv(os.path.join(VENTES_DIR, 'articles.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"articles.csv généré : {len(articles)} lignes")
    # 2.4 Commandes clients
    commandes = df[['N° Cde','Ref cde client','Date demandée client','Date accusée AMCO','Code client']].drop_duplicates()
    commandes.columns = ['num_commande','ref_commande_client','date_demandee','date_accusee','code_client']
    commandes.to_csv(os.path.join(VENTES_DIR, 'commandesclients.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"commandesclients.csv généré : {len(commandes)} lignes")
    # 2.5 Factures de vente
    factures = df[['N° facture','date facture','N° BL','Date BL','condition_livraison','Code client','N° Cde']].drop_duplicates()
    factures.columns = ['num_facture','date_facture','num_bl','date_bl','condition_livraison','code_client','id_commande_client_fk']
    factures.to_csv(os.path.join(VENTES_DIR, 'factures.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"factures.csv généré : {len(factures)} lignes")
    # 2.6 Lignes de facture
    lignes = df[['Qté fact','Prix Unitaire','N° facture','code article']].copy()
    lignes.columns = ['qte_vendue','prix_unitaire_vente','id_facture_fk','code_article']
    lignes.to_csv(os.path.join(VENTES_DIR, 'lignesfacture.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"lignesfacture.csv généré : {len(lignes)} lignes")

def generer_csv_achats():
    df = pd.read_csv(os.path.join(dossier_datalake_processed, 'tabla_generale_achats.csv'),
                     parse_dates=['Date BL','date facture','Date demandée client','Date accusée AMCO'],
                     encoding='utf-8-sig')
    # 3.1 Fournisseurs
    fourn = df[['Code client','Raison sociale','Famille du client','responsable du dossier','représentant']].drop_duplicates()
    fourn.columns = ['code_fournisseur','raison_sociale','famille_fournisseur','responsable_dossier','representant']
    fourn.to_csv(os.path.join(ACHATS_DIR, 'fournisseurs.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"fournisseurs.csv généré : {len(fourn)} lignes")
    # 3.2 Familles d’articles achats
    familles = df[['famille article libellé','sous-famille article libellé']].drop_duplicates()
    familles.columns = ['libelle_famille','libelle_sous_famille']
    familles['code_famille'] = (familles['libelle_famille']
                                .str.replace(r'\s+', '_', regex=True)
                                .str.lower())
    familles.to_csv(os.path.join(ACHATS_DIR, 'famillesarticles.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"famillesarticles.csv généré : {len(familles)} lignes")
    # 3.3 Articles achats
    articles = df[['code article','Désignation','Numéro de plan','famille article libellé']].drop_duplicates()
    articles.columns = ['code_article','designation','numero_plan','libelle_famille']
    map_fam = familles.set_index('libelle_famille')['code_famille']
    articles['id_famille_fk'] = articles['libelle_famille'].map(map_fam)
    articles = articles[['code_article','designation','numero_plan','id_famille_fk']]
    articles.to_csv(os.path.join(ACHATS_DIR, 'articles.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"articles.csv généré : {len(articles)} lignes")
    # 3.4 Articles par fournisseur
    art_fourn = df[['code article','Ref cde fournisseur']].drop_duplicates()
    art_fourn.columns = ['code_article','ref_article_fournisseur']
    art_fourn.to_csv(os.path.join(ACHATS_DIR, 'articlesfournisseurs.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"articlesfournisseurs.csv généré : {len(art_fourn)} lignes")
    # 3.5 Commandes fournisseurs
    commandes = df[['N° BC','Ref cde fournisseur','Date demandée client','Date accusée AMCO']].drop_duplicates()
    commandes.columns = ['num_commande','ref_commande_fourn','date_commande','date_livraison_prevue']
    commandes.to_csv(os.path.join(ACHATS_DIR, 'commandesfournisseurs.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"commandesfournisseurs.csv généré : {len(commandes)} lignes")
    # 3.6 Factures fournisseurs
    fact = df[['N° facture','date facture','N° BC','Date BL']].drop_duplicates()
    fact.columns = ['num_facture','date_facture','num_bl','date_bl']
    fact.to_csv(os.path.join(ACHATS_DIR, 'facturesfournisseurs.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"facturesfournisseurs.csv généré : {len(fact)} lignes")
    # 3.7 Lignes facture fournisseur
    lignes = df[['Qté fact','Prix Unitaire','N° facture','code article']].copy()
    lignes.columns = ['qte_achetee','prix_unitaire_achat','id_facture_fourn_fk','code_article']
    lignes.to_csv(os.path.join(ACHATS_DIR, 'lignesfacturefournisseur.csv'), index=False, encoding='utf-8-sig')
    logging.info(f"lignesfacturefournisseur.csv généré : {len(lignes)} lignes")

def main():
    generer_csv_ventes()
    generer_csv_achats()
    logging.info("Tous les CSV 3FN ont été générés dans les dossiers 'ventes' et 'achats' sous processed")

if __name__ == "__main__":
    main()
