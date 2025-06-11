#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unicodedata
import pandas as pd
import json
from pathlib import Path
from sqlalchemy import text, create_engine
from sqlalchemy.exc import SQLAlchemyError
import importlib.util
import argparse
import time

# Assurez-vous que ces imports correspondent à votre structure de projet
from src.models.tables import (
    metadata_ventes, metadata_achats,
    famille_ventes, articles_ventes, comptet_ventes, docligne_ventes,
    famille_achats, articles_achats, comptet_achats, fournisseur_achats, docligne_achats
)
from src.outils.chemins import racine_projet, dossier_config, dossier_xlsx_propres

# --------------------------------------------------------------------
# Fonctions utilitaires (inchangées)
# --------------------------------------------------------------------
def detect_driver():
    if importlib.util.find_spec("psycopg2"):
        return "postgresql+psycopg2"
    elif importlib.util.find_spec("mysql.connector"):
        return "mysql+mysqlconnector"
    else:
        raise ImportError("Aucun driver compatible détecté.")

def nettoyer_dataframe(df):
    df = df.dropna(axis=1, how="all")
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].replace("nan", None)
        if "DATE" in col.upper():
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    return df.where(pd.notna(df), None)

def nettoyer_texte_objet(df):
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = (
            df[col].astype(str)
                    .replace('nan', None)
                    .apply(lambda x: unicodedata.normalize('NFKC', x)
                            .encode('utf-8', errors='replace')
                            .decode('utf-8', errors='replace') if x else None)
                    )

    return df

def charger_fichiers_excel(dossier, fichiers):
    tables = {}
    for nom_table, fichier in fichiers.items():
        chemin = dossier / fichier
        if chemin.exists():
            df = pd.read_excel(chemin)
            for col in df.select_dtypes(include=["object"]).columns:
                df[col] = df[col].astype(str).str.strip()
            tables[nom_table] = df
        else:
            print(f"Fichier introuvable : {chemin}")
    return tables

# --------------------------------------------------------------------
# NOUVELLE FONCTION : Insertion des données
# --------------------------------------------------------------------
def inserer_donnees(moteur_cible, tables_a_inserer, nom_schema=None):
    """Insère un dictionnaire de DataFrames dans la base de données."""
    print(f"\n--- DÉBUT DE L'INSERTION DANS {'LE SCHÉMA ' + nom_schema if nom_schema else 'LA BASE DE DONNÉES'} ---")
    
    # Dictionnaire pour mapper les noms de variables aux objets Table de SQLAlchemy
    map_tables = {
        "famille_ventes": famille_ventes, "articles_ventes": articles_ventes,
        "comptet_ventes": comptet_ventes, "docligne_ventes": docligne_ventes,
        "famille_achats": famille_achats, "articles_achats": articles_achats,
        "comptet_achats": comptet_achats, "fournisseur_achats": fournisseur_achats,
        "docligne_achats": docligne_achats
    }
    
    # Ordre d'insertion pour respecter les clés étrangères
    ordre_insertion = [
        "famille_ventes", "articles_ventes", "comptet_ventes", 
        "famille_achats", "articles_achats", "comptet_achats", "fournisseur_achats",
        # Les tables avec FK à la fin
        "docligne_ventes", "docligne_achats"
    ]
    
    for nom_variable in ordre_insertion:
        if nom_variable in tables_a_inserer:
            df = tables_a_inserer[nom_variable]
            table_obj = map_tables[nom_variable]
            nom_table_db = table_obj.name # Le vrai nom de la table, ex: "FAMILLE"
            
            try:
                print(f"Insertion de {len(df)} lignes dans la table '{nom_table_db}'...")
                df.to_sql(
                    name=nom_table_db,
                    con=moteur_cible,
                    if_exists="append",  # Ajoute les données, ne supprime pas la table
                    index=False,         # Ne pas écrire l'index de pandas comme colonne
                    schema=nom_schema,   # Essentiel pour PostgreSQL, ignoré par MySQL
                    chunksize=1000       # Insère par lots pour une meilleure performance
                )
                print(f"  -> Succès.")
            except Exception as e:
                print(f"  -> ERREUR lors de l'insertion dans '{nom_table_db}': {e}")
                # Vous pouvez décider de stopper le script en cas d'erreur
                # raise e


# --------------------------------------------------------------------
# Début du script principal (inchangé)
# --------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Construction de la BDD")
parser.add_argument("--db-type", choices=["postgresql", "mysql"], required=True)
args = parser.parse_args()
db_type = args.db_type

config_path = dossier_config / ("postgres_config.json" if db_type == "postgresql" else "mysql_config.json")
if not config_path.exists():
    raise FileNotFoundError(f"Fichier de configuration introuvable : {config_path}")

with open(config_path, "r", encoding="utf-8") as f:
    config = json.load(f)

driver = detect_driver()
base_url = f"{driver}://{config['db_user']}:{config['db_password']}@{config['db_host']}:{config['db_port']}/"
full_url = base_url + config['db_name'] + ("?ssl_disabled=True" if db_type == "mysql" else "")

moteur = create_engine(full_url)
moteur_ventes = None
moteur_achats = None

if db_type == "mysql":
    moteur_ventes = create_engine(base_url + "Ventes" + "?ssl_disabled=True")
    moteur_achats = create_engine(base_url + "Achats" + "?ssl_disabled=True")

with moteur.begin() as conn:
    if db_type == "postgresql":
        utilisateur = config["db_user"]
        conn.execute(text('DROP SCHEMA IF EXISTS "Ventes" CASCADE;'))
        conn.execute(text('DROP SCHEMA IF EXISTS "Achats" CASCADE;'))
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "Ventes" AUTHORIZATION "{utilisateur}";'))
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "Achats" AUTHORIZATION "{utilisateur}";'))
    else:
        conn.execute(text("DROP DATABASE IF EXISTS Ventes;"))
        conn.execute(text("DROP DATABASE IF EXISTS Achats;"))
        conn.execute(text("CREATE DATABASE Ventes;"))
        conn.execute(text("CREATE DATABASE Achats;"))

if db_type == "postgresql":
    metadata_ventes.create_all(moteur)
    metadata_achats.create_all(moteur)
else:
    metadata_ventes.create_all(moteur_ventes)
    metadata_achats.create_all(moteur_achats)

fichiers_ventes = {
    "famille_ventes": "F_FAMILLE_propre.xlsx",
    "articles_ventes": "F_ARTICLE_propre.xlsx",
    "comptet_ventes": "F_COMPTET_propre.xlsx",
    "docligne_ventes": "F_DOCLIGNE_propre.xlsx"
}
fichiers_achats = {
    "famille_achats": "F_FAMILLE_propre.xlsx",
    "articles_achats": "F_ARTICLE_propre.xlsx",
    "comptet_achats": "F_COMPTET_propre.xlsx",
    "fournisseur_achats": "F_ARTFOURNISS_propre.xlsx",
    "docligne_achats": "F_DOCLIGNE_propre.xlsx"
}

tables_ventes = charger_fichiers_excel(dossier_xlsx_propres, fichiers_ventes)
tables_achats = charger_fichiers_excel(dossier_xlsx_propres, fichiers_achats)

for nom_table, df in tables_ventes.items():
    df_nettoye = nettoyer_dataframe(df)
    tables_ventes[nom_table] = df_nettoye

for nom_table, df in tables_achats.items():
    df_nettoye = nettoyer_dataframe(df)
    tables_achats[nom_table] = df_nettoye

if "fournisseur_achats" in tables_achats and "docligne_achats" in tables_achats:
    fourn_vals = (
        tables_achats["fournisseur_achats"]["AF_REFFOURNISS"]
        .dropna().astype(str).str.strip().unique()
    )
    df_docligne = tables_achats["docligne_achats"]
    mask = (
        df_docligne["AF_REFFOURNISS"].notna()
    ) & (
        df_docligne["AF_REFFOURNISS"].astype(str).str.strip().isin(fourn_vals)
    )
    tables_achats["docligne_achats"] = df_docligne.loc[mask]
    print(f"{len(tables_achats['docligne_achats'])} lignes de DOCLIGNE conservées après filtrage (Achats)")

if db_type == "postgresql":
    inserer_donnees(moteur, tables_ventes, nom_schema="Ventes")
    inserer_donnees(moteur, tables_achats, nom_schema="Achats")
else: # mysql
    inserer_donnees(moteur_ventes, tables_ventes)
    inserer_donnees(moteur_achats, tables_achats)

# --------------------------------------------------------------------
# Fermeture propre
# --------------------------------------------------------------------
moteur.dispose()
if db_type == "mysql":
    moteur_ventes.dispose()
    moteur_achats.dispose()

print("\nOpération terminée. Connexions fermées proprement.")