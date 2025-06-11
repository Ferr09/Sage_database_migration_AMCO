#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unicodedata
import pandas as pd
import json
from pathlib import Path
from sqlalchemy import text, create_engine, Numeric, Integer, TIMESTAMP
import importlib.util
import argparse

# Assurez-vous que ces imports correspondent à votre structure de projet
from src.models.tables import metadata_ventes, metadata_achats
from src.outils.chemins import dossier_config, dossier_xlsx_propres

# --------------------------------------------------------------------
# Fonctions utilitaires
# --------------------------------------------------------------------
def detect_driver():
    if importlib.util.find_spec("psycopg2"):
        return "postgresql+psycopg2"
    elif importlib.util.find_spec("mysql.connector"):
        return "mysql+mysqlconnector"
    else:
        raise ImportError("Aucun driver de base de données compatible (psycopg2 ou mysql-connector) n'a été trouvé.")

def charger_fichiers_excel(dossier, fichiers):
    tables = {}
    for nom_table, fichier in fichiers.items():
        chemin = dossier / fichier
        if chemin.exists():
            print(f"Chargement du fichier : {fichier}...")
            df = pd.read_excel(chemin, engine='openpyxl')
            # Nettoyage initial des espaces superflus dans les colonnes de type 'object'
            for col in df.select_dtypes(include=["object"]).columns:
                df[col] = df[col].astype(str).str.strip().replace('nan', None)
            tables[nom_table] = df
        else:
            print(f"AVERTISSEMENT : Fichier introuvable, il sera ignoré : {chemin}")
    return tables

def forcer_types_donnees(df, table_metadata):
    """Convertit les colonnes d'un DataFrame en fonction des types définis dans SQLAlchemy."""
    for col_name, col_type in table_metadata.columns.items():
        if col_name in df.columns:
            try:
                if isinstance(col_type.type, (Numeric, Integer)):
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                    if isinstance(col_type.type, Integer):
                        # Gère les NaN avant de convertir en entier nullable
                        df[col_name] = df[col_name].astype('Int64')
                elif isinstance(col_type.type, (TIMESTAMP)):
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
            except Exception as e:
                print(f"  Avertissement lors de la conversion de la colonne '{col_name}': {e}")
    return df.where(pd.notna(df), None)

def inserer_donnees(moteur_cible, tables_a_inserer, metadatas, nom_schema=None):
    """Insère un dictionnaire de DataFrames dans la base de données."""
    print(f"\n--- DÉBUT DE L'INSERTION DANS {'LE SCHÉMA ' + nom_schema if nom_schema else 'LA BASE DE DONNÉES'} ---")
    
    # Ordre d'insertion pour respecter les clés étrangères
    ordre_insertion = [
        "famille", "articles", "comptet", "fournisseur", "docligne"
    ]
    
    for nom_base in ordre_insertion:
        # Trouver la bonne table dans les dictionnaires
        nom_variable = next((k for k in tables_a_inserer.keys() if nom_base in k), None)
        if nom_variable:
            df = tables_a_inserer[nom_variable]
            nom_table_db = nom_base.upper()
            table_metadata = metadatas.tables[nom_table_db]

            # Forcer les types juste avant l'insertion
            df_typed = forcer_types_donnees(df.copy(), table_metadata)
            
            try:
                print(f"Insertion de {len(df_typed)} lignes dans la table '{nom_table_db}'...")
                df_typed.to_sql(
                    name=nom_table_db,
                    con=moteur_cible,
                    if_exists="append",
                    index=False,
                    schema=nom_schema,
                    chunksize=1000,
                    method='multi'
                )
                print(f"  -> Succès.")
            except Exception as e:
                print(f"  -> ERREUR lors de l'insertion dans '{nom_table_db}': {e}")

# --------------------------------------------------------------------
# Script principal
# --------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Construction et chargement de la BDD Ventes/Achats")
    parser.add_argument("--db-type", choices=["postgresql", "mysql"], required=True, help="Type de base de données cible.")
    args = parser.parse_args()
    db_type = args.db_type

    # --- Lecture Configuration ---
    config_path = dossier_config / (f"{db_type}_config.json")
    if not config_path.exists():
        raise FileNotFoundError(f"Fichier de configuration introuvable : {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    # --- Création Moteurs SQLAlchemy ---
    driver = detect_driver()
    base_url = f"{driver}://{config['db_user']}:{config['db_password']}@{config['db_host']}:{config['db_port']}/"
    ssl_args = "?ssl_disabled=True" if db_type == "mysql" else ""

    if db_type == "postgresql":
        moteur = create_engine(base_url + config['db_name'] + ssl_args)
    else: # mysql
        moteur = create_engine(base_url + ssl_args) # Moteur racine pour créer/supprimer les DBs
        moteur_ventes = create_engine(base_url + "Ventes" + ssl_args)
        moteur_achats = create_engine(base_url + "Achats" + ssl_args)

    # --- Création/Suppression Structures ---
    print(f"Configuration pour {db_type.upper()}...")
    with moteur.connect() as conn:
        if db_type == "postgresql":
            conn.execute(text('DROP SCHEMA IF EXISTS "Ventes" CASCADE;'))
            conn.execute(text('DROP SCHEMA IF EXISTS "Achats" CASCADE;'))
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "Ventes";'))
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "Achats";'))
        else:
            conn.execute(text("DROP DATABASE IF EXISTS Ventes;"))
            conn.execute(text("DROP DATABASE IF EXISTS Achats;"))
            conn.execute(text("CREATE DATABASE Ventes CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
            conn.execute(text("CREATE DATABASE Achats CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
        conn.commit() if hasattr(conn, 'commit') else None # For SQLAlchemy 2.0+ with legacy connections

    # --- Création des Tables ---
    if db_type == "postgresql":
        metadata_ventes.create_all(moteur, schema="Ventes")
        metadata_achats.create_all(moteur, schema="Achats")
    else:
        metadata_ventes.create_all(moteur_ventes)
        metadata_achats.create_all(moteur_achats)
    print("Structure des tables créée.")

    # --- Chargement des Fichiers Excel ---
    fichiers_ventes = {"famille_ventes": "F_FAMILLE_propre.xlsx", "articles_ventes": "F_ARTICLE_propre.xlsx", "comptet_ventes": "F_COMPTET_propre.xlsx", "docligne_ventes": "F_DOCLIGNE_propre.xlsx"}
    fichiers_achats = {"famille_achats": "F_FAMILLE_propre.xlsx", "articles_achats": "F_ARTICLE_propre.xlsx", "comptet_achats": "F_COMPTET_propre.xlsx", "fournisseur_achats": "F_ARTFOURNISS_propre.xlsx", "docligne_achats": "F_DOCLIGNE_propre.xlsx"}
    
    tables_ventes = charger_fichiers_excel(dossier_xlsx_propres, fichiers_ventes)
    tables_achats = charger_fichiers_excel(dossier_xlsx_propres, fichiers_achats)
    
    # --- Insertion des Données ---
    if db_type == "postgresql":
        inserer_donnees(moteur, tables_ventes, metadata_ventes, nom_schema="Ventes")
        inserer_donnees(moteur, tables_achats, metadata_achats, nom_schema="Achats")
    else: # mysql
        inserer_donnees(moteur_ventes, tables_ventes, metadata_ventes)
        inserer_donnees(moteur_achats, tables_achats, metadata_achats)

    # --- Fermeture ---
    moteur.dispose()
    if db_type == "mysql":
        moteur_ventes.dispose()
        moteur_achats.dispose()

    print("\nOpération terminée avec succès.")