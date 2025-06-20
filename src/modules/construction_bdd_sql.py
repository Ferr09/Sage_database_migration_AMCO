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
            for col in df.select_dtypes(include=["object"]).columns:
                df[col] = df[col].astype(str).str.strip().replace('nan', None)
            tables[nom_table] = df
        else:
            print(f"AVERTISSEMENT : Fichier introuvable, il sera ignoré : {chemin}")
    return tables

def forcer_types_donnees(df, table_metadata):
    for col_name, col_type in table_metadata.columns.items():
        if col_name in df.columns:
            try:
                if isinstance(col_type.type, (Numeric, Integer)):
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                    if isinstance(col_type.type, Integer):
                        df[col_name] = df[col_name].astype('Int64')
                elif isinstance(col_type.type, TIMESTAMP):
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
            except Exception as e:
                print(f"  Avertissement conversion colonne '{col_name}': {e}")
    return df.where(pd.notna(df), None)

def inserer_donnees(moteur_cible, tables_a_inserer, metadatas, nom_schema=None):
    print(f"\n--- DÉBUT INSERTION DANS {'SCHEMA ' + nom_schema if nom_schema else 'LA BASE'} ---")
    if not tables_a_inserer:
        print("Aucun DataFrame à insérer.")
        return

    map_nom = {
        "famille": "FAMILLE",
        "articles": "ARTICLES",
        "comptet": "COMPTET",
        "fournisseur": "ARTFOURNISS",
        "docligne": "DOCLIGNE"
    }
    ordre = ["famille", "articles", "comptet", "fournisseur", "docligne"]
    cles = sorted(
        tables_a_inserer.keys(),
        key=lambda k: next((ordre.index(b) for b in ordre if b in k), float('inf'))
    )

    # PAS DE SET SESSION ICI : on a déjà ajusté au GLOBAL
    with moteur_cible.connect() as conn:
        for cle in cles:
            try:
                base = next(b for b in ordre if b in cle)
                table_db = map_nom[base]
                print(f"\n[Traitement table {table_db}]")
                df = tables_a_inserer[cle]
                ni = len(df)
                print(f"  - Lignes dans Excel : {ni}")
                if df.empty:
                    continue

                meta = metadatas.tables[table_db]
                df2 = forcer_types_donnees(df.copy(), meta)
                n2 = len(df2)
                print(f"  - Lignes après conversion : {n2}")
                if n2 < ni:
                    print(f"    ! {ni - n2} ligne(s) perdues")

                if df2.empty:
                    continue

                print(f"  - Insertion de {n2} lignes dans '{table_db}'...")
                df2.to_sql(
                    name=table_db,
                    con=conn,
                    if_exists="append",
                    index=False,
                    schema=nom_schema,
                    chunksize=1000,
                    method='multi'
                )
                print("    -> Succès")
            except Exception as e:
                print(f"  ERREUR table '{cle}': {e}")

# --------------------------------------------------------------------
# Script principal
# --------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chargement BDD Ventes/Achats")
    parser.add_argument("--db-type", choices=["postgresql", "mysql"], required=True)
    args = parser.parse_args()
    db_type = args.db_type

    # Lecture config
    cfg_path = dossier_config / f"{db_type}_config.json"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config introuvable : {cfg_path}")
    config = json.load(open(cfg_path, "r", encoding="utf-8"))

    driver = detect_driver()
    base_url = f"{driver}://{config['db_user']}:{config['db_password']}@" \
               f"{config['db_host']}:{config['db_port']}/"
    ssl_args = "?ssl_disabled=True" if db_type == "mysql" else ""

    if db_type == "postgresql":
        moteur = create_engine(base_url + config['db_name'] + ssl_args)
        moteur_ventes = moteur
        moteur_achats = moteur

    else:  # mysql
        # 1) Engine en autocommit pour SET GLOBAL
        auto_eng = create_engine(
            base_url + ssl_args,
            isolation_level="AUTOCOMMIT"
        )
        print("● Elevación: SET GLOBAL max_allowed_packet = 134217728")
        with auto_eng.connect() as conn_root:
            conn_root.execute(text("SET GLOBAL max_allowed_packet = 134217728"))
        auto_eng.dispose()

        # 2) Recréation des engines “normaux”
        print("● Reconstruction des engines MySQL sans SET SESSION")
        moteur = create_engine(base_url + ssl_args)
        moteur_ventes = create_engine(
            base_url + "Ventes" + ssl_args,
            isolation_level="AUTOCOMMIT"
        )
        moteur_achats = create_engine(
            base_url + "Achats" + ssl_args,
            isolation_level="AUTOCOMMIT"
        )

    # Création / Suppression structures
    print(f"Configuration {db_type.upper()}...")
    with moteur.connect() as conn:
        if db_type == "postgresql":
            conn.execute(text('DROP SCHEMA IF EXISTS "Ventes" CASCADE;'))
            conn.execute(text('DROP SCHEMA IF EXISTS "Achats" CASCADE;'))
            conn.execute(text('CREATE SCHEMA IF NOT EXISTS "Ventes";'))
            conn.execute(text('CREATE SCHEMA IF NOT EXISTS "Achats";'))
            conn.commit()
        else:
            conn.execute(text("DROP DATABASE IF EXISTS Ventes;"))
            conn.execute(text("DROP DATABASE IF EXISTS Achats;"))
            conn.execute(text(
                "CREATE DATABASE Ventes CHARACTER SET utf8mb4 "
                "COLLATE utf8mb4_unicode_ci;"
            ))
            conn.execute(text(
                "CREATE DATABASE Achats CHARACTER SET utf8mb4 "
                "COLLATE utf8mb4_unicode_ci;"
            ))
    print("Structures configurées.")

    # Création des tables
    if db_type == "postgresql":
        metadata_ventes.create_all(moteur, schema="Ventes")
        metadata_achats.create_all(moteur, schema="Achats")
    else:
        metadata_ventes.create_all(moteur_ventes)
        metadata_achats.create_all(moteur_achats)
    print("Tables créées.")

    # Chargement Excel
    fichiers_v = {
        "famille_ventes": "F_FAMILLE_propre.xlsx",
        "articles_ventes": "F_ARTICLE_propre.xlsx",
        "comptet_ventes": "F_COMPTET_propre.xlsx",
        "docligne_ventes": "F_DOCLIGNE_propre.xlsx"
    }
    fichiers_a = {
        "famille_achats": "F_FAMILLE_propre.xlsx",
        "articles_achats": "F_ARTICLE_propre.xlsx",
        "comptet_achats": "F_COMPTET_propre.xlsx",
        "fournisseur_achats": "F_ARTFOURNISS_propre.xlsx",
        "docligne_achats": "F_DOCLIGNE_propre.xlsx"
    }
    tables_v = charger_fichiers_excel(dossier_xlsx_propres, fichiers_v)
    tables_a = charger_fichiers_excel(dossier_xlsx_propres, fichiers_a)

    # Insertion données
    if db_type == "postgresql":
        inserer_donnees(moteur, tables_v, metadata_ventes, nom_schema="Ventes")
        inserer_donnees(moteur, tables_a, metadata_achats, nom_schema="Achats")
    else:
        inserer_donnees(moteur_ventes, tables_v, metadata_ventes)
        inserer_donnees(moteur_achats, tables_a, metadata_achats)

    # Fermeture
    moteur.dispose()
    if db_type == "mysql":
        moteur_ventes.dispose()
        moteur_achats.dispose()

    print("\nOpération terminée avec succès.")
