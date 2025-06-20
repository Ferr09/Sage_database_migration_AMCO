#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import json
from pathlib import Path
from sqlalchemy import text, create_engine, Numeric, Integer, TIMESTAMP
import importlib.util
import argparse

from src.models.tables import metadata_ventes, metadata_achats
from src.outils.chemins import dossier_config, dossier_xlsx_propres

def detect_driver():
    if importlib.util.find_spec("psycopg2"):
        return "postgresql+psycopg2"
    elif importlib.util.find_spec("mysql.connector"):
        return "mysql+mysqlconnector"
    else:
        raise ImportError("Aucun driver compatible (psycopg2 ou mysql-connector) trouvé.")

def charger_fichiers_excel(dossier, fichiers):
    tables = {}
    for nom, fichier in fichiers.items():
        chemin = dossier / fichier
        if chemin.exists():
            print(f"Chargement : {fichier}")
            df = pd.read_excel(chemin, engine='openpyxl')
            for col in df.select_dtypes(include=["object"]).columns:
                df[col] = df[col].astype(str).str.strip().replace('nan', None)
            tables[nom] = df
        else:
            print(f"AVERTISSEMENT : {chemin} introuvable, ignoré.")
    return tables

def forcer_types_donnees(df, table_meta):
    for col, col_meta in table_meta.columns.items():
        if col in df.columns:
            try:
                if isinstance(col_meta.type, (Numeric, Integer)):
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    if isinstance(col_meta.type, Integer):
                        df[col] = df[col].astype('Int64')
                elif isinstance(col_meta.type, TIMESTAMP):
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            except:
                pass
    return df.where(pd.notna(df), None)

def inserer_donnees(moteur, tables, metadatas, schema=None):
    print(f"\n--- INSERTION DANS {'SCHÉMA ' + schema if schema else 'BDD'} ---")
    if not tables:
        print("Aucun DataFrame à insérer.")
        return

    mapping = {
        "famille": "FAMILLE", "articles": "ARTICLES",
        "comptet": "COMPTET", "fournisseur": "ARTFOURNISS", "docligne": "DOCLIGNE"
    }
    ordre = ["famille", "articles", "comptet", "fournisseur", "docligne"]
    cles = sorted(tables.keys(), key=lambda k: next((ordre.index(b) for b in ordre if b in k), 999))

    for cle in cles:
        table_db = mapping[next(b for b in ordre if b in cle)]
        df = tables[cle]
        if df.empty:
            print(f"[{table_db}] vide, skipped.")
            continue

        df2 = forcer_types_donnees(df.copy(), metadatas.tables[table_db])
        print(f"[{table_db}] tentative d'insertion de {len(df2)} lignes...")

        try:
            with moteur.connect() as conn:
                tx = conn.begin()
                try:
                    df2.to_sql(
                        name=table_db, con=conn, if_exists="append",
                        index=False, schema=schema, chunksize=1000, method='multi'
                    )
                    tx.commit()
                    print(f"  -> Succès {table_db}")
                except Exception as err:
                    tx.rollback()
                    msg = err.orig.args[1] if hasattr(err, 'orig') else str(err)
                    print(f"  -> ERREUR {table_db} : {msg}")
        except Exception as c_err:
            print(f"  -> ERREUR connexion {table_db} : {c_err}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chargement BDD Ventes/Achats")
    parser.add_argument("--db-type", choices=["postgresql", "mysql"], required=True)
    args = parser.parse_args()
    db_type = args.db_type

    cfg = dossier_config / f"{db_type}_config.json"
    if not cfg.exists():
        raise FileNotFoundError(f"Config introuvable : {cfg}")
    config = json.load(open(cfg, "r", encoding="utf-8"))

    driver = detect_driver()
    base = f"{driver}://{config['db_user']}:{config['db_password']}@" \
           f"{config['db_host']}:{config['db_port']}/"
    ssl = "?ssl_disabled=True" if db_type == "mysql" else ""

    if db_type == "postgresql":
        moteur = create_engine(base + config['db_name'] + ssl)
        mv, ma = moteur, moteur
    else:
        auto = create_engine(base + ssl, isolation_level="AUTOCOMMIT")
        print("● SET GLOBAL max_allowed_packet = 134217728")
        with auto.connect() as c:
            c.execute(text("SET GLOBAL max_allowed_packet = 134217728"))
        auto.dispose()

        print("● Recreation des engines MySQL")
        moteur = create_engine(base + ssl)
        mv = create_engine(base + "Ventes" + ssl, isolation_level="AUTOCOMMIT")
        ma = create_engine(base + "Achats" + ssl, isolation_level="AUTOCOMMIT")

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
                "CREATE DATABASE Ventes CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
            ))
            conn.execute(text(
                "CREATE DATABASE Achats CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
            ))
    print("Structures configurées.")

    if db_type == "postgresql":
        metadata_ventes.create_all(moteur, schema="Ventes")
        metadata_achats.create_all(moteur, schema="Achats")
    else:
        metadata_ventes.create_all(mv)
        metadata_achats.create_all(ma)
    print("Tables créées.")

    files_v = {
        "famille_ventes": "F_FAMILLE_propre.xlsx",
        "articles_ventes": "F_ARTICLE_propre.xlsx",
        "comptet_ventes": "F_COMPTET_propre.xlsx",
        "docligne_ventes": "F_DOCLIGNE_propre.xlsx"
    }
    files_a = {
        "famille_achats": "F_FAMILLE_propre.xlsx",
        "articles_achats": "F_ARTICLE_propre.xlsx",
        "comptet_achats": "F_COMPTET_propre.xlsx",
        "fournisseur_achats": "F_ARTFOURNISS_propre.xlsx",
        "docligne_achats": "F_DOCLIGNE_propre.xlsx"
    }
    tv = charger_fichiers_excel(dossier_xlsx_propres, files_v)
    ta = charger_fichiers_excel(dossier_xlsx_propres, files_a)

    if db_type == "postgresql":
        inserer_donnees(moteur, tv, metadata_ventes, schema="Ventes")
        inserer_donnees(moteur, ta, metadata_achats, schema="Achats")
    else:
        inserer_donnees(mv, tv, metadata_ventes)
        inserer_donnees(ma, ta, metadata_achats)

    moteur.dispose()
    if db_type == "mysql":
        mv.dispose()
        ma.dispose()

    print("\nOpération terminée.")  
