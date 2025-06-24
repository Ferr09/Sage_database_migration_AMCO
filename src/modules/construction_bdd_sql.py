#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script pour charger les données des ventes et achats depuis des fichiers Excel
vers une base de données PostgreSQL ou MySQL.

Ce script peut également être utilisé pour exporter la structure des bases de données
MySQL ('Ventes' et 'Achats') dans des fichiers texte.
"""

import os
import pandas as pd
import MySQLdb
import json
from pathlib import Path
import importlib.util
import argparse
from sqlalchemy import (
    text, create_engine, Numeric, Integer, TIMESTAMP,
    MetaData, Table, Column
)
from sqlalchemy.exc import SQLAlchemyError

# Importations depuis les modules locaux
from src.models.tables import metadata_ventes, metadata_achats
from src.outils.chemins import dossier_config, dossier_xlsx_propres


def detect_driver():
    """Détecte le driver de base de données SQL disponible."""
    if importlib.util.find_spec("MySQLdb"):
        return "mysql+mysqldb"
    elif importlib.util.find_spec("psycopg2"):
        return "postgresql+psycopg2"
    else:
        raise ImportError("Aucun driver compatible (MySQLdb ou psycopg2) trouvé.")


def charger_fichiers_excel(dossier, fichiers):
    """Charge un ou plusieurs fichiers Excel dans des DataFrames Pandas."""
    tables = {}
    for nom, fichier in fichiers.items():
        chemin = dossier / fichier
        if chemin.exists():
            print(f"Chargement du fichier : {fichier}")
            df = pd.read_excel(chemin, engine='openpyxl')
            for col in df.select_dtypes(include=["object"]).columns:
                df[col] = df[col].astype(str).str.strip().replace('nan', None)
            tables[nom] = df
        else:
            print(f"AVERTISSEMENT : Le fichier {chemin} est introuvable, il sera ignoré.")
    return tables


def forcer_types_donnees(df, table_meta):
    """Force la conversion des types de données d'un DataFrame pour correspondre aux métadonnées SQL."""
    for col, col_meta in table_meta.columns.items():
        if col in df.columns:
            try:
                if isinstance(col_meta.type, (Numeric, Integer)):
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    if isinstance(col_meta.type, Integer):
                        df[col] = df[col].astype('Int64')
                elif isinstance(col_meta.type, TIMESTAMP):
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception:
                pass
    return df.where(pd.notna(df), None)


def gerer_docligne_staging(moteur, df, metadatas, db_type, schema=None, mysql_opts=None):
    """
    Gère le chargement de DOCLIGNE via une table de staging.
    Pour MySQL : utilise CSV + LOAD DATA LOCAL INFILE + optimisations InnoDB.
    Pour PostgreSQL : utilise to_sql(replace) + INSERT…SELECT classique.
    """
    nom_staging = "DOCLIGNE_STAGING"
    nom_final   = "DOCLIGNE"
    table_meta  = metadatas.tables[nom_final]

    # Mise en forme des guillemets uniforme
    if db_type == 'mysql':
        wrap = lambda t: f"`{t}`"
    else:
        wrap = lambda t: f'"{schema}"."{t}"' if schema else f'"{t}"'

    full_stg = wrap(nom_staging)
    tbl_final = wrap(nom_final)
    tbl_art   = wrap("ARTICLES")
    tbl_comp  = wrap("COMPTET")
    cols      = [c.name for c in table_meta.columns]
    cols_fmt  = ", ".join(wrap(c) for c in cols)
    ar_ref    = wrap('AR_Ref')
    ct_num    = wrap('CT_Num')

    sql_transfer = f"""
        INSERT INTO {tbl_final} ({cols_fmt})
        SELECT s.* FROM {full_stg} AS s
          INNER JOIN {tbl_art} a ON s.{ar_ref}=a.{ar_ref}
          INNER JOIN {tbl_comp} c ON s.{ct_num}=c.{ct_num};
    """

    # Pré-exportation CSV si MySQL
    if db_type == 'mysql':
        df_clean = forcer_types_donnees(df.copy(), table_meta)
        csv_path = f"{nom_staging}.csv"
        df_clean.to_csv(csv_path, index=False, header=False) # header=False pour LOAD DATA

    try:
        # ÉTAPE A : Suppression/Création de la table de staging
        with moteur.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {full_stg};"))
            meta = MetaData()
            cols_def = [Column(c.name, c.type) for c in table_meta.columns]
            Table(nom_staging, meta, *cols_def, schema=schema).create(conn)

        # ÉTAPE B : Chargement en masse et insertion
        with moteur.begin() as conn:
            if db_type == 'mysql':
                # Appliquer les optimisations InnoDB
                if mysql_opts:
                    for var, val in mysql_opts.items():
                        conn.execute(text(f"SET SESSION {var} = {val};"))
                # Désactiver les clés étrangères et les index
                conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
                conn.execute(text(f"ALTER TABLE {full_stg} DISABLE KEYS;"))
                # Commande LOAD DATA
                sql_load = f"""
                    LOAD DATA LOCAL INFILE '{os.path.abspath(csv_path)}'
                    INTO TABLE {full_stg}
                    FIELDS TERMINATED BY ','
                    ENCLOSED BY '"'
                    LINES TERMINATED BY '\\n';
                """
                conn.execute(text(sql_load))
                # Réactiver les clés étrangères et les index
                conn.execute(text(f"ALTER TABLE {full_stg} ENABLE KEYS;"))
                conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
            else: # Variante pour PostgreSQL
                df_clean = forcer_types_donnees(df.copy(), table_meta)
                df_clean.to_sql(
                    name=nom_staging, con=conn, schema=schema, if_exists="append",
                    index=False, chunksize=10000, method="multi"
                )

            # Transfert vers la table finale
            result = conn.execute(text(sql_transfer))
            ins = result.rowcount or 0
            total = len(df_clean)
            rej = total - ins
            print(f"  -> Lignes insérées : {ins}, Lignes rejetées : {rej}")

            # Suppression de la table de staging
            conn.execute(text(f"DROP TABLE IF EXISTS {full_stg};"))

        print("  -> La gestion de staging pour DOCLIGNE est terminée avec succès.")
    except SQLAlchemyError as e:
        print(f"  -> ERREUR critique pendant le staging : {e}")
    finally:
        if db_type == 'mysql' and 'csv_path' in locals() and os.path.exists(csv_path):
            os.remove(csv_path)


def inserer_donnees(moteur, tables, metadatas, db_type, schema=None):
    """Orchestre l'insertion des données dans la base de données."""
    print(f"\n--- INSERTION DANS {'le schéma ' + schema if schema else 'la BDD'} ---")
    if not tables:
        print("Aucun DataFrame à insérer.")
        return

    mapping = {"famille": "FAMILLE", "articles": "ARTICLES", "comptet": "COMPTET", "fournisseur": "ARTFOURNISS", "docligne": "DOCLIGNE"}
    ordre = ["famille", "articles", "comptet", "fournisseur", "docligne"]
    cles = sorted(tables.keys(), key=lambda k: next((ordre.index(b) for b in ordre if b in k), 999))
    mysql_opts = {"innodb_flush_log_at_trx_commit": 2, "innodb_log_buffer_size": 67108864, "innodb_flush_method": "'O_DIRECT'"} if db_type == 'mysql' else None

    for cle in cles:
        table_db = mapping.get(next((b for b in ordre if b in cle), None))
        if not table_db: continue
        df = tables[cle]
        if df.empty:
            print(f"[{table_db}] DataFrame vide, ignoré.")
            continue
        cols_meta = set(metadatas.tables[table_db].columns.keys())
        cols_communes = [c for c in df.columns if c in cols_meta]
        df_filtre = df[cols_communes]

        if table_db == "DOCLIGNE":
            gerer_docligne_staging(moteur, df_filtre, metadatas, db_type, schema=schema, mysql_opts=mysql_opts)
        else:
            print(f"[{table_db}] Insertion directe de {len(df_filtre)} lignes...")
            try:
                with moteur.begin() as conn:
                    if db_type == 'mysql': conn.execute(text("SET SESSION innodb_flush_log_at_trx_commit = 2;"))
                    df2 = forcer_types_donnees(df_filtre.copy(), metadatas.tables[table_db])
                    df2.to_sql(name=table_db, con=conn, if_exists="append", index=False, schema=schema, chunksize=5000, method='multi')
                print(f"  -> Succès de l'insertion dans {table_db}")
            except Exception as err:
                msg = err.orig.args[1] if hasattr(err, 'orig') else str(err)
                print(f"  -> ERREUR lors de l'insertion dans {table_db} : {msg}")


def exporter_structure_bdd(moteur, dossier_parent):
    """
    Se connecte à une base de données via un moteur SQLAlchemy et, pour chaque table,
    crée un fichier texte contenant la liste de ses colonnes.
    """
    nom_bdd = moteur.url.database
    if not nom_bdd:
        print(f"  -> ERREUR: Impossible de déterminer le nom de la base de données pour le moteur.")
        return
    dossier_sortie = os.path.join(dossier_parent, nom_bdd)
    os.makedirs(dossier_sortie, exist_ok=True)
    print(f"\n--- Exportation de la structure de '{nom_bdd}' vers le dossier '{dossier_sortie}' ---")
    
    try:
        with moteur.connect() as conn:
            resultat_tables = conn.execute(text("SHOW TABLES;"))
            tables = [table[0] for table in resultat_tables]
            if not tables:
                print("  -> Aucune table trouvée dans cette base de données.")
                return
            print(f"  -> {len(tables)} tables trouvées. Création des fichiers...")
            for nom_table in tables:
                resultat_cols = conn.execute(text(f"SHOW COLUMNS FROM `{nom_table}`;"))
                colonnes = [colonne[0] for colonne in resultat_cols]
                chemin_fichier = os.path.join(dossier_sortie, f"{nom_table}.txt")
                with open(chemin_fichier, 'w', encoding='utf-8') as f:
                    f.write(f"# Colonnes de la table : `{nom_table}`\n\n")
                    for colonne in colonnes: f.write(f"{colonne}\n")
                print(f"    - Fichier '{chemin_fichier}' créé.")
            print(f"--- Exportation de '{nom_bdd}' terminée. ---")
    except SQLAlchemyError as e:
        print(f"  -> ERREUR lors de l'exportation de la structure pour '{nom_bdd}': {e}")


# ==============================================================================
# --- POINT D'ENTRÉE DU SCRIPT ---
# ==============================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script de chargement de données ou d'exportation de la structure d'une BDD.")
    parser.add_argument("--db-type", choices=["postgresql", "mysql"], required=True, help="Spécifie le type de base de données cible.")
    parser.add_argument("--exporter-structure", action="store_true", help="Si présent, exporte la structure de la BDD et quitte.")
    args = parser.parse_args()
    db_type = args.db_type

    cfg_path = dossier_config / f"{db_type}_config.json"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Fichier de configuration introuvable : {cfg_path}")
    config = json.load(open(cfg_path, "r", encoding="utf-8"))

    driver = detect_driver()
    base_url = f"{driver}://{config['db_user']}:{config['db_password']}@{config['db_host']}:{config['db_port']}/"
    connect_args = {}

    if db_type == "postgresql":
        moteur = create_engine(base_url + config['db_name'], connect_args=connect_args)
        mv = ma = moteur
    else: # mysql
        auto_engine = create_engine(base_url, connect_args={'local_infile': 1}, isolation_level="AUTOCOMMIT")
        print("● Ajustement de 'max_allowed_packet' pour MySQL...")
        with auto_engine.connect() as c: c.execute(text("SET GLOBAL max_allowed_packet = 134217728"))
        auto_engine.dispose()
        print("● Création des moteurs de connexion MySQL...")
        moteur = create_engine(base_url, connect_args={'local_infile': 1}, isolation_level="AUTOCOMMIT")
        mv = create_engine(base_url + "Ventes", connect_args={'local_infile': 1}, isolation_level="AUTOCOMMIT")
        ma = create_engine(base_url + "Achats", connect_args={'local_infile': 1}, isolation_level="AUTOCOMMIT")

    if args.exporter_structure:
        if db_type == "mysql":
            dossier_principal_export = "structure_exportee"
            exporter_structure_bdd(mv, dossier_principal_export)
            exporter_structure_bdd(ma, dossier_principal_export)
        else:
            print("L'exportation de structure n'est actuellement implémentée que pour MySQL.")
        print("\nOpération d'exportation terminée.")
        exit()

    print(f"Réinitialisation de la structure pour {db_type.upper()}...")
    with moteur.connect() as conn:
        with conn.begin():
            if db_type == "postgresql":
                conn.execute(text('DROP SCHEMA IF EXISTS "Ventes" CASCADE;'))
                conn.execute(text('DROP SCHEMA IF EXISTS "Achats" CASCADE;'))
                conn.execute(text('CREATE SCHEMA IF NOT EXISTS "Ventes";'))
                conn.execute(text('CREATE SCHEMA IF NOT EXISTS "Achats";'))
            else:
                conn.execute(text("DROP DATABASE IF EXISTS Ventes;"))
                conn.execute(text("DROP DATABASE IF EXISTS Achats;"))
                conn.execute(text("CREATE DATABASE Ventes CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
                conn.execute(text("CREATE DATABASE Achats CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
    print("Les structures de la base de données ont été configurées.")

    if db_type == "postgresql":
        metadata_ventes.create_all(moteur, schema="Ventes")
        metadata_achats.create_all(moteur, schema="Achats")
    else:
        metadata_ventes.create_all(mv)
        metadata_achats.create_all(ma)
    print("Les tables ont été créées avec succès.")

    files_v = {"famille_ventes": "F_FAMILLE_propre.xlsx", "articles_ventes": "F_ARTICLE_propre.xlsx", "comptet_ventes": "F_COMPTET_propre.xlsx", "docligne_ventes": "F_DOCLIGNE_propre.xlsx"}
    files_a = {"famille_achats": "F_FAMILLE_propre.xlsx", "articles_achats": "F_ARTICLE_propre.xlsx", "comptet_achats": "F_COMPTET_propre.xlsx", "fournisseur_achats": "F_ARTFOURNISS_propre.xlsx", "docligne_achats": "F_DOCLIGNE_propre.xlsx"}
    tv = charger_fichiers_excel(dossier_xlsx_propres, files_v)
    ta = charger_fichiers_excel(dossier_xlsx_propres, files_a)

    if db_type == "postgresql":
        inserer_donnees(moteur, tv, metadata_ventes, db_type, schema="Ventes")
        inserer_donnees(moteur, ta, metadata_achats, db_type, schema="Achats")
    else:
        inserer_donnees(mv, tv, metadata_ventes, db_type)
        inserer_donnees(ma, ta, metadata_achats, db_type)

    moteur.dispose()
    if db_type == "mysql":
        mv.dispose()
        ma.dispose()

    print("\nOpération terminée.")