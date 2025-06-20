#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script pour charger les données des ventes et achats depuis des fichiers Excel
vers une base de données PostgreSQL ou MySQL.

Ce script utilise une table de staging pour la table 'DOCLIGNE' afin d'accélérer
le processus de chargement et de garantir l'intégrité référentielle en ne
chargeant que les lignes valides.
"""

import pandas as pd
import json
from pathlib import Path
from sqlalchemy import text, create_engine, Numeric, Integer, TIMESTAMP, MetaData, Table, Column
import importlib.util
import argparse

# Importations depuis les modules locaux
from src.models.tables import metadata_ventes, metadata_achats
from src.outils.chemins import dossier_config, dossier_xlsx_propres


def detect_driver():
    """Détecte le driver de base de données SQL disponible."""
    if importlib.util.find_spec("psycopg2"):
        return "postgresql+psycopg2"
    elif importlib.util.find_spec("mysql.connector"):
        return "mysql+mysqlconnector"
    else:
        raise ImportError("Aucun driver compatible (psycopg2 ou mysql-connector) trouvé.")


def charger_fichiers_excel(dossier, fichiers):
    """Charge un ou plusieurs fichiers Excel dans des DataFrames Pandas."""
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


def gerer_docligne_staging(moteur, df, metadatas, schema=None, db_type='postgresql'):
    """
    Gère le chargement de la table DOCLIGNE en utilisant une table de staging.
    """
    nom_table_staging = "DOCLIGNE_STAGING"
    nom_table_finale = "DOCLIGNE"
    
    table_finale_meta = metadatas.tables[nom_table_finale]
    
    print(f"[{nom_table_finale}] Utilisation de la méthode de staging...")
    
    meta_staging = MetaData()
    colonnes_staging = [Column(c.name, c.type) for c in table_finale_meta.columns]
    table_staging = Table(nom_table_staging, meta_staging, *colonnes_staging, schema=schema)
    
    # --- 1. Préparer et créer la table de staging ---
    # CORRECCIÓN: Usar un bloque de transacción explícito para las operaciones DDL.
    try:
        with moteur.connect() as conn:
            with conn.begin(): # Inicia la transacción (commit/rollback automático)
                conn.execute(text(f"DROP TABLE IF EXISTS {table_staging.fullname} CASCADE;"))
                table_staging.create(conn)
        print(f"  -> Table de staging '{table_staging.fullname}' créée.")
    except Exception as e:
        print(f"  -> ERREUR lors de la création de la table de staging : {e}")
        return

    # --- 2. Charger les données brutes dans la table de staging ---
    df_propre = forcer_types_donnees(df.copy(), table_finale_meta)
    
    try:
        # CORRECCIÓN: Aunque to_sql puede funcionar sin transacción explícita, es mejor práctica incluirla.
        with moteur.connect() as conn:
            with conn.begin(): # La transacción asegura que toda la carga sea atómica.
                df_propre.to_sql(
                    name=nom_table_staging,
                    con=conn,
                    if_exists="append",
                    index=False,
                    schema=schema,
                    chunksize=10000,
                    method='multi'
                )
        print(f"  -> {len(df_propre)} lignes chargées dans la table de staging.")
    except Exception as e:
        print(f"  -> ERREUR critique lors du chargement dans la table de staging : {e}")
        return

    # --- 3. Transférer les données valides de staging à la table finale ---
    colonnes_str = ", ".join([f'`{c.name}`' if db_type == 'mysql' else f'"{c.name}"' for c in table_finale_meta.columns])
    
    if db_type == 'postgresql':
        tbl_staging = f'"{schema}"."{nom_table_staging}"'
        tbl_finale = f'"{schema}"."{nom_table_finale}"'
        tbl_articles = f'"{schema}"."ARTICLES"'
        tbl_comptet = f'"{schema}"."COMPTET"'
        # Nombres de columnas con comillas dobles para Postgres
        col_ar_ref = '"AR_Ref"'
        col_ct_num = '"CT_Num"'
    else: # mysql
        tbl_staging = f"`{nom_table_staging}`"
        tbl_finale = f"`{nom_table_finale}`"
        tbl_articles = "`ARTICLES`"
        tbl_comptet = "`COMPTET`"
        # Nombres de columnas con acentos graves para MySQL
        col_ar_ref = '`AR_Ref`'
        col_ct_num = '`CT_Num`'

    sql_transfert = f"""
        INSERT INTO {tbl_finale} ({colonnes_str})
        SELECT s.*
        FROM {tbl_staging} s
        INNER JOIN {tbl_articles} a ON s.{col_ar_ref} = a.{col_ar_ref}
        INNER JOIN {tbl_comptet} c ON s.{col_ct_num} = c.{col_ct_num};
    """
    
    with moteur.connect() as conn:
        try:
            total_staging = conn.execute(text(f"SELECT COUNT(*) FROM {tbl_staging}")).scalar()
            
            with conn.begin() as tx:
                result = conn.execute(text(sql_transfert))
            
            lignes_inserees = result.rowcount
            lignes_rejetees = total_staging - lignes_inserees
            
            print(f"  -> Transfert terminé.")
            print(f"    - Lignes valides insérées dans '{nom_table_finale}': {lignes_inserees}")
            print(f"    - Lignes rejetées (références inexistantes): {lignes_rejetees}")

        except Exception as e:
            print(f"  -> ERREUR lors du transfert de staging vers la table finale : {e}")
        finally:
            # --- 4. Nettoyage : supprimer la table de staging ---
            with conn.begin():
                conn.execute(text(f"DROP TABLE IF EXISTS {tbl_staging} CASCADE;"))
            print(f"  -> Table de staging '{tbl_staging}' supprimée.")


def inserer_donnees(moteur, tables, metadatas, schema=None, db_type='postgresql'):
    """
    Orchestre l'insertion des données. Utilise une méthode de staging pour DOCLIGNE
    et une méthode directe pour les autres tables.
    """
    print(f"\n--- INSERTION DANS {'SCHÉMA ' + schema if schema else 'BDD'} ---")
    if not tables:
        print("Aucun DataFrame à insérer.")
        return

    mapping = {
        "famille": "FAMILLE", "articles": "ARTICLES", "comptet": "COMPTET",
        "fournisseur": "ARTFOURNISS", "docligne": "DOCLIGNE"
    }
    ordre = ["famille", "articles", "comptet", "fournisseur", "docligne"]
    cles = sorted(tables.keys(), key=lambda k: next((ordre.index(b) for b in ordre if b in k), 999))

    for cle in cles:
        table_db = mapping.get(next((b for b in ordre if b in cle), None))
        if not table_db: continue
        
        df = tables[cle]
        if df.empty:
            print(f"[{table_db}] DataFrame vide, ignoré.")
            continue

        cols_meta = set(metadatas.tables[table_db].columns.keys())
        cols_comunes = [c for c in df.columns if c in cols_meta]
        df_filtre = df[cols_comunes]

        if table_db == "DOCLIGNE":
            gerer_docligne_staging(moteur, df_filtre, metadatas, schema, db_type)
        else:
            print(f"[{table_db}] Insertion directe de {len(df_filtre)} lignes...")
            df2 = forcer_types_donnees(df_filtre.copy(), metadatas.tables[table_db])
            try:
                with moteur.connect() as conn:
                    with conn.begin():
                        df2.to_sql(name=table_db, con=conn, if_exists="append", index=False,
                                   schema=schema, chunksize=1000, method='multi')
                print(f"  -> Succès de l'insertion dans {table_db}")
            except Exception as err:
                msg = err.orig.args[1] if hasattr(err, 'orig') else str(err)
                print(f"  -> ERREUR lors de l'insertion dans {table_db} : {msg}")

# ==============================================================================
# --- POINT D'ENTRÉE DU SCRIPT ---
# ==============================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script de chargement de données Ventes/Achats vers une BDD.")
    parser.add_argument("--db-type", choices=["postgresql", "mysql"], required=True, help="Spécifie le type de base de données cible.")
    args = parser.parse_args()
    db_type = args.db_type

    cfg = dossier_config / f"{db_type}_config.json"
    if not cfg.exists():
        raise FileNotFoundError(f"Fichier de configuration introuvable : {cfg}")
    config = json.load(open(cfg, "r", encoding="utf-8"))

    driver = detect_driver()
    base = f"{driver}://{config['db_user']}:{config['db_password']}@{config['db_host']}:{config['db_port']}/"
    connect_args = {}
    if db_type == "mysql":
        ssl = "?ssl_disabled=True"
    else:
        ssl = ""

    if db_type == "postgresql":
        moteur = create_engine(base + config['db_name'] + ssl, connect_args=connect_args)
        mv, ma = moteur, moteur
    else:
        auto = create_engine(base + ssl, isolation_level="AUTOCOMMIT", connect_args=connect_args)
        print("● Ajustement de 'max_allowed_packet' pour MySQL...")
        with auto.connect() as c:
            c.execute(text("SET GLOBAL max_allowed_packet = 134217728"))
        auto.dispose()
        print("● Création des moteurs de connexion MySQL...")
        moteur = create_engine(base + ssl, connect_args=connect_args)
        mv = create_engine(base + "Ventes" + ssl, isolation_level="AUTOCOMMIT", connect_args=connect_args)
        ma = create_engine(base + "Achats" + ssl, isolation_level="AUTOCOMMIT", connect_args=connect_args)

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
    print("Structures de la base de données configurées.")

    if db_type == "postgresql":
        metadata_ventes.create_all(moteur, schema="Ventes")
        metadata_achats.create_all(moteur, schema="Achats")
    else:
        metadata_ventes.create_all(mv)
        metadata_achats.create_all(ma)
    print("Tables créées avec succès.")

    files_v = {
        "famille_ventes": "F_FAMILLE_propre.xlsx", "articles_ventes": "F_ARTICLE_propre.xlsx",
        "comptet_ventes": "F_COMPTET_propre.xlsx", "docligne_ventes": "F_DOCLIGNE_propre.xlsx"
    }
    files_a = {
        "famille_achats": "F_FAMILLE_propre.xlsx", "articles_achats": "F_ARTICLE_propre.xlsx",
        "comptet_achats": "F_COMPTET_propre.xlsx", "fournisseur_achats": "F_ARTFOURNISS_propre.xlsx",
        "docligne_achats": "F_DOCLIGNE_propre.xlsx"
    }
    tv = charger_fichiers_excel(dossier_xlsx_propres, files_v)
    ta = charger_fichiers_excel(dossier_xlsx_propres, files_a)

    if db_type == "postgresql":
        inserer_donnees(moteur, tv, metadata_ventes, schema="Ventes", db_type=db_type)
        inserer_donnees(moteur, ta, metadata_achats, schema="Achats", db_type=db_type)
    else:
        inserer_donnees(mv, tv, metadata_ventes, db_type=db_type)
        inserer_donnees(ma, ta, metadata_achats, db_type=db_type)

    moteur.dispose()
    if db_type == "mysql":
        mv.dispose()
        ma.dispose()

    print("\nOpération terminée.")