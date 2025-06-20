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
    1. Crée une table de staging sans contraintes.
    2. Charge rapidement toutes les données du DataFrame dans la table de staging.
    3. Utilise une requête INSERT...SELECT avec des JOINs pour insérer uniquement
       les lignes valides dans la table finale DOCLIGNE.
    4. Compte les rejets et supprime la table de staging.
    """
    nom_table_staging = "DOCLIGNE_STAGING"
    nom_table_finale = "DOCLIGNE"
    
    table_finale_meta = metadatas.tables[nom_table_finale]
    
    # --- 1. Préparer et créer la table de staging ---
    print(f"[{nom_table_finale}] Utilisation de la méthode de staging...")
    
    # Créer une définition de table de staging (clone de la table finale mais sans contraintes)
    meta_staging = MetaData()
    colonnes_staging = [Column(c.name, c.type) for c in table_finale_meta.columns]
    table_staging = Table(nom_table_staging, meta_staging, *colonnes_staging, schema=schema)
    
    with moteur.connect() as conn:
        # Supprimer l'ancienne table de staging si elle existe, puis la créer
        conn.execute(text(f"DROP TABLE IF EXISTS {table_staging.fullname} CASCADE;"))
        conn.commit()
        table_staging.create(conn)
        conn.commit()
        print(f"  -> Table de staging '{table_staging.fullname}' créée.")

    # --- 2. Charger les données brutes dans la table de staging ---
    df_propre = forcer_types_donnees(df.copy(), table_finale_meta)
    
    try:
        with moteur.connect() as conn:
            # Cette insertion est rapide car il n'y a pas de vérification de clés étrangères
            df_propre.to_sql(
                name=nom_table_staging,
                con=conn,
                if_exists="append",
                index=False,
                schema=schema,
                chunksize=10000, # Chunksize plus grand pour la vitesse
                method='multi'
            )
            conn.commit()
        print(f"  -> {len(df_propre)} lignes chargées dans la table de staging.")
    except Exception as e:
        print(f"  -> ERREUR critique lors du chargement dans la table de staging : {e}")
        return

    # --- 3. Transférer les données valides de staging à la table finale ---
    # Noms des colonnes pour les requêtes SQL
    colonnes_str = ", ".join([f'"{c.name}"' for c in table_finale_meta.columns])
    
    # Noms des tables avec gestion du schéma
    if db_type == 'postgresql':
        tbl_staging = f'"{schema}"."{nom_table_staging}"'
        tbl_finale = f'"{schema}"."{nom_table_finale}"'
        tbl_articles = f'"{schema}"."ARTICLES"'
        tbl_comptet = f'"{schema}"."COMPTET"'
    else: # mysql
        tbl_staging = f"`{nom_table_staging}`"
        tbl_finale = f"`{nom_table_finale}`"
        tbl_articles = "`ARTICLES`"
        tbl_comptet = "`COMPTET`"

    # La requête SQL qui fait la magie : insère seulement si les clés existent
    # NOTE : Adaptez les noms des colonnes de jointure (AR_Ref, CT_Num) si nécessaire
    sql_transfert = f"""
        INSERT INTO {tbl_finale} ({colonnes_str})
        SELECT s.*
        FROM {tbl_staging} s
        INNER JOIN {tbl_articles} a ON s."AR_Ref" = a."AR_Ref"
        INNER JOIN {tbl_comptet} c ON s."CT_Num" = c."CT_Num";
    """
    
    with moteur.connect() as conn:
        try:
            # Compter les lignes avant transfert
            total_staging = conn.execute(text(f"SELECT COUNT(*) FROM {tbl_staging}")).scalar()
            
            # Exécuter le transfert
            tx = conn.begin()
            result = conn.execute(text(sql_transfert))
            tx.commit()
            
            lignes_inserees = result.rowcount
            lignes_rejetees = total_staging - lignes_inserees
            
            print(f"  -> Transfert terminé.")
            print(f"    - Lignes valides insérées dans '{nom_table_finale}': {lignes_inserees}")
            print(f"    - Lignes rejetées (références inexistantes): {lignes_rejetees}")

        except Exception as e:
            print(f"  -> ERREUR lors du transfert de staging vers la table finale : {e}")
            if 'tx' in locals() and tx.is_active:
                tx.rollback()
        finally:
            # --- 4. Nettoyage : supprimer la table de staging ---
            conn.execute(text(f"DROP TABLE {tbl_staging} CASCADE;"))
            conn.commit()
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

        # --- Stratégie d'insertion différenciée ---
        if table_db == "DOCLIGNE":
            gerer_docligne_staging(moteur, df_filtre, metadatas, schema, db_type)
        else:
            # Méthode directe pour les tables plus petites
            print(f"[{table_db}] Insertion directe de {len(df_filtre)} lignes...")
            df2 = forcer_types_donnees(df_filtre.copy(), metadatas.tables[table_db])
            try:
                with moteur.connect() as conn:
                    tx = conn.begin()
                    try:
                        df2.to_sql(name=table_db, con=conn, if_exists="append", index=False,
                                   schema=schema, chunksize=1000, method='multi')
                        tx.commit()
                        print(f"  -> Succès de l'insertion dans {table_db}")
                    except Exception as err:
                        tx.rollback()
                        msg = err.orig.args[1] if hasattr(err, 'orig') else str(err)
                        print(f"  -> ERREUR lors de l'insertion dans {table_db} : {msg}")
            except Exception as c_err:
                print(f"  -> ERREUR de connexion pour la table {table_db} : {c_err}")

# ==============================================================================
# --- POINT D'ENTRÉE DU SCRIPT ---
# ==============================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script de chargement de données Ventes/Achats vers une BDD.")
    parser.add_argument("--db-type", choices=["postgresql", "mysql"], required=True, help="Spécifie le type de base de données cible.")
    args = parser.parse_args()
    db_type = args.db_type

    # --- 2. Chargement de la configuration ---
    cfg = dossier_config / f"{db_type}_config.json"
    if not cfg.exists():
        raise FileNotFoundError(f"Fichier de configuration introuvable : {cfg}")
    config = json.load(open(cfg, "r", encoding="utf-8"))

    # --- 3. Création de l'URL de base ---
    driver = detect_driver()
    base = f"{driver}://{config['db_user']}:{config['db_password']}@{config['db_host']}:{config['db_port']}/"
    connect_args = {}
    if db_type == "mysql":
        ssl = "?ssl_disabled=True"
    else:
        ssl = ""

    # --- 4. Création des moteurs de connexion ---
    if db_type == "postgresql":
        moteur = create_engine(base + config['db_name'] + ssl, connect_args=connect_args)
        mv, ma = moteur, moteur
    else: # mysql
        auto = create_engine(base + ssl, isolation_level="AUTOCOMMIT", connect_args=connect_args)
        print("● Ajustement de 'max_allowed_packet' pour MySQL...")
        with auto.connect() as c:
            c.execute(text("SET GLOBAL max_allowed_packet = 134217728"))
        auto.dispose()
        print("● Création des moteurs de connexion MySQL...")
        moteur = create_engine(base + ssl, connect_args=connect_args)
        mv = create_engine(base + "Ventes" + ssl, isolation_level="AUTOCOMMIT", connect_args=connect_args)
        ma = create_engine(base + "Achats" + ssl, isolation_level="AUTOCOMMIT", connect_args=connect_args)

    # --- 5. Réinitialisation de la structure ---
    print(f"Réinitialisation de la structure pour {db_type.upper()}...")
    with moteur.connect() as conn:
        tx = conn.begin()
        if db_type == "postgresql":
            conn.execute(text('DROP SCHEMA IF EXISTS "Ventes" CASCADE;'))
            conn.execute(text('DROP SCHEMA IF EXISTS "Achats" CASCADE;'))
            conn.execute(text('CREATE SCHEMA IF NOT EXISTS "Ventes";'))
            conn.execute(text('CREATE SCHEMA IF NOT EXISTS "Achats";'))
        else: # mysql
            conn.execute(text("DROP DATABASE IF EXISTS Ventes;"))
            conn.execute(text("DROP DATABASE IF EXISTS Achats;"))
            conn.execute(text("CREATE DATABASE Ventes CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
            conn.execute(text("CREATE DATABASE Achats CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
        tx.commit()
    print("Structures de la base de données configurées.")

    # --- 6. Création des tables ---
    if db_type == "postgresql":
        metadata_ventes.create_all(moteur, schema="Ventes")
        metadata_achats.create_all(moteur, schema="Achats")
    else: # mysql
        metadata_ventes.create_all(mv)
        metadata_achats.create_all(ma)
    print("Tables créées avec succès.")

    # --- 7. Définition et chargement des fichiers ---
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

    # --- 8. Insertion des données ---
    if db_type == "postgresql":
        inserer_donnees(moteur, tv, metadata_ventes, schema="Ventes", db_type=db_type)
        inserer_donnees(moteur, ta, metadata_achats, schema="Achats", db_type=db_type)
    else: # mysql
        inserer_donnees(mv, tv, metadata_ventes, db_type=db_type)
        inserer_donnees(ma, ta, metadata_achats, db_type=db_type)

    # --- 9. Libération des connexions ---
    moteur.dispose()
    if db_type == "mysql":
        mv.dispose()
        ma.dispose()

    # --- 10. Message de fin ---
    print("\nOpération terminée.")