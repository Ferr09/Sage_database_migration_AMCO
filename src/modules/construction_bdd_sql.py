#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script pour charger les données des ventes et achats depuis des fichiers Excel
vers une base de données PostgreSQL ou MySQL.
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
from contextlib import contextmanager

# Importations depuis les modules locaux du projet
try:
    from src.models.tables import metadata_ventes, metadata_achats
    from src.outils.chemins import dossier_config, dossier_xlsx_propres, dossier_csv_extraits
except ImportError:
    print("AVERTISSEMENT: Impossible d'importer les modules locaux. Assurez-vous que le script est exécuté depuis la racine du projet.")
    # On définit des chemins par défaut pour que le script puisse s'exécuter
    dossier_config = Path('./config')
    dossier_xlsx_propres = Path('./donnees_propres')


@contextmanager
def mysql_global_optimiser(moteur, variable, valeur_temp):
    """
    Gestionnaire de contexte pour changer TEMPORAIREMENT une variable GLOBALE MySQL.
    ATTENTION : Nécessite des privilèges élevés (SUPER ou SYSTEM_VARIABLES_ADMIN).
    """
    connexion = None
    valeur_originale = None
    try:
        connexion = moteur.connect()
        # 1. Lire et sauvegarder la valeur originale
        resultat = connexion.execute(text(f"SELECT @@GLOBAL.{variable};"))
        valeur_originale = resultat.scalar()
        print(f"● [Optimisation] Valeur originale de '{variable}' : {valeur_originale}")

        # 2. Appliquer la nouvelle valeur temporaire si elle est différente
        if str(valeur_originale) != str(valeur_temp):
            print(f"● [Optimisation] Changement de '{variable}' à '{valeur_temp}'...")
            connexion.execute(text(f"SET GLOBAL {variable} = {valeur_temp};"))
        
        # Cède le contrôle au bloc 'with'
        yield

    except SQLAlchemyError as e:
        print(f"  -> ERREUR: Impossible de changer la variable globale '{variable}'.")
        print(f"  -> Vérifiez les privilèges de l'utilisateur. Erreur d'origine : {e}")
        raise # Relance l'erreur pour arrêter le script

    finally:
        # 3. Restaurer la valeur originale, quoi qu'il arrive
        if valeur_originale is not None and str(valeur_originale) != str(valeur_temp):
            print(f"● [Optimisation] Restauration de '{variable}' à sa valeur originale '{valeur_originale}'...")
            if connexion and not connexion.closed:
                 connexion.execute(text(f"SET GLOBAL {variable} = {valeur_originale};"))
        if connexion:
            connexion.close()


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
            
            # --- LOGIQUE DE CONSOLIDATION INTELLIGENTE POUR ACHATS ---
            if "ARTFOURNISS" in fichier:
                if 'AF_REFFOURNISS' in df.columns:
                    df.dropna(subset=['AF_REFFOURNISS'], inplace=True) # Pré-nettoyage
                    comptes = df['AF_REFFOURNISS'].value_counts()
                    refs_doublons = comptes[comptes > 1].index.tolist()

                    if refs_doublons:
                        print(f"  -> Consolidation de {len(refs_doublons)} référence(s) fournisseur en double...")
                        df_uniques = df[~df['AF_REFFOURNISS'].isin(refs_doublons)]
                        df_a_consolider = df[df['AF_REFFOURNISS'].isin(refs_doublons)]
                        
                        def premier_non_nul(series):
                            valeurs_non_nulles = series.dropna()
                            return valeurs_non_nulles.iloc[0] if not valeurs_non_nulles.empty else None

                        df_consolide = df_a_consolider.groupby('AF_REFFOURNISS', as_index=False).agg(premier_non_nul)
                        df = pd.concat([df_uniques, df_consolide], ignore_index=True)
                        print(f"  -> Consolidation terminée. Total de lignes final : {len(df)}")
            # --- FIN DE LA LOGIQUE DE CONSOLIDATION ---

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


def gerer_docligne_staging(moteur, df, metadatas, db_type, schema=None):
    """
    Gère le chargement de DOCLIGNE via staging.
    Filtre les DL_NO corrompus puis charge directement le CSV d'extraction
    en évitant le rewrite de fichier temporaire.
    """
    nom_staging = "DOCLIGNE_STAGING"
    nom_final   = "DOCLIGNE"
    table_meta  = metadatas.tables[nom_final]

    # 1) Préparation des identifiants SQL
    if db_type == 'mysql':
        wrap = lambda t: f"`{t}`"
    else:
        wrap = lambda t: f'"{schema}"."{t}"' if schema else f'"{t}"'

    full_stg = wrap(nom_staging)
    tbl_fin  = wrap(nom_final)
    tbl_art  = wrap("ARTICLES")
    tbl_comp = wrap("COMPTET")

    colonnes = [c.name for c in table_meta.columns]
    cols_fmt = ", ".join(wrap(c) for c in colonnes)
    ar_ref   = wrap("AR_Ref")
    ct_num   = wrap("CT_Num")

    sql_transfer = f"""
        INSERT INTO {tbl_fin} ({cols_fmt})
        SELECT {cols_fmt}
          FROM {full_stg} AS s
          INNER JOIN {tbl_art} AS a ON s.{ar_ref}=a.{ar_ref}
          INNER JOIN {tbl_comp} AS c ON s.{ct_num}=c.{ct_num};
    """

    # 2) Charger le CSV original (exporté par F_DOCLIGNE.csv)
    chemin_csv = dossier_csv_extraits / "F_DOCLIGNE.csv"
    if not chemin_csv.exists():
        raise FileNotFoundError(f"Le fichier source {chemin_csv} est manquant.")

    # 3) DROP & CREATE table staging (transaction isolée)
    with moteur.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {full_stg};"))
        meta = MetaData()
        cols_def = [Column(c.name, c.type) for c in table_meta.columns]
        Table(nom_staging, meta, *cols_def, schema=schema).create(conn)

    # 4) Bulk‐load MySQL (ligne Unix puis Windows)
    if db_type == 'mysql':
        sql_load_base = f"""
            LOAD DATA LOCAL INFILE '{chemin_csv.resolve().as_posix()}'
            INTO TABLE {full_stg}
            CHARACTER SET utf8
            FIELDS TERMINATED BY ','
            ENCLOSED BY '"'
            LINES TERMINATED BY '\\n'
            IGNORE 1 LINES;
        """
        sql_load_win = sql_load_base.replace("\\n", "\\r\\n")

        with moteur.begin() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
            conn.execute(text(f"ALTER TABLE {full_stg} DISABLE KEYS;"))
            try:
                conn.execute(text(sql_load_base))
            except Exception:
                conn.execute(text(sql_load_win))
            conn.execute(text(f"ALTER TABLE {full_stg} ENABLE KEYS;"))
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
    else:
        # Variante PostgreSQL
        df_clean = forcer_types_donnees(df.copy(), table_meta)
        df_clean.to_sql(
            name=nom_staging,
            con=moteur,
            schema=schema,
            if_exists="append",
            index=False,
            chunksize=10000,
            method="multi"
        )

    # 5) Transfert et diagnostic
    with moteur.begin() as conn:
        total_stg = conn.execute(text(f"SELECT COUNT(*) FROM {full_stg};")).scalar()
        result    = conn.execute(text(sql_transfer))
        ins       = result.rowcount or 0
        rej       = total_stg - ins
        print(f"  -> Chargées en staging : {total_stg}, insérées : {ins}, rejetées : {rej}")
        conn.execute(text(f"DROP TABLE IF EXISTS {full_stg};"))


def inserer_donnees(moteur, tables, metadatas, db_type, schema=None):
    """
    Orchestre l'insertion des données.
    Désactive temporairement les contraintes de clés étrangères pour MySQL pour une insertion en masse.
    """
    print(f"\n--- INSERTION DANS {'le schéma ' + schema if schema else 'la BDD'} ---")
    if not tables:
        print("Aucun DataFrame à insérer.")
        return

    connexion_principale = moteur.connect()
    
    try:
        if db_type == 'mysql':
            # --- MODIFICATION : Désactiver les contraintes de clés étrangères ---
            print("● [Optimisation] Désactivation des contraintes de clés étrangères...")
            connexion_principale.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))

        mapping = {"famille": "FAMILLE", "articles": "ARTICLES", "comptet": "COMPTET", "fournisseur": "ARTFOURNISS", "docligne": "DOCLIGNE"}
        ordre = ["famille", "articles", "comptet", "fournisseur", "docligne"]
        cles = sorted(tables.keys(), key=lambda k: next((ordre.index(b) for b in ordre if b in k), 999))

        for cle in cles:
            table_db = mapping.get(next((b for b in ordre if b in cle), None))
            if not table_db: continue
            df = tables[cle]
            if df.empty:
                print(f"[{table_db}] DataFrame vide, ignoré.")
                continue
            cols_communes = [c for c in df.columns if c in metadatas.tables[table_db].columns.keys()]
            df_filtre = df[cols_communes]

            if table_db == "DOCLIGNE":
                # La méthode de staging pour DOCLIGNE est déjà robuste grâce au INNER JOIN.
                gerer_docligne_staging(moteur, df_filtre, metadatas, db_type, schema=schema)
            else:
                print(f"[{table_db}] Insertion directe de {len(df_filtre)} lignes...")
                try:
                    # On utilise la connexion principale qui a les FK désactivées
                    df2 = forcer_types_donnees(df_filtre.copy(), metadatas.tables[table_db])
                    df2.to_sql(name=table_db, con=connexion_principale, if_exists="append", index=False, schema=schema, chunksize=5000, method='multi')
                    print(f"  -> Succès de l'insertion dans {table_db}")
                except Exception as err:
                    msg = err.orig.args[1] if hasattr(err, 'orig') else str(err)
                    print(f"  -> ERREUR lors de l'insertion dans {table_db} : {msg}")
                    # On pourrait choisir d'arrêter tout le script ici si une table échoue
                    # raise

    finally:
        # --- MODIFICATION : Le bloc 'finally' garantit que les contraintes sont toujours réactivées ---
        if db_type == 'mysql':
            print("[Optimisation] Réactivation des contraintes de clés étrangères...")
            connexion_principale.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
        
        # On ferme la connexion manuelle
        connexion_principale.close()

# ==============================================================================
# --- POINT D'ENTRÉE DU SCRIPT ---
# ==============================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script de chargement de données pour Ventes/Achats.")
    parser.add_argument("--db-type", choices=["postgresql", "mysql"], required=True, help="Spécifie le type de base de données cible.")
    args = parser.parse_args()
    db_type = args.db_type

    cfg_path = dossier_config / f"{db_type}_config.json"
    if not cfg_path.exists(): raise FileNotFoundError(f"Fichier de configuration introuvable : {cfg_path}")
    config = json.load(open(cfg_path, "r", encoding="utf-8"))

    driver = detect_driver()
    base_url = f"{driver}://{config['db_user']}:{config['db_password']}@{config['db_host']}:{config['db_port']}/"
    
    if db_type == "postgresql":
        moteur = create_engine(base_url + config['db_name'])
        mv = ma = moteur
    else:
        auto_engine = create_engine(base_url, connect_args={'local_infile': 1}, isolation_level="AUTOCOMMIT")
        print("● Ajustement de 'max_allowed_packet' pour MySQL...")
        with auto_engine.connect() as c: c.execute(text("SET GLOBAL max_allowed_packet = 134217728"))
        auto_engine.dispose()
        print("● Création des moteurs de connexion MySQL...")
        moteur = create_engine(base_url, connect_args={'local_infile': 1}, isolation_level="AUTOCOMMIT")
        mv = create_engine(base_url + "Ventes", connect_args={'local_infile': 1}, isolation_level="AUTOCOMMIT")
        ma = create_engine(base_url + "Achats", connect_args={'local_infile': 1}, isolation_level="AUTOCOMMIT")

    print(f"Réinitialisation de la structure pour {db_type.upper()}...")
    with moteur.connect() as conn:
        with conn.begin():
            if db_type == "postgresql":
                conn.execute(text('DROP SCHEMA IF EXISTS "Ventes" CASCADE; CREATE SCHEMA "Ventes";'))
                conn.execute(text('DROP SCHEMA IF EXISTS "Achats" CASCADE; CREATE SCHEMA "Achats";'))
            else:
                conn.execute(text("DROP DATABASE IF EXISTS Ventes; CREATE DATABASE Ventes CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
                conn.execute(text("DROP DATABASE IF EXISTS Achats; CREATE DATABASE Achats CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
    print("Les structures de la base de données ont été configurées.")

    if db_type == "postgresql":
        metadata_ventes.create_all(moteur, schema="Ventes"); metadata_achats.create_all(moteur, schema="Achats")
    else:
        metadata_ventes.create_all(mv); metadata_achats.create_all(ma)
    print("Les tables ont été créées avec succès.")

    files_v = {"famille_ventes": "F_FAMILLE_propre.xlsx", "articles_ventes": "F_ARTICLE_propre.xlsx", "comptet_ventes": "F_COMPTET_propre.xlsx", "docligne_ventes": "F_DOCLIGNE_propre.xlsx"}
    files_a = {"famille_achats": "F_FAMILLE_propre.xlsx", "articles_achats": "F_ARTICLE_propre.xlsx", "comptet_achats": "F_COMPTET_propre.xlsx", "fournisseur_achats": "F_ARTFOURNISS_propre.xlsx", "docligne_achats": "F_DOCLIGNE_propre.xlsx"}
    tv = charger_fichiers_excel(dossier_xlsx_propres, files_v)
    ta = charger_fichiers_excel(dossier_xlsx_propres, files_a)

    try:
        if db_type == "mysql":
            with mysql_global_optimiser(moteur, 'innodb_flush_log_at_trx_commit', 2):
                inserer_donnees(mv, tv, metadata_ventes, db_type)
                inserer_donnees(ma, ta, metadata_achats, db_type)
        else:
            inserer_donnees(moteur, tv, metadata_ventes, db_type, schema="Ventes")
            inserer_donnees(moteur, ta, metadata_achats, db_type, schema="Achats")
    except Exception as e:
        print(f"\nUne erreur majeure est survenue pendant l'insertion des données : {e}")
    finally:
        moteur.dispose()
        if db_type == "mysql": mv.dispose(); ma.dispose()

    print("\nOpération terminée.")