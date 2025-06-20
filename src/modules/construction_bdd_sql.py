#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script pour charger les données des ventes et achats depuis des fichiers Excel
vers une base de données PostgreSQL ou MySQL.

Ce script est conçu pour réinitialiser et peupler entièrement les bases de données
ou schémas cibles ('Ventes' et 'Achats') à chaque exécution.
"""

import pandas as pd
import json
from pathlib import Path
from sqlalchemy import text, create_engine, Numeric, Integer, TIMESTAMP
import importlib.util
import argparse

# Importations depuis les modules locaux
from src.models.tables import metadata_ventes, metadata_achats
from src.outils.chemins import dossier_config, dossier_xlsx_propres


def detect_driver():
    """
    Détecte le driver de base de données SQL disponible dans l'environnement.

    Vérifie la présence des bibliothèques 'psycopg2' (pour PostgreSQL) et
    'mysql.connector' (pour MySQL).

    Lève:
        ImportError: Si aucun driver compatible n'est trouvé.

    Renvoie:
        str: La chaîne de connexion du driver à utiliser avec SQLAlchemy
             (ex: 'postgresql+psycopg2').
    """
    if importlib.util.find_spec("psycopg2"):
        return "postgresql+psycopg2"
    elif importlib.util.find_spec("mysql.connector"):
        return "mysql+mysqlconnector"
    else:
        raise ImportError("Aucun driver compatible (psycopg2 ou mysql-connector) trouvé.")


def charger_fichiers_excel(dossier, fichiers):
    """
    Charge un ou plusieurs fichiers Excel dans des DataFrames Pandas.

    Parcourt un dictionnaire de fichiers, les charge et applique un nettoyage
    de base : suppression des espaces superflus dans les chaînes de caractères
    et conversion des chaînes littérales 'nan' en None.

    Paramètres:
        dossier (pathlib.Path): Le chemin du répertoire contenant les fichiers Excel.
        fichiers (dict): Un dictionnaire où les clés sont des noms logiques et
                         les valeurs sont les noms des fichiers Excel à charger.

    Renvoie:
        dict: Un dictionnaire de DataFrames Pandas, où les clés sont les noms
              logiques fournis. Les fichiers non trouvés sont ignorés.
    """
    tables = {}
    for nom, fichier in fichiers.items():
        chemin = dossier / fichier
        if chemin.exists():
            print(f"Chargement : {fichier}")
            df = pd.read_excel(chemin, engine='openpyxl')
            # Nettoyage des colonnes textuelles
            for col in df.select_dtypes(include=["object"]).columns:
                df[col] = df[col].astype(str).str.strip().replace('nan', None)
            tables[nom] = df
        else:
            print(f"AVERTISSEMENT : {chemin} introuvable, ignoré.")
    return tables


def forcer_types_donnees(df, table_meta):
    """
    Force la conversion des types de données d'un DataFrame pour correspondre aux métadonnées SQL.

    Aligne les types des colonnes (numérique, entier, timestamp) sur ceux définis
    dans l'objet Table de SQLAlchemy. Gère les erreurs de conversion en les
    remplaçant par NaN/NaT, puis convertit ces derniers en None pour l'insertion en BDD.

    Paramètres:
        df (pd.DataFrame): Le DataFrame à transformer.
        table_meta (sqlalchemy.Table): L'objet Table de SQLAlchemy contenant la définition
                                     des colonnes de la table cible.

    Renvoie:
        pd.DataFrame: Le DataFrame avec les types corrigés et les valeurs nulles
                      normalisées à None.
    """
    for col, col_meta in table_meta.columns.items():
        if col in df.columns:
            try:
                if isinstance(col_meta.type, (Numeric, Integer)):
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    if isinstance(col_meta.type, Integer):
                        # Utilise Int64 pour supporter les NaN dans les colonnes d'entiers
                        df[col] = df[col].astype('Int64')
                elif isinstance(col_meta.type, TIMESTAMP):
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception:
                # Ignore les erreurs si une colonne entière échoue à la conversion
                pass
    # Remplace les NaN/NaT générés par None, qui est l'équivalent de NULL en SQL
    return df.where(pd.notna(df), None)


def inserer_donnees(moteur, tables, metadatas, schema=None):
    """
    Insère les données des DataFrames dans la base de données de manière transactionnelle.

    Trie les tables pour respecter un ordre d'insertion logique (pour les clés étrangères).
    Pour chaque table, filtre les colonnes pour ne garder que celles qui existent
    dans la BDD, force les types, puis insère les données via 'to_sql'. Une transaction
    est utilisée pour chaque table afin de garantir l'intégrité (commit ou rollback).

    Paramètres:
        moteur (sqlalchemy.Engine): Le moteur SQLAlchemy pour la connexion à la BDD.
        tables (dict): Dictionnaire de DataFrames à insérer.
        metadatas (sqlalchemy.MetaData): L'objet MetaData contenant la structure des tables cibles.
        schema (str, optional): Le nom du schéma cible (principalement pour PostgreSQL).
                                Par défaut à None.
    """
    print(f"\n--- INSERTION DANS {'SCHÉMA ' + schema if schema else 'BDD'} ---")
    if not tables:
        print("Aucun DataFrame à insérer.")
        return

    # Mapping pour faire correspondre le nom du DataFrame au nom de la table en BDD
    mapping = {
        "famille": "FAMILLE", "articles": "ARTICLES",
        "comptet": "COMPTET", "fournisseur": "ARTFOURNISS", "docligne": "DOCLIGNE"
    }
    # Ordre d'insertion pour respecter les dépendances (clés étrangères)
    ordre = ["famille", "articles", "comptet", "fournisseur", "docligne"]
    cles = sorted(tables.keys(), key=lambda k: next((ordre.index(b) for b in ordre if b in k), 999))

    for cle in cles:
        table_db = mapping[next(b for b in ordre if b in cle)]
        df = tables[cle]
        if df.empty:
            print(f"[{table_db}] DataFrame vide, ignoré.")
            continue

        # --- Filtrer pour ne garder que les colonnes qui existent dans la table de destination ---
        cols_meta = set(metadatas.tables[table_db].columns.keys())
        cols_comunes = [c for c in df.columns if c in cols_meta]
        df = df[cols_comunes]

        # Conversion des types de données avant insertion
        df2 = forcer_types_donnees(df.copy(), metadatas.tables[table_db])
        print(f"[{table_db}] Tentative d'insertion de {len(df2)} lignes...")

        try:
            with moteur.connect() as conn:
                # Utilise une transaction pour chaque table
                tx = conn.begin()
                try:
                    df2.to_sql(
                        name=table_db,
                        con=conn,
                        if_exists="append",
                        index=False,
                        schema=schema,
                        chunksize=1000,  # Insère par lots pour optimiser la mémoire
                        method='multi'   # Méthode d'insertion rapide
                    )
                    tx.commit()  # Valide la transaction si tout s'est bien passé
                    print(f"  -> Succès de l'insertion dans {table_db}")
                except Exception as err:
                    tx.rollback()  # Annule la transaction en cas d'erreur
                    # Tente d'extraire un message d'erreur plus clair
                    msg = err.orig.args[1] if hasattr(err, 'orig') else str(err)
                    print(f"  -> ERREUR lors de l'insertion dans {table_db} : {msg}")
        except Exception as c_err:
            print(f"  -> ERREUR de connexion pour la table {table_db} : {c_err}")


# ==============================================================================
# --- POINT D'ENTRÉE DU SCRIPT ---
# ==============================================================================
if __name__ == "__main__":
    # --- 1. Analyse des arguments de la ligne de commande ---
    parser = argparse.ArgumentParser(description="Script de chargement de données Ventes/Achats vers une BDD.")
    parser.add_argument("--db-type", choices=["postgresql", "mysql"], required=True,
                        help="Spécifie le type de base de données cible.")
    args = parser.parse_args()
    db_type = args.db_type

    # --- 2. Chargement de la configuration spécifique à la BDD ---
    cfg = dossier_config / f"{db_type}_config.json"
    if not cfg.exists():
        raise FileNotFoundError(f"Fichier de configuration introuvable : {cfg}")
    config = json.load(open(cfg, "r", encoding="utf-8"))

    # --- 3. Détection du driver et création de l'URL de base ---
    driver = detect_driver()
    base = f"{driver}://{config['db_user']}:{config['db_password']}@" \
           f"{config['db_host']}:{config['db_port']}/"
    ssl = "?ssl_disabled=True" if db_type == "mysql" else ""

    # --- 4. Création des moteurs de connexion (spécifique à PG/MySQL) ---
    if db_type == "postgresql":
        # Pour PostgreSQL, un seul moteur gère tout via les schémas.
        moteur = create_engine(base + config['db_name'] + ssl)
        mv, ma = moteur, moteur  # Les moteurs Ventes et Achats sont les mêmes.
    else: # mysql
        # Pour MySQL, on a besoin de plusieurs moteurs :
        # 1. Un moteur de "service" pour créer/supprimer les BDD.
        auto = create_engine(base + ssl, isolation_level="AUTOCOMMIT")
        print("● Ajustement de 'max_allowed_packet' pour MySQL...")
        with auto.connect() as c:
            c.execute(text("SET GLOBAL max_allowed_packet = 134217728"))
        auto.dispose()

        # 2. Un moteur principal et des moteurs spécifiques pour chaque BDD.
        print("● Création des moteurs de connexion MySQL...")
        moteur = create_engine(base + ssl) # Moteur générique
        mv = create_engine(base + "Ventes" + ssl, isolation_level="AUTOCOMMIT") # Moteur Ventes
        ma = create_engine(base + "Achats" + ssl, isolation_level="AUTOCOMMIT") # Moteur Achats

    # --- 5. Réinitialisation de la structure de la BDD (schémas/bases) ---
    print(f"Réinitialisation de la structure pour {db_type.upper()}...")
    with moteur.connect() as conn:
        if db_type == "postgresql":
            # Pour PostgreSQL, on supprime et recrée les schémas.
            conn.execute(text('DROP SCHEMA IF EXISTS "Ventes" CASCADE;'))
            conn.execute(text('DROP SCHEMA IF EXISTS "Achats" CASCADE;'))
            conn.execute(text('CREATE SCHEMA IF NOT EXISTS "Ventes";'))
            conn.execute(text('CREATE SCHEMA IF NOT EXISTS "Achats";'))
            conn.commit() # commit est nécessaire pour les DDL dans une transaction
        else: # mysql
            # Pour MySQL, on supprime et recrée les bases de données.
            conn.execute(text("DROP DATABASE IF EXISTS Ventes;"))
            conn.execute(text("DROP DATABASE IF EXISTS Achats;"))
            conn.execute(text(
                "CREATE DATABASE Ventes CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
            ))
            conn.execute(text(
                "CREATE DATABASE Achats CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
            ))
    print("Structures de la base de données configurées.")

    # --- 6. Création des tables à partir des métadonnées ---
    if db_type == "postgresql":
        metadata_ventes.create_all(moteur, schema="Ventes")
        metadata_achats.create_all(moteur, schema="Achats")
    else: # mysql
        metadata_ventes.create_all(mv)  # Utilise le moteur spécifique à la BDD Ventes
        metadata_achats.create_all(ma)  # Utilise le moteur spécifique à la BDD Achats
    print("Tables créées avec succès.")

    # --- 7. Définition des fichiers Excel à charger pour Ventes et Achats ---
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

    # --- 8. Chargement des données des fichiers Excel dans des DataFrames ---
    tv = charger_fichiers_excel(dossier_xlsx_propres, files_v)
    ta = charger_fichiers_excel(dossier_xlsx_propres, files_a)

    # --- 9. Insertion des données dans la base de données ---
    if db_type == "postgresql":
        inserer_donnees(moteur, tv, metadata_ventes, schema="Ventes")
        inserer_donnees(moteur, ta, metadata_achats, schema="Achats")
    else: # mysql
        inserer_donnees(mv, tv, metadata_ventes) # Utilise le moteur Ventes
        inserer_donnees(ma, ta, metadata_achats) # Utilise le moteur Achats

    # --- 10. Libération des connexions du pool ---
    moteur.dispose()
    if db_type == "mysql":
        mv.dispose()
        ma.dispose()

    # --- 11. Message de fin ---
    print("\nOpération terminée avec succès.")