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
    """
    Insère un dictionnaire de DataFrames dans la base de données de manière robuste,
    en fournissant un rapport de débogage détaillé à chaque étape.
    """
    print(f"\n--- DÉBUT DU PROCESSUS D'INSERTION DANS {'LE SCHÉMA ' + nom_schema if nom_schema else 'LA BASE DE DONNÉES'} ---")

    if not tables_a_inserer:
        print("Avertissement : Aucun DataFrame à insérer. Le processus est terminé.")
        return

    map_nom_variable_a_nom_table = {
        "famille": "FAMILLE",
        "articles": "ARTICLES",
        "comptet": "COMPTET",
        "fournisseur": "ARTFOURNISS",
        "docligne": "DOCLIGNE"
    }
    ordre_base = ["famille", "articles", "comptet", "fournisseur", "docligne"]
    
    cles_a_traiter = sorted(
        tables_a_inserer.keys(),
        key=lambda k: next((ordre_base.index(b) for b in ordre_base if b in k), float('inf'))
    )

    for nom_cle_variable in cles_a_traiter:
        try:
            base_name = next(b for b in ordre_base if b in nom_cle_variable)
            nom_table_db = map_nom_variable_a_nom_table[base_name]
            
            # --- ÉTAPE 1 : VÉRIFICATION INITIALE ---
            print(f"\n[Traitement de la table : {nom_table_db}]")
            df = tables_a_inserer[nom_cle_variable]
            lignes_initiales = len(df)
            print(f"  - Lignes trouvées dans le fichier Excel : {lignes_initiales}")

            if df.empty:
                print("  -> DataFrame initial vide. Aucune action requise.")
                continue  # Passe à la table suivante

            # --- ÉTAPE 2 : CONVERSION DES TYPES ET VÉRIFICATION ---
            table_metadata = metadatas.tables[nom_table_db]
            df_typed = forcer_types_donnees(df.copy(), table_metadata)
            lignes_apres_conversion = len(df_typed)
            
            print(f"  - Lignes restantes après conversion des types : {lignes_apres_conversion}")

            if lignes_apres_conversion < lignes_initiales:
                pertes = lignes_initiales - lignes_apres_conversion
                print(f"  -> AVERTISSEMENT : {pertes} ligne(s) ont potentiellement été perdues (valeurs non conformes).")

            if df_typed.empty:
                print(f"  -> DataFrame final vide pour '{nom_table_db}'. Aucune ligne ne sera insérée.")
                continue # Passe à la table suivante si le df est maintenant vide

            # --- ÉTAPE 3 : INSERTION EN BASE DE DONNÉES ---
            print(f"  - Tentative d'insertion de {lignes_apres_conversion} lignes dans la table '{nom_table_db}'...")
            df_typed.to_sql(
                name=nom_table_db,
                con=moteur_cible,
                if_exists="append",
                index=False,
                schema=nom_schema,
                chunksize=1000,
                method=None # Pour debugging, on utilise la méthode par défaut, au lieu de method='multi'
            )
            print(f"  -> SUCCÈS : Les données pour '{nom_table_db}' ont été insérées.")

        except StopIteration:
            print(f"Avertissement : La variable '{nom_cle_variable}' n'a pas de correspondance dans l'ordre d'insertion et sera ignorée.")
        except KeyError:
            # Cette erreur est plus précise maintenant
            print(f"ERREUR CRITIQUE : La table '{nom_table_db}' est introuvable dans les métadonnées.")
            print("             Veuillez vérifier que le nom est correct dans `map_nom_variable_a_nom_table` ET dans `tables.py`.")
        except Exception as e:
            print(f"  -> ERREUR INCONNUE lors du traitement de la table '{nom_table_db}': {e}")
            
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
    # La lógica para PostgreSQL no cambia y NO USA AUTOCOMMIT
    moteur = create_engine(base_url + config['db_name'] + ssl_args)
    moteur_ventes = moteur # En PG, usamos el mismo motor para todo
    moteur_achats = moteur # En PG, usamos el mismo motor para todo
else: # mysql
    # Moteur racine pour créer/supprimer les DBs (sin autocommit)
    moteur = create_engine(base_url + ssl_args) 
    
    # ===== LA SOLUCIÓN DEFINITIVA ESTÁ AQUÍ =====
    # Creamos motores específicos para cada DB con AUTOCOMMIT activado.
    print("Configuration des moteurs pour MySQL avec AUTOCOMMIT...")
    moteur_ventes = create_engine(base_url + "Ventes" + ssl_args, isolation_level="AUTOCOMMIT")
    moteur_achats = create_engine(base_url + "Achats" + ssl_args, isolation_level="AUTOCOMMIT")

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

    # ===== DÉBUT DU CODE DE DÉBOGAGE 1 =====
    print("\n--- DÉBOGAGE : VÉRIFICATION DES DATAFRAMES APRÈS CHARGEMENT ---")
    for nom, df in tables_ventes.items():
        if not df.empty:
            print(f"[Ventes] DataFrame '{nom}' chargé avec {len(df)} lignes. Deux premières lignes :")
            print(df.head(2))
        else:
            print(f"[Ventes] ¡¡¡AVERTISSEMENT!!! Le DataFrame '{nom}' est VIDE après chargement.")

    for nom, df in tables_achats.items():
        if not df.empty:
            print(f"[Achats] DataFrame '{nom}' chargé avec {len(df)} lignes. Deux premières lignes :")
            print(df.head(2))
        else:
            print(f"[Achats] ¡¡¡AVERTISSEMENT!!! Le DataFrame '{nom}' est VIDE après chargement.")
    print("--- FIN DU DÉBOGAGE 1 ---\n")
    # ===== FIN DU CODE DE DÉBOGAGE 1 =====

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