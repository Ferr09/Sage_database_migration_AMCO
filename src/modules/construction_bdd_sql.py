#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unicodedata
import pandas as pd
import json
from pathlib import Path
from sqlalchemy import Date, text, Numeric, create_engine, Table, Column, String, MetaData, ForeignKey, Float as SQLFloat
from sqlalchemy.exc import SQLAlchemyError
import importlib.util
import argparse
import time

# --------------------------------------------------------------------
# Importation des métadonnées et définitions de tables depuis models.tables
# --------------------------------------------------------------------
from src.models.tables import (
    metadata_ventes, metadata_achats,
    famille_ventes, articles_ventes, comptet_ventes, docligne_ventes,
    famille_achats, articles_achats, comptet_achats, fournisseur_achats, docligne_achats
)

# --------------------------------------------------------------------
# Importation des chemins absolus depuis outils.chemins
# --------------------------------------------------------------------
from src.outils.chemins import racine_projet, dossier_config, dossier_xlsx_propres

# --------------------------------------------------------------------
# Fonctions utilitaires locales
# --------------------------------------------------------------------
def detect_driver():
    """
    Détecte quel driver SQL est installé (psycopg2 ou pymysql)
    et retourne la chaîne driver SQLAlchemy sans port.
    """
    if importlib.util.find_spec("psycopg2"):
        return "postgresql+psycopg2"
    elif importlib.util.find_spec("pymysql"):
        return "mysql+pymysql"
    else:
        raise ImportError("Aucun driver compatible détecté (psycopg2 ou pymysql).")

def filtrer_colonnes(df: pd.DataFrame, table_sqlalchemy: Table) -> pd.DataFrame:
    """
    Garde uniquement les colonnes du DataFrame qui figurent dans la définition SQLAlchemy.
    """
    colonnes_sql = [col.name for col in table_sqlalchemy.columns]
    return df.loc[:, df.columns.intersection(colonnes_sql)]

def nettoyer_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie un DataFrame en :
    - supprimant les colonnes entièrement vides
    - remplaçant 'nan' par None
    - convertissant les colonnes contenant 'DATE' en type date
    - remplaçant NaN par None
    """
    df = df.dropna(axis=1, how="all")
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].replace('nan', None)
    for col in df.columns:
        if "DATE" in col.upper():
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    return df.where(pd.notna(df), None)

def nettoyer_texte_objet(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie les colonnes de type objet (texte) pour éviter les problèmes d'encodage :
    - Normalise les accents et caractères spéciaux.
    - Remplace 'nan' par None.
    """
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = (
            df[col].astype(str)
                  .replace('nan', None)
                  .apply(lambda x: unicodedata.normalize('NFKC', x)
                         .encode('utf-8', errors='replace')
                         .decode('utf-8', errors='replace') if x else None)
        )
    return df

# --------------------------------------------------------------------
# Lecture de l’argument --db-type (PostgreSQL ou MySQL)
# --------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Construction de la BDD (PostgreSQL/MySQL)")
parser.add_argument(
    "--db-type",
    choices=["postgresql", "mysql"],
    required=True,
    help="Type de base de données : 'postgresql' ou 'mysql'"
)
args = parser.parse_args()
db_type = args.db_type

# --------------------------------------------------------------------
# Détermination du chemin vers le fichier de configuration JSON
# --------------------------------------------------------------------
if db_type == "postgresql":
    chemin_config = dossier_config / "postgres_config.json"
else:
    chemin_config = dossier_config / "mysql_config.json"

if not chemin_config.exists():
    raise FileNotFoundError(f"Le fichier de configuration {chemin_config} est introuvable.")

# --------------------------------------------------------------------
# Chargement du JSON de configuration
# --------------------------------------------------------------------
with open(chemin_config, "r", encoding="utf-8") as f:
    config = json.load(f)

# --------------------------------------------------------------------
# Validation de la présence du port dans la configuration
# --------------------------------------------------------------------
port_config = config.get("db_port")
if not port_config or str(port_config).strip() in {"", "0", "null"}:
    raise KeyError(f"Le port n'est pas défini correctement dans {chemin_config}.")

# --------------------------------------------------------------------
# Détection du driver SQL sans supposer le port
# --------------------------------------------------------------------
driver = detect_driver()

# --------------------------------------------------------------------
# Construction de l’URL de connexion SQLAlchemy
# --------------------------------------------------------------------
url_connexion = (
    f"{driver}://{config['db_user']}:{config['db_password']}"
    f"@{config['db_host']}:{config['db_port']}/{config['db_name']}?ssl_disabled=True"
)

# Création du moteur SQLAlchemy
moteur = create_engine(url_connexion)

# --------------------------------------------------------------------
# Création (ou recréation) des schémas « Achats » et « Ventes »
# --------------------------------------------------------------------
utilisateur = config["db_user"]
with moteur.begin() as conn:
    conn.execute(text('DROP SCHEMA IF EXISTS "Ventes" CASCADE;'))
    conn.execute(text('DROP SCHEMA IF EXISTS "Achats" CASCADE;'))
    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "Achats" AUTHORIZATION "{utilisateur}";'))
    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "Ventes" AUTHORIZATION "{utilisateur}";'))


time.sleep(1)

# Test de la connexion
try:
    with moteur.connect() as connexion:
        print("Connexion réussie à la base de données.")
except Exception as erreur:
    print("Échec de la connexion :", erreur)
    exit(1)

# --------------------------------------------------------------------
# Création des tables physiques en base si nécessaire
# --------------------------------------------------------------------
metadata_achats.create_all(moteur, checkfirst=True)
metadata_ventes.create_all(moteur, checkfirst=True)

# --------------------------------------------------------------------
# Définition du dossier contenant les fichiers Excel nettoyés
# --------------------------------------------------------------------
chemin_dossier_xlsx = dossier_xlsx_propres

# --------------------------------------------------------------------
# Fichiers Excel à charger pour le schéma Ventes
# --------------------------------------------------------------------
fichiers_ventes = {
    "famille_ventes":  "F_FAMILLE_propre.xlsx",
    "articles_ventes": "F_ARTICLE_propre.xlsx",
    "comptet_ventes":  "F_COMPTET_propre.xlsx",
    "docligne_ventes": "F_DOCLIGNE_propre.xlsx"
}

# --------------------------------------------------------------------
# Fichiers Excel à charger pour le schéma Achats
# --------------------------------------------------------------------
fichiers_achats = {
    "famille_achats":     "F_FAMILLE_propre.xlsx",
    "articles_achats":    "F_ARTICLE_propre.xlsx",
    "comptet_achats":     "F_COMPTET_propre.xlsx",
    "fournisseur_achats": "F_ARTFOURNISS_propre.xlsx",
    "docligne_achats":    "F_DOCLIGNE_propre.xlsx"
}

# --------------------------------------------------------------------
# Chargement des DataFrames depuis les fichiers Excel « propres »
# --------------------------------------------------------------------
tables_ventes = {}
tables_achats = {}

# Charger les fichiers Ventes
for nom_table, nom_fichier in fichiers_ventes.items():
    chemin_excel = chemin_dossier_xlsx / nom_fichier
    if chemin_excel.exists():
        df = pd.read_excel(chemin_excel)
        # Nettoyage léger : suppression des espaces dans les colonnes texte
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].astype(str).str.strip()
        tables_ventes[nom_table] = df
    else:
        print(f"Fichier introuvable (Ventes) : {chemin_excel}")

# Charger les fichiers Achats
for nom_table, nom_fichier in fichiers_achats.items():
    chemin_excel = chemin_dossier_xlsx / nom_fichier
    if chemin_excel.exists():
        df = pd.read_excel(chemin_excel)
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].astype(str).str.strip()
        tables_achats[nom_table] = df
    else:
        print(f"Fichier introuvable (Achats) : {chemin_excel}")

# --------------------------------------------------------------------
# Nettoyage des DataFrames Ventes
# --------------------------------------------------------------------
for nom_table, df in tables_ventes.items():
    df_nettoye = nettoyer_dataframe(df)
    tables_ventes[nom_table] = df_nettoye
    print(f"Colonnes nettoyées pour {nom_table} (Ventes) : {df_nettoye.columns.tolist()}")

# Nettoyage des DataFrames Achats
for nom_table, df in tables_achats.items():
    df_nettoye = nettoyer_dataframe(df)
    tables_achats[nom_table] = df_nettoye
    print(f"Colonnes nettoyées pour {nom_table} (Achats) : {df_nettoye.columns.tolist()}")

# --------------------------------------------------------------------
# Normalisation des clés AF_REFFOURNISS (schéma Achats)
# --------------------------------------------------------------------
if "fournisseur_achats" in tables_achats and "docligne_achats" in tables_achats:
    fourn_vals = (
        tables_achats["fournisseur_achats"]["AF_REFFOURNISS"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
    )
    df_docligne = tables_achats["docligne_achats"]
    mask = (
        df_docligne["AF_REFFOURNISS"].notna()
    ) & (
        df_docligne["AF_REFFOURNISS"].astype(str).str.strip().isin(fourn_vals)
    )
    tables_achats["docligne_achats"] = df_docligne.loc[mask]
    print(f"{len(tables_achats['docligne_achats'])} lignes de DOCLIGNE conservées après filtrage (Achats)")

# --------------------------------------------------------------------
# Ordre d’insertion et correspondances SQL
# --------------------------------------------------------------------
ordre_insertion_ventes = ["famille_ventes", "articles_ventes", "comptet_ventes", "docligne_ventes"]
noms_sql_ventes = {
    "famille_ventes":  "FAMILLE",
    "articles_ventes": "ARTICLES",
    "comptet_ventes":  "COMPTET",
    "docligne_ventes": "DOCLIGNE"
}

ordre_insertion_achats = ["famille_achats", "articles_achats", "comptet_achats", "fournisseur_achats", "docligne_achats"]
noms_sql_achats = {
    "famille_achats":     "FAMILLE",
    "articles_achats":    "ARTICLES",
    "comptet_achats":     "COMPTET",
    "fournisseur_achats": "ARTFOURNISS",
    "docligne_achats":    "DOCLIGNE"
}

# --------------------------------------------------------------------
# Insertion des données dans le schéma Ventes
# --------------------------------------------------------------------
for nom_logique in ordre_insertion_ventes:
    nom_table_sql = noms_sql_ventes[nom_logique]
    table_obj = {
        "famille_ventes":  famille_ventes,
        "articles_ventes": articles_ventes,
        "comptet_ventes":  comptet_ventes,
        "docligne_ventes": docligne_ventes
    }[nom_logique]
    df = tables_ventes.get(nom_logique, pd.DataFrame())

    # 1) Filtrer selon le modèle SQLAlchemy
    df_filtre = filtrer_colonnes(df, table_obj)

    # 2) Remplacer les "." isolés par NA
    df_filtre = df_filtre.replace({".": pd.NA})

    # 3) Traitements spécifiques
    if nom_logique == "comptet_ventes":
        df_filtre = df_filtre.dropna(subset=["CT_NUM"])
        df_filtre["CT_INTITULE"] = df_filtre["CT_INTITULE"].astype(str)
        df_filtre["CT_INTITULE"] = nettoyer_texte_objet(df_filtre[["CT_INTITULE"]])["CT_INTITULE"]

    elif nom_logique == "docligne_ventes":
        df_filtre = df_filtre.dropna(subset=["DL_NO", "AR_REF"])
        df_filtre["CT_NUM"] = (
            df_filtre["CT_NUM"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
        )
        df_filtre["DL_DESIGN"] = nettoyer_texte_objet(df_filtre[["DL_DESIGN"]])["DL_DESIGN"]
        df_filtre["CT_NUM"]       = df_filtre["CT_NUM"].astype(str)
        df_filtre["AC_REFCLIENT"] = df_filtre["AC_REFCLIENT"].astype(str)
        df_filtre["AR_REF"]       = df_filtre["AR_REF"].astype(str)
        df_filtre["DL_DESIGN"]    = df_filtre["DL_DESIGN"].astype(str)
        df_filtre["DL_QTE"]           = pd.to_numeric(df_filtre["DL_QTE"],           errors="coerce")
        df_filtre["DL_PRIXUNITAIRE"]  = pd.to_numeric(df_filtre["DL_PRIXUNITAIRE"],  errors="coerce")
        df_filtre["DL_MONTANTHT"]     = pd.to_numeric(df_filtre["DL_MONTANTHT"],     errors="coerce")
        ct_ok = set(tables_ventes["comptet_ventes"]["CT_NUM"].astype(str).str.strip())
        df_filtre = df_filtre[df_filtre["CT_NUM"].isin(ct_ok)]

    elif nom_logique == "articles_ventes":
        df_filtre = df_filtre.dropna(subset=["AR_REF"])
        df_filtre["AR_REF"] = df_filtre["AR_REF"].astype(str)

    elif nom_logique == "famille_ventes":
        df_filtre["FA_CENTRAL"]  = df_filtre["FA_CENTRAL"].astype(str)
        df_filtre["FA_INTITULE"] = df_filtre["FA_INTITULE"].astype(str)

    # 4) Copie pour éviter SettingWithCopyWarning
    df_filtre = df_filtre.copy()

    # Nettoyage final : transformer 'None', 'nan', 'NaN' en None
    for col in df_filtre.columns:
        df_filtre[col] = df_filtre[col].apply(
            lambda x: None if pd.isna(x) or str(x).strip() in ['None', 'nan', 'NaN'] else x
        )

    # 5) Insertion en base
    try:
        df_filtre.to_sql(
            nom_table_sql,
            moteur,
            schema="Ventes",
            if_exists="append",
            index=False
        )
        print(f"Inséré dans Ventes.{nom_table_sql} : {len(df_filtre)} lignes.")
    except SQLAlchemyError as err:
        print(f"Erreur insertion Ventes.{nom_table_sql} : {err}")

print("Chargement des données Ventes terminé.")

# --------------------------------------------------------------------
# Insertion des données dans le schéma Achats
# --------------------------------------------------------------------
for nom_logique in ordre_insertion_achats:
    nom_table_sql = noms_sql_achats[nom_logique]
    table_obj = {
        "famille_achats":     famille_achats,
        "articles_achats":    articles_achats,
        "comptet_achats":     comptet_achats,
        "fournisseur_achats": fournisseur_achats,
        "docligne_achats":    docligne_achats
    }[nom_logique]
    df = tables_achats.get(nom_logique, pd.DataFrame())

    df_filtre = filtrer_colonnes(df, table_obj)
    df_filtre = df_filtre.replace({".": pd.NA})

    if nom_logique == "comptet_achats":
        df_filtre = df_filtre.dropna(subset=["CT_NUM"])
        df_filtre["CT_NUM"]      = df_filtre["CT_NUM"].astype(str)
        df_filtre["CT_INTITULE"] = df_filtre["CT_INTITULE"].astype(str)
        df_filtre["CT_INTITULE"] = nettoyer_texte_objet(df_filtre[["CT_INTITULE"]])["CT_INTITULE"]

    elif nom_logique == "docligne_achats":
        df_filtre = df_filtre.dropna(subset=["DL_NO", "AF_REFFOURNISS", "AR_REF"])
        df_filtre["CT_NUM"] = (
            df_filtre["CT_NUM"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
        )
        df_filtre["DL_DESIGN"] = nettoyer_texte_objet(df_filtre[["DL_DESIGN"]])["DL_DESIGN"]
        df_filtre["CT_NUM"]       = df_filtre["CT_NUM"].astype(str)
        df_filtre["AC_REFCLIENT"] = df_filtre["AC_REFCLIENT"].astype(str)
        df_filtre["AR_REF"]       = df_filtre["AR_REF"].astype(str)
        df_filtre["DL_DESIGN"]    = df_filtre["DL_DESIGN"].astype(str)
        df_filtre["DL_QTE"]           = pd.to_numeric(df_filtre["DL_QTE"],           errors="coerce")
        df_filtre["DL_PRIXUNITAIRE"]  = pd.to_numeric(df_filtre["DL_PRIXUNITAIRE"],  errors="coerce")
        df_filtre["DL_MONTANTHT"]     = pd.to_numeric(df_filtre["DL_MONTANTHT"],     errors="coerce")
        df_filtre = df_filtre[df_filtre["AF_REFFOURNISS"].astype(str).str.strip().isin(
            tables_achats["fournisseur_achats"]["AF_REFFOURNISS"].dropna().astype(str).str.strip()
        )]

    elif nom_logique == "fournisseur_achats":
        df_filtre = df_filtre.dropna(subset=["AF_REFFOURNISS"])
        df_filtre = df_filtre.drop_duplicates(subset=["AF_REFFOURNISS"])
        df_filtre["AF_REFFOURNISS"] = df_filtre["AF_REFFOURNISS"].astype(str)

    elif nom_logique == "articles_achats":
        df_filtre = df_filtre.dropna(subset=["AR_REF"])
        df_filtre["AR_REF"] = df_filtre["AR_REF"].astype(str)

    elif nom_logique == "famille_achats":
        df_filtre["FA_CENTRAL"]  = df_filtre["FA_CENTRAL"].astype(str)
        df_filtre["FA_INTITULE"] = df_filtre["FA_INTITULE"].astype(str)

    df_filtre = df_filtre.copy()
    for col in df_filtre.columns:
        df_filtre[col] = df_filtre[col].apply(
            lambda x: None if pd.isna(x) or str(x).strip() in ['None', 'nan', 'NaN'] else x
        )

    try:
        df_filtre.to_sql(
            nom_table_sql,
            moteur,
            schema="Achats",
            if_exists="append",
            index=False
        )
        print(f"Inséré dans Achats.{nom_table_sql} : {len(df_filtre)} lignes.")
    except SQLAlchemyError as err:
        print(f"Erreur insertion Achats.{nom_table_sql} : {err}")

print("Chargement des données Achats terminé.")

# --------------------------------------------------------------------
# Fermeture propre du moteur SQLAlchemy
# --------------------------------------------------------------------
moteur.dispose()
print("Connexion fermée proprement.")
