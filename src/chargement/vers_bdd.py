#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import getpass
from pathlib import Path
from postgrest import APIError

import pandas as pd
from supabase import create_client, Client

# Asegúrate de que tus importaciones de 'chemins' sean correctas
try:
    from src.outils.chemins import dossier_datalake_processed, dossier_config
except ImportError:
    # Ajusta esta ruta si es necesario para que coincida con tu estructura de proyecto
    projet_root = Path(__file__).resolve().parents[2]
    import sys
    sys.path.insert(0, str(projet_root))
    from src.outils.chemins import dossier_datalake_processed, dossier_config


def load_supabase_config() -> dict:
    """Charge la configuration Supabase depuis un fichier ou la demande à l'utilisateur."""
    cfg_file = dossier_config / "supabase_config.json"
    if cfg_file.exists():
        try:
            config = json.loads(cfg_file.read_text(encoding="utf-8"))
            if config.get("url") and config.get("key"):
                 use = input(f"{cfg_file.name} détecté. L’utiliser ? (o/n) : ").strip().lower()
                 if use.startswith("o"):
                    return config
        except json.JSONDecodeError:
            print(f"AVERTISSEMENT: Le fichier {cfg_file.name} est corrompu. Création d'un nouveau fichier.")
    
    print("Création de supabase_config.json")
    url = input("URL Supabase (ex : https://xyz.supabase.co) : ").strip()
    key = getpass.getpass("Service role key (API) : ").strip()
    conf = {"url": url, "key": key}
    dossier_config.mkdir(exist_ok=True)
    cfg_file.write_text(json.dumps(conf, indent=2), encoding="utf-8")
    print(f"{cfg_file.name} créé.")
    return conf

def connect_supabase(conf: dict) -> Client:
    """Crée et renvoie un client Supabase."""
    return create_client(conf["url"], conf["key"])

def upload_csv(supabase: Client, csv_path: Path, schema: str, table: str, processed_dir: Path):
    """Charge un fichier CSV vers une table Supabase, avec nettoyage et vérification des clés."""
    print(f"Traitement de {csv_path} pour la table {schema}.{table}...")
    if not csv_path.exists():
        print(f"  AVERTISSEMENT : Fichier non trouvé. Étape ignorée.")
        return

    df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig").where(pd.notnull, None)

    # --- Nettoyage et Vérifications Préalables ---
    
    # Règle spéciale pour dim_article: remplir les désignations vides
    if table == "dim_article" and 'designation' in df.columns:
        print("  Nettoyage de la colonne 'designation' pour dim_article...")
        df['designation'].fillna('NON SPÉCIFIÉ', inplace=True)
        df['designation'].replace('', 'NON SPÉCIFIÉ', inplace=True)

    # Règle spéciale pour les tables de faits: vérifier les clés étrangères
    if table.startswith("fact_"):
        foreign_keys_to_check = {
            "fact_ventes": [("dim_article_id", "dim_article.csv")],
            "fact_achats": [("dim_article_id", "dim_article.csv"), ("dim_fournisseur_id", "dim_fournisseur.csv")]
        }
        if table in foreign_keys_to_check:
            for fk_col, dim_fname in foreign_keys_to_check[table]:
                dim_path = processed_dir / dim_fname
                if dim_path.exists() and fk_col in df.columns:
                    print(f"  Vérification de la clé étrangère '{fk_col}' pour {table}...")
                    dim_df = pd.read_csv(dim_path, dtype=str)
                    if fk_col in dim_df.columns:
                        lignes_originales = len(df)
                        valid_ids = dim_df[fk_col].dropna().unique()
                        df = df[df[fk_col].isin(valid_ids)]
                        lignes_supprimees = lignes_originales - len(df)
                        if lignes_supprimees > 0:
                            print(f"  AVERTISSEMENT: {lignes_supprimees} lignes de {table} supprimées car leur '{fk_col}' n'existe pas dans la dimension correspondante.")
    
    # --- Définition des clés naturelles et colonnes à ignorer ---
    key_map = {
        "dim_client":           ("code_client", ["dim_client_id"]),
        "dim_famillesarticles": ("libelle_sous_famille", ["id_famille"]),
        "dim_article":          ("code_article", ["dim_article_id", "famille_id"]),
        "dim_fournisseur":      ("ct_numpayeur", ["dim_fournisseur_id", "famille_fournisseur"]),
        "dim_temps":            ("date_cle", ["dim_temps_id"]),
        "dim_date":             ("date_full", ["date_id"]),
        "dim_mode_expedition":  ("code_expedit", ["mode_id"]),
        "dim_famille_article":  ("fa_intitule", ["famille_id"]),
        "docligne":             ("dl_no", []),
        "fact_ventes":          ("dl_no", []),
        "fact_achats":          ("bon_de_commande", [])
    }
    
    if table not in key_map:
        raise RuntimeError(f"Configuration de table inconnue pour : {table}")

    natural_key, drop_ids = key_map[table]

    # --- Renommage des colonnes si nécessaire ---
    if table == "dim_fournisseur" and 'code_fournisseur' in df.columns:
        df = df.rename(columns={'code_fournisseur': 'ct_numpayeur'})
    if table == "dim_article" and 'id_famille' in df.columns:
        df = df.rename(columns={'id_famille': 'famille_id'})
    
    # --- Préparation finale avant l'envoi ---
    df = df.drop(columns=[c for c in drop_ids if c in df.columns], errors="ignore")
    df.dropna(subset=[natural_key], inplace=True)
    df = df.drop_duplicates(subset=[natural_key])

    if df.empty:
        print(f"  INFO : Aucune donnée valide à charger pour {table} après nettoyage.")
        return

    records = df.to_dict(orient='records')
    
    # --- Envoi à Supabase ---
    try:
        supabase.schema(schema).table(table).upsert(records, on_conflict=natural_key).execute()
        print(f"  → Succès : {len(records)} enregistrements traités pour {schema}.{table}.")
    except APIError as e:
        print(f"  ERREUR lors de l'upsert pour {schema}.{table}: {e}")
        # Affiche la première ligne problématique pour aider au débogage
        if records:
            print("  Exemple de ligne qui a pu causer l'erreur :")
            print(f"  {records[0]}")
        raise e

def main():
    """Fonction principale pour orchestrer le chargement des données."""
    conf = load_supabase_config()
    supabase = connect_supabase(conf)

    # VENTES
    print("\n--- DÉBUT DU CHARGEMENT DES VENTES ---")
    dossier_ventes = dossier_datalake_processed / "ventes"
    fichiers_ventes = [
        "dim_client.csv", "dim_famillesarticles.csv", "dim_article.csv",
        "dim_temps.csv", "fact_ventes.csv"
    ]
    # L'ordre est important : d'abord les dimensions, puis les faits.
    for fname in sorted(fichiers_ventes, key=lambda x: x.startswith('fact_')):
        upload_csv(supabase, dossier_ventes / fname, "ventes", Path(fname).stem, dossier_ventes)

    # ACHATS
    print("\n--- DÉBUT DU CHARGEMENT DES ACHATS ---")
    dossier_achats = dossier_datalake_processed / "achats"
    fichiers_achats = [
        "dim_fournisseur.csv", "dim_famille_article.csv", "dim_article.csv",
        "dim_mode_expedition.csv", "docligne.csv", "dim_date.csv",
        "fact_achats.csv"
    ]
    # L'ordre est important : d'abord les dimensions, puis les faits.
    for fname in sorted(fichiers_achats, key=lambda x: x.startswith('fact_')):
        upload_csv(supabase, dossier_achats / fname, "achats", Path(fname).stem, dossier_achats)

    print("\n→ Chargement en modèle étoile terminé avec succès !")

if __name__ == "__main__":
    main()