#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import getpass
from pathlib import Path
from postgrest import APIError

import pandas as pd
from supabase import create_client, Client

# (Imports y funciones load_supabase_config, connect_supabase sin cambios)
try:
    from src.outils.chemins import dossier_datalake_processed, dossier_config
except ImportError:
    projet_root = Path(__file__).resolve().parents[2]
    import sys
    sys.path.insert(0, str(projet_root))
    from src.outils.chemins import dossier_datalake_processed, dossier_config

def load_supabase_config() -> dict:
    cfg_file = dossier_config / "supabase_config.json"
    if cfg_file.exists():
        try:
            config = json.loads(cfg_file.read_text(encoding="utf-8"))
            if config.get("url") and config.get("key"):
                 use = input(f"{cfg_file.name} détecté. L’utiliser ? (o/n) : ").strip().lower()
                 if use.startswith("o"):
                    return config
        except json.JSONDecodeError:
            print(f"AVERTISSEMENT: Le fichier {cfg_file.name} est corrompu.")
    print("Création de supabase_config.json")
    url = input("URL Supabase (ex : https://xyz.supabase.co) : ").strip()
    key = getpass.getpass("Service role key (API) : ").strip()
    conf = {"url": url, "key": key}
    dossier_config.mkdir(exist_ok=True)
    cfg_file.write_text(json.dumps(conf, indent=2), encoding="utf-8")
    print(f"{cfg_file.name} créé.")
    return conf

def connect_supabase(conf: dict) -> Client:
    return create_client(conf["url"], conf["key"])

def upload_csv(supabase: Client, csv_path: Path, schema: str, table: str):
    print(f"Traitement de {csv_path} vers la table {schema}.{table}...")
    if not csv_path.exists():
        print(f"  AVERTISSEMENT : Fichier non trouvé. Étape ignorée.")
        return

    df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig").replace('', pd.NA).where(pd.notnull, None)
    if table == "dim_article" and 'designation' in df.columns:
        df['designation'] = df['designation'].fillna('NON SPÉCIFIÉ')

    key_map = {
        "dim_client":           ("code_client", ["dim_client_id"]),
        "dim_famillesarticles": ("code_famille", ["id_famille"]),
        "dim_article":          ("code_article", ["dim_article_id"]),
        "dim_fournisseur":      ("code_fournisseur", ["dim_fournisseur_id"]),
        "dim_temps":            ("date_cle", ["dim_temps_id"]),
        "dim_date":             ("date_full", ["date_id"]),
        "dim_famille_article":  ("code_famille", ["famille_id"]),
        "fact_ventes":          ("dl_no", []),
        "fact_achats":          ("bon_de_commande", []) 
    }
    
    if table not in key_map:
        print(f"  AVERTISSEMENT: Table '{table}' non configurée. Ignorée.")
        return

    natural_key, drop_ids = key_map[table]
    
    if table == 'dim_article' and schema == 'achats':
        key_map['dim_article'] = ('ar_ref', ['article_id'])
        if 'ar_ref' in df.columns:
            df = df.rename(columns={'ar_ref': 'code_article'})
            natural_key = 'code_article'
    
    df_to_upload = df.drop(columns=[c for c in drop_ids if c in df.columns], errors="ignore")
    df_to_upload.dropna(subset=[natural_key], inplace=True)
    df_to_upload = df_to_upload.drop_duplicates(subset=[natural_key])

    if df_to_upload.empty:
        print(f"  INFO : Aucune donnée à charger pour {table}.")
        return

    records = df_to_upload.to_dict(orient='records')
    
    try:
        supabase.schema(schema).table(table).upsert(records, on_conflict=natural_key).execute()
        print(f"  → Succès : {len(records)} enregistrements traités pour {schema}.{table}.")
    except APIError as e:
        print(f"  ERREUR lors de l'upsert pour {schema}.{table}: {e}")
        if records:
            print("  Exemple de ligne qui a pu causer l'erreur :")
            print(f"  {records[0]}")
        raise e

def main():
    """Fonction principale pour orchestrer le chargement des données."""
    conf = load_supabase_config()
    supabase = connect_supabase(conf)

    # --- CORRECTION : ORDRE DE CHARGEMENT EXPLICITE ET LOGIQUE ---
    
    # VENTES
    print("\n--- DÉBUT DU CHARGEMENT DU SCHÉMA 'VENTES' ---")
    dossier_ventes = dossier_datalake_processed / "ventes"
    # L'ordre est crucial : les dimensions dont d'autres dépendent vont en premier.
    ordre_chargement_ventes = [
        "dim_client.csv",
        "dim_temps.csv",
        "dim_famillesarticles.csv", # Doit être chargé AVANT dim_article
        "dim_article.csv",          # Dépend de dim_famillesarticles
        "fact_ventes.csv"           # En dernier, dépend de tout le reste
    ]
    for fname in ordre_chargement_ventes:
        upload_csv(supabase, dossier_ventes / fname, "ventes", Path(fname).stem)

    # ACHATS
    print("\n--- DÉBUT DU CHARGEMENT DU SCHÉMA 'ACHATS' ---")
    dossier_achats = dossier_datalake_processed / "achats"
    # Même logique pour les achats
    ordre_chargement_achats = [
        "dim_fournisseur.csv",
        "dim_date.csv",
        "dim_famille_article.csv", # Doit être chargé AVANT dim_article
        "dim_article.csv",         # Dépend de dim_famille_article
        "fact_achats.csv"          # En dernier
    ]
    # On ignore les fichiers non listés pour éviter les erreurs
    fichiers_existants_achats = [f for f in ordre_chargement_achats if (dossier_achats / f).exists()]
    for fname in fichiers_existants_achats:
        upload_csv(supabase, dossier_achats / fname, "achats", Path(fname).stem)

    print("\n→ Chargement en modèle étoile terminé avec succès !")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nL'OPÉRATION A ÉCHOUÉ. Erreur non capturée : {e}")