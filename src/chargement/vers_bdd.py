#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import getpass
from pathlib import Path
from postgrest import APIError

import pandas as pd
from supabase import create_client, Client

from src.outils.chemins import dossier_datalake_processed, dossier_config

# --------------------------------------------------------------------
# 1) Charger ou créer supabase_config.json
# --------------------------------------------------------------------

def load_supabase_config() -> dict:
    cfg_file = dossier_config / "supabase_config.json"
    if cfg_file.exists():
        use = input(f"{cfg_file.name} détecté. L’utiliser ? (o/n) : ").strip().lower()
        if use.startswith("o"):
            return json.loads(cfg_file.read_text(encoding="utf-8"))
    print("Création de supabase_config.json")
    url = input("URL Supabase (ex : https://xyz.supabase.co) : ").strip()
    key = getpass.getpass("Service role key (API) : ").strip()
    conf = {"url": url, "key": key}
    dossier_config.mkdir(exist_ok=True)
    cfg_file.write_text(json.dumps(conf, indent=2), encoding="utf-8")
    print(f"{cfg_file.name} créé.")
    return conf

# --------------------------------------------------------------------
# 2) Connexion Supabase
# --------------------------------------------------------------------

def connect_supabase(conf: dict) -> Client:
    return create_client(conf["url"], conf["key"])

# --------------------------------------------------------------------
# 3) Fonction générique d’upload d’un CSV vers un schema.table
# --------------------------------------------------------------------

def upload_csv(supabase: Client, csv_path: Path, schema: str, table: str):
    df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")

    # Déterminer colonne on_conflict
    if table == 'dim_article':
        conflict_col = 'code_article'
        # Ne pas envoyer dim_article_id pour éviter conflit PK
        df = df.drop(columns=[col for col in df.columns if col.endswith('_id')], errors='ignore')
    elif table == 'dim_fournisseur':
        conflict_col = 'code_fournisseur'
        df = df.drop(columns=[col for col in df.columns if col.endswith('_id')], errors='ignore')
    else:
        conflict_col = f"{table}_id"

    # Supprimer doublons dans le batch
    if conflict_col in df.columns:
        df = df.drop_duplicates(subset=[conflict_col])

    records = df.where(pd.notnull(df), None).to_dict(orient="records")
    print(f"Upserting {len(records)} lignes dans {schema}.{table}…")

    try:
        supabase.schema(schema).table(table).upsert(records, on_conflict=conflict_col).execute()
    except APIError as e:
        raise RuntimeError(f"Erreur upsert {schema}.{table} : {e}") from e

    print(f"→ {len(records)} lignes upsertées dans {schema}.{table}.")

# --------------------------------------------------------------------
# 4) Main
# --------------------------------------------------------------------

def main():
    conf = load_supabase_config()
    supabase = connect_supabase(conf)

    # VENTES en modèle étoile
    ventes_dir = dossier_datalake_processed / "ventes"
    for fname in [
        "dim_client.csv",
        "dim_article.csv",
        "dim_temps.csv",
        "fact_ventes.csv"
    ]:
        table = Path(fname).stem
        upload_csv(supabase, ventes_dir / fname, schema="ventes", table=table)

    # ACHATS en modèle étoile
    achats_dir = dossier_datalake_processed / "achats"
    for fname in [
        "dim_fournisseur.csv",
        "dim_article.csv",
        "dim_temps.csv",
        "fact_achats.csv"
    ]:
        table = Path(fname).stem
        upload_csv(supabase, achats_dir / fname, schema="achats", table=table)

    print("→ Chargement en modèle étoile terminé avec succès !")

if __name__ == "__main__":
    main()
