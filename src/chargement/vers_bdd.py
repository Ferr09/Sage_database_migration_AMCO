#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import getpass
from pathlib import Path

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
    records = df.where(pd.notnull(df), None).to_dict(orient="records")
    full_name = f"{schema}.{table}"
    print(f"Insertion de {len(records)} lignes dans {full_name}…")
    res = supabase.from_(full_name).insert(records).execute()
    if res.error:
        raise RuntimeError(f"Erreur insertion {full_name} : {res.error.message}")
    print(f"→ {len(records)} lignes insérées dans {full_name}.")

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
        table = Path(fname).stem  # dim_client, dim_article, etc.
        upload_csv(supabase, ventes_dir / fname, schema="ventes", table=table)

    # ACHATS en modèle étoile
    achats_dir = dossier_datalake_processed / "achats"
    for fname in [
        "dim_fournisseur.csv",
        "dim_article.csv",
        "dim_temps.csv",
        "fact_achats.csv"
    ]:
        table = Path(fname).stem  # dim_fournisseur, dim_article, etc.
        upload_csv(supabase, achats_dir / fname, schema="achats", table=table)

    print("→ Chargement en modèle étoile terminé avec succès !")

if __name__ == "__main__":
    main()
