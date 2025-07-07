#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import getpass
from pathlib import Path
from postgrest import APIError

import pandas as pd
from supabase import create_client, Client

from src.outils.chemins import dossier_datalake_processed, dossier_config

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

def connect_supabase(conf: dict) -> Client:
    return create_client(conf["url"], conf["key"])

def upload_csv(supabase: Client, csv_path: Path, schema: str, table: str):
    # 1) Chargement du CSV
    df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig").where(pd.notnull, None)

    # 2) Dimensions
    if table.startswith("dim_"):
        key_map = {
            "dim_client":           ("code_client",           ["dim_client_id"]),
            "dim_famillesarticles": ("code_famille",          ["id_famille"]),
            "dim_article":          ("code_article",          ["dim_article_id"]),
            "dim_fournisseur":      ("code_fournisseur",      ["dim_fournisseur_id"]),
            "dim_temps":            ("date_cle",              ["dim_temps_id"]),
        }
        if table not in key_map:
            raise RuntimeError(f"Dimension inconnue : {table}")

        natural_key, drop_ids = key_map[table]
        # Retirer les colonnes _id pour laisser la séquence
        df = df.drop(columns=[c for c in drop_ids if c in df.columns], errors="ignore")

        try:
            # Cas spécial : dim_famillesarticles utilise clé composée
            if table == "dim_famillesarticles":
                df = df.drop_duplicates(subset=["libelle_sous_famille"])
                records = df.to_dict(orient="records")

                print(f"Upserting {len(records)} lignes dans {schema}.{table} (clé=libelle_sous_famille)…")
                supabase.schema(schema) \
                        .table(table) \
                        .upsert(records, on_conflict="libelle_sous_famille") \
                        .execute()
                print(f"→ {len(records)} lignes upsertées dans {schema}.{table}.")
                return

            # Clé simple pour les autres dimensions
            df = df.drop_duplicates(subset=[natural_key])
            records = df.to_dict(orient="records")

            print(f"Upserting {len(records)} lignes dans {schema}.{table} (clé={natural_key})…")
            supabase.schema(schema) \
                     .table(table) \
                     .upsert(records, on_conflict=natural_key) \
                     .execute()
            print(f"→ {len(records)} lignes upsertées dans {schema}.{table}.")
            return

        except APIError as e:
            raise RuntimeError(f"Erreur upsert {schema}.{table} : {e}") from e

    # 3) Tables de faits – upsert sur dl_no (champ UNIQUE dans le DDL)
    if table.startswith("fact_"):
        natural_key = "dl_no"
        df = df.drop_duplicates(subset=[natural_key])
        records = df.to_dict(orient="records")

        try:
            print(f"Upserting {len(records)} lignes dans {schema}.{table} (clé={natural_key})…")
            supabase.schema(schema) \
                     .table(table) \
                     .upsert(records, on_conflict=natural_key) \
                     .execute()
            print(f"→ {len(records)} lignes upsertées dans {schema}.{table}.")
            return

        except APIError as e:
            raise RuntimeError(f"Erreur upsert {schema}.{table} : {e}") from e

    # 4) Cas inattendu
    raise RuntimeError(f"Table non gérée : {schema}.{table}")

def main():
    conf = load_supabase_config()
    supabase = connect_supabase(conf)

    # VENTES
    ventes_dir = dossier_datalake_processed / "ventes"
    for fname in [
        "dim_client.csv",
        "dim_famillesarticles.csv",
        "dim_article.csv",
        "dim_temps.csv",
        "fact_ventes.csv"
    ]:
        table = Path(fname).stem
        upload_csv(supabase, ventes_dir / fname, schema="ventes", table=table)

    # ACHATS
    achats_dir = dossier_datalake_processed / "achats"
    for fname in [
        "dim_fournisseur.csv",
        "dim_famillesarticles.csv",
        "dim_article.csv",
        "dim_temps.csv",
        "fact_achats.csv"
    ]:
        table = Path(fname).stem
        # upload_csv(supabase, achats_dir / fname, schema="achats", table=table)

    print("→ Chargement en modèle étoile terminé avec succès !")

if __name__ == "__main__":
    main()
