#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

# Charger la config
with open("config/supabase_config.json", encoding="utf-8") as f:
    cfg = json.load(f)

# Créer l’engine
engine = create_engine(
    f"postgresql+psycopg2://{cfg['db_user']}:{cfg['db_password']}"
    f"@{cfg['db_host']}:{cfg['db_port']}/{cfg['db_name']}"
)

base = Path("data_lake/processed")

# Fonction générique
def load_csv(schema, table):
    path = base / schema / f"{table}.csv"
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    df.to_sql(table, engine, schema=schema, if_exists="replace", index=False)
    print(f"Chargé {schema}.{table} ({len(df)} lignes)")

# Ventes
for tbl in ["Clients","FamillesArticles","Articles",
            "CommandesClients","Factures","LignesFacture"]:
    load_csv("ventes", tbl)

# Achats
for tbl in ["Fournisseurs","FamillesArticles","Articles",
            "ArticlesFournisseurs","CommandesFournisseurs",
            "FacturesFournisseurs","LignesFactureFournisseur"]:
    load_csv("achats", tbl)

engine.dispose()
