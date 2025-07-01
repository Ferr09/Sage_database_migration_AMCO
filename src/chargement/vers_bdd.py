#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
vers_bdd.py

Script unique pour :
- Charger la configuration Supabase/PostgreSQL
- Lire les CSV "tabla_generale_ventes.csv" et "tabla_generale_achats.csv"
- Décomposer chaque table générale en 3FN (schémas 'ventes' et 'achats')
- Insérer directement les tables normalisées dans la base Supabase
"""

import os
import json
import getpass
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

# --------------------------------------------------------------------
# Import des chemins
# --------------------------------------------------------------------
try:
    from src.outils.chemins import dossier_datalake_processed, dossier_config
except ImportError:
    projet_root = Path(__file__).resolve().parents[2]
    import sys
    sys.path.insert(0, str(projet_root / "src"))
    from outils.chemins import dossier_datalake_processed, dossier_config

# --------------------------------------------------------------------
# Chargement ou création de la configuration
# --------------------------------------------------------------------
import json
import getpass
from pathlib import Path
from sqlalchemy import create_engine

# dossier_config défini dans src/outils/chemins
from src.outils.chemins import dossier_config  

def charger_config() -> dict:
    cfg_file = dossier_config / "supabase_config.json"
    if cfg_file.exists():
        if input(f"{cfg_file.name} détecté. L’utiliser ? (o/n) : ").strip().lower().startswith("o"):
            return json.loads(cfg_file.read_text(encoding="utf-8"))
    # Sinon, on la crée
    print("Création de supabase_config.json :")
    h   = input("Hôte (ex : aws-0-eu-west-3.pooler.supabase.com) : ").strip()
    p   = input("Port (par défaut 6543) : ").strip() or "6543"
    db  = input("Nom de la base (ex : postgres) : ").strip()
    u   = input("Utilisateur (ex : postgres.hnaavbczhsxjcmmdnhrh) : ").strip()
    pwd = getpass.getpass("Mot de passe : ").strip()
    conf = {
        "db_host":     h,
        "db_port":     p,
        "db_name":     db,
        "db_user":     u,
        "db_password": pwd,
        "sslmode":     "require"
    }
    dossier_config.mkdir(exist_ok=True)
    cfg_file.write_text(json.dumps(conf, indent=4), encoding="utf-8")
    print(f"{cfg_file.name} créé.")
    return conf

def connect_engine(conf: dict):
    """
    Crée un engine SQLAlchemy pointant sur le pooler Supabase (port 6543)
    avec SSL mode obligatoirement à require.
    """
    # Construction de l'URL sans le préfixe "db."
    url = (
        f"postgresql://{conf['db_user']}:{conf['db_password']}"
        f"@{conf['db_host']}:{conf['db_port']}/{conf['db_name']}"
        f"?sslmode={conf.get('sslmode','require')}"
    )
    return create_engine(url)

# --------------------------------------------------------------------
# Génération 3FN pour le schéma 'ventes'
# --------------------------------------------------------------------
def generer_3fn_ventes(df: pd.DataFrame, engine):
    # 1. Clients
    df_clients = df[[
        "Code client","Raison sociale","Famille du client",
        "responsable du dossier","représentant"
    ]].drop_duplicates().reset_index(drop=True)
    df_clients.insert(0, "id_client", df_clients.index + 1)
    df_clients.columns = [
        "id_client","code_client","raison_sociale",
        "famille_client","responsable_dossier","representant"
    ]
    df_clients.to_sql("clients", engine, schema="ventes", if_exists="append", index=False)

    # 2. FamillesArticles
    df_fam = df[[
        "famille article libellé","sous-famille article libellé"
    ]].drop_duplicates().reset_index(drop=True)
    df_fam.insert(0, "id_famille", df_fam.index + 1)
    df_fam["code_famille"] = df_fam["famille article libellé"]
    df_fam = df_fam[[
        "id_famille","code_famille",
        "famille article libellé","sous-famille article libellé"
    ]]
    df_fam.columns = [
        "id_famille","code_famille",
        "libelle_famille","libelle_sous_famille"
    ]
    df_fam.to_sql("famillesarticles", engine, schema="ventes", if_exists="append", index=False)

    # Préparer les dictionnaires pour les clés étrangères
    map_client = dict(zip(df_clients["code_client"], df_clients["id_client"]))
    map_fam    = dict(zip(df_fam["code_famille"], df_fam["id_famille"]))

    # 3. Articles
    df_art = df[[
        "code article","Désignation","Numéro de plan",
        "Ref art client","famille article libellé"
    ]].drop_duplicates().reset_index(drop=True)
    df_art.insert(0, "id_article", df_art.index + 1)
    df_art["id_famille_fk"] = df_art["famille article libellé"].map(map_fam).astype("Int64")
    df_art = df_art[[
        "id_article","code article","Désignation",
        "Numéro de plan","Ref art client","id_famille_fk"
    ]]
    df_art.columns = [
        "id_article","code_article","designation",
        "numero_plan","ref_article_client","id_famille_fk"
    ]
    df_art.to_sql("articles", engine, schema="ventes", if_exists="append", index=False)

    # 4. CommandesClients
    df_cmd = df[[
        "N° Cde","Ref cde client",
        "Date demandée client","Date accusée AMCO","Code client"
    ]].drop_duplicates().reset_index(drop=True)
    df_cmd.insert(0, "id_commande_client", df_cmd.index + 1)
    df_cmd["id_client_fk"] = df_cmd["Code client"].map(map_client).astype("Int64")
    df_cmd = df_cmd[[
        "id_commande_client","N° Cde","Ref cde client",
        "Date demandée client","Date accusée AMCO","id_client_fk"
    ]]
    df_cmd.columns = [
        "id_commande_client","num_commande","ref_commande_client",
        "date_demandee","date_accusee","id_client_fk"
    ]
    df_cmd.to_sql("commandesclients", engine, schema="ventes", if_exists="append", index=False)

    # 5. Factures
    df_fac = df[[
        "N° facture","date facture","N° BL","Date BL",
        "Code client","N° Cde"
    ]].drop_duplicates().reset_index(drop=True)
    df_fac.insert(0, "id_facture", df_fac.index + 1)
    df_fac["condition_livraison"] = None
    df_fac["id_client_fk"] = df_fac["Code client"].map(map_client).astype("Int64")
    map_cmd = dict(zip(df_cmd["num_commande"], df_cmd["id_commande_client"]))
    df_fac["id_commande_client_fk"] = df_fac["N° Cde"].map(map_cmd).astype("Int64")
    df_fac = df_fac[[
        "id_facture","N° facture","date facture","N° BL","Date BL",
        "condition_livraison","id_client_fk","id_commande_client_fk"
    ]]
    df_fac.columns = [
        "id_facture","num_facture","date_facture","num_bl","date_bl",
        "condition_livraison","id_client_fk","id_commande_client_fk"
    ]
    df_fac.to_sql("factures", engine, schema="ventes", if_exists="append", index=False)

    # 6. LignesFacture
    df_lf = df[[
        "Qté fact","Prix Unitaire","Tot HT","N° facture","code article"
    ]].drop_duplicates().reset_index(drop=True)
    df_lf.insert(0, "id_ligne_facture", df_lf.index + 1)
    map_fac = dict(zip(df_fac["num_facture"], df_fac["id_facture"]))
    map_art = dict(zip(df_art["code_article"], df_art["id_article"]))
    df_lf["id_facture_fk"] = df_lf["N° facture"].map(map_fac).astype("Int64")
    df_lf["id_article_fk"] = df_lf["code article"].map(map_art).astype("Int64")
    df_lf = df_lf[[
        "id_ligne_facture","Qté fact","Prix Unitaire",
        "id_facture_fk","id_article_fk"
    ]]
    df_lf.columns = [
        "id_ligne_facture","qte_vendue","prix_unitaire_vente",
        "id_facture_fk","id_article_fk"
    ]
    df_lf.to_sql("lignesfacture", engine, schema="ventes", if_exists="append", index=False)

# --------------------------------------------------------------------
# Génération 3FN pour le schéma 'achats'
# --------------------------------------------------------------------
def generer_3fn_achats(df: pd.DataFrame, engine):
    # 1. Fournisseurs
    df_fr = df[[
        "Ref cde fournisseur","Raison sociale",
        "famille client","responsable du dossier","représentant"
    ]].drop_duplicates().reset_index(drop=True)
    df_fr.insert(0, "id_fournisseur", df_fr.index + 1)
    df_fr.columns = [
        "id_fournisseur","code_fournisseur","raison_sociale",
        "famille_fournisseur","responsable_dossier","representant"
    ]
    df_fr.to_sql("fournisseurs", engine, schema="achats", if_exists="append", index=False)

    # 2. FamillesArticles
    df_fa = df[[
        "famille article libellé","sous-famille article libellé"
    ]].drop_duplicates().reset_index(drop=True)
    df_fa.insert(0, "id_famille", df_fa.index + 1)
    df_fa["code_famille"] = df_fa["famille article libellé"]
    df_fa = df_fa[[
        "id_famille","code_famille",
        "famille article libellé","sous-famille article libellé"
    ]]
    df_fa.columns = [
        "id_famille","code_famille",
        "libelle_famille","libelle_sous_famille"
    ]
    df_fa.to_sql("famillesarticles", engine, schema="achats", if_exists="append", index=False)

    map_fourn = dict(zip(df_fr["code_fournisseur"], df_fr["id_fournisseur"]))
    map_fa    = dict(zip(df_fa["code_famille"], df_fa["id_famille"]))

    # 3. Articles
    df_art = df[[
        "code article","Désignation","Numéro de plan"
    ]].drop_duplicates().reset_index(drop=True)
    df_art.insert(0, "id_article", df_art.index + 1)
    df_art["id_famille_fk"] = df_art["code article"].map(map_fa).astype("Int64")
    df_art = df_art[[
        "id_article","code article","Désignation","Numéro de plan","id_famille_fk"
    ]]
    df_art.columns = [
        "id_article","code_article","designation","numero_plan","id_famille_fk"
    ]
    df_art.to_sql("articles", engine, schema="achats", if_exists="append", index=False)

    # 4. ArticlesFournisseurs
    df_af = df[[
        "code article","Ref cde fournisseur"
    ]].drop_duplicates().reset_index(drop=True)
    df_af.insert(0, "id_art_fourn", df_af.index + 1)
    df_af["id_article_fk"]     = df_af["code article"].map(dict(zip(df_art["code_article"], df_art["id_article"]))).astype("Int64")
    df_af["id_fournisseur_fk"]  = df_af["Ref cde fournisseur"].map(map_fourn).astype("Int64")
    df_af = df_af[[
        "id_art_fourn","Ref cde fournisseur","id_article_fk","id_fournisseur_fk"
    ]]
    df_af.columns = [
        "id_art_fourn","ref_article_fournisseur","id_article_fk","id_fournisseur_fk"
    ]
    df_af.to_sql("articlesfournisseurs", engine, schema="achats", if_exists="append", index=False)

    # 5. CommandesFournisseurs
    df_cf = df[[
        "N° Cde","Date BL","Ref cde fournisseur"
    ]].drop_duplicates().reset_index(drop=True)
    df_cf.insert(0, "id_commande_fourn", df_cf.index + 1)
    df_cf["id_fournisseur_fk"] = df_cf["Ref cde fournisseur"].map(map_fourn).astype("Int64")
    df_cf.columns = ["id_commande_fourn","num_commande","date_commande","id_fournisseur_fk"]
    df_cf.to_sql("commandesfournisseurs", engine, schema="achats", if_exists="append", index=False)

    # 6. FacturesFournisseurs
    df_ff = df[[
        "N° facture","date facture","N° BL","Ref cde fournisseur","N° Cde"
    ]].drop_duplicates().reset_index(drop=True)
    df_ff.insert(0, "id_facture_fourn", df_ff.index + 1)
    df_ff["id_fournisseur_fk"]     = df_ff["Ref cde fournisseur"].map(map_fourn).astype("Int64")
    map_cmdf = dict(zip(df_cf["num_commande"], df_cf["id_commande_fourn"]))
    df_ff["id_commande_fourn_fk"]   = df_ff["N° Cde"].map(map_cmdf).astype("Int64")
    df_ff = df_ff[[
        "id_facture_fourn","N° facture","date facture","N° BL",
        "id_fournisseur_fk","id_commande_fourn_fk"
    ]]
    df_ff.columns = [
        "id_facture_fourn","num_facture","date_facture","num_bl",
        "id_fournisseur_fk","id_commande_fourn_fk"
    ]
    df_ff.to_sql("facturesfournisseurs", engine, schema="achats", if_exists="append", index=False)

    # 7. LignesFactureFournisseur
    df_lf = df[[
        "Qté fact","Prix Unitaire","N° facture","code article"
    ]].drop_duplicates().reset_index(drop=True)
    df_lf.insert(0, "id_ligne_facture_fourn", df_lf.index + 1)
    map_ff = dict(zip(df_ff["num_facture"], df_ff["id_facture_fourn"]))
    df_lf["qte_achetee"]             = pd.to_numeric(df_lf["Qté fact"], errors="coerce")
    df_lf["prix_unitaire_achat"]     = pd.to_numeric(df_lf["Prix Unitaire"], errors="coerce")
    df_lf["id_facture_fourn_fk"]     = df_lf["N° facture"].map(map_ff).astype("Int64")
    df_lf["id_article_fk"]           = df_lf["code article"].map(dict(zip(df_art["code_article"], df_art["id_article"]))).astype("Int64")
    df_lf = df_lf[[
        "id_ligne_facture_fourn","qte_achetee","prix_unitaire_achat",
        "id_facture_fourn_fk","id_article_fk"
    ]]
    df_lf.to_sql("lignesfacturefournisseur", engine, schema="achats", if_exists="append", index=False)

# --------------------------------------------------------------------
# Point d’entrée
# --------------------------------------------------------------------
def main():
    conf   = charger_config()
    engine = connect_engine(conf)

    # Chargement des CSV généraux
    df_ventes = pd.read_csv(dossier_datalake_processed/"tabla_generale_ventes.csv", dtype=str, encoding="utf-8-sig")
    print("→ Génération 3FN pour 'ventes'…")
    generer_3fn_ventes(df_ventes, engine)

    df_achats = pd.read_csv(dossier_datalake_processed/"tabla_generale_achats.csv", dtype=str, encoding="utf-8-sig")
    print("→ Génération 3FN pour 'achats'…")
    generer_3fn_achats(df_achats, engine)

    print("→ Import terminé.")

if __name__ == "__main__":
    main()

