#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
structuration_3FN.py

Création des tables 3FN pour les schémas 'ventes' et 'achats' en évitant
les merges gourmands en mémoire : utilisation de dict+map pour les FK.
"""

import os
import sys
from pathlib import Path
import pandas as pd

try:
    from src.outils.chemins import dossier_datalake_processed
except ImportError:
    # si exécuté hors de src/
    projet_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(projet_root / "src"))
    from outils.chemins import dossier_datalake_processed

# -------------------------------------------------------------------
# Chargement des CSV généraux (déjà générés)
# -------------------------------------------------------------------
ventes = pd.read_csv(
    dossier_datalake_processed / "tabla_generale_ventes.csv",
    dtype=str, encoding="utf-8-sig"
)
achats = pd.read_csv(
    dossier_datalake_processed / "tabla_generale_achats.csv",
    dtype=str, encoding="utf-8-sig"
)

# -------------------------------------------------------------------
# Création des dossiers de sortie
# -------------------------------------------------------------------
os.makedirs(dossier_datalake_processed / "ventes", exist_ok=True)
os.makedirs(dossier_datalake_processed / "achats", exist_ok=True)

# -------------------------------------------------------------------
# 1) SCHÉMA 'ventes' (3FN)
# -------------------------------------------------------------------

# 1.1 – Clients
df_clients = ventes[[
    "Code client","Raison sociale","Famille du client",
    "responsable du dossier","représentant"
]].drop_duplicates().reset_index(drop=True)
df_clients.insert(0, "id_client", df_clients.index + 1)
df_clients.columns = [
    "id_client","code_client","raison_sociale","famille_client",
    "responsable_dossier","representant"
]
df_clients.to_csv(dossier_datalake_processed / "ventes" / "Clients.csv", index=False)

# 1.2 – FamillesArticles
df_fam = ventes[[
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
df_fam.to_csv(dossier_datalake_processed / "ventes" / "FamillesArticles.csv", index=False)

# 1.3 – Articles (sans "Ref art client")
df_art = ventes[[
    "code article","Désignation",
    "Numéro de plan","famille article libellé"
]].drop_duplicates().reset_index(drop=True)
df_art.insert(0, "id_article", df_art.index + 1)
# rattacher id_famille via df_fam
df_art = df_art.merge(
    df_fam[["code_famille","id_famille"]],
    left_on="famille article libellé", right_on="code_famille", how="left"
)
df_art = df_art[[
    "id_article","code article","Désignation",
    "Numéro de plan","id_famille"
]]
df_art.columns = [
    "id_article","code_article","designation",
    "numero_plan","id_famille_fk"
]
df_art.to_csv(dossier_datalake_processed / "ventes" / "Articles.csv", index=False)

# 1.4 – CommandesClients
df_cmd = ventes[[
    "N° Cde","Ref cde client",
    "Date demandée client","Date accusée AMCO","Code client"
]].drop_duplicates().reset_index(drop=True)
df_cmd.insert(0, "id_commande_client", df_cmd.index + 1)
df_cmd = df_cmd.merge(
    df_clients[["code_client","id_client"]],
    left_on="Code client", right_on="code_client", how="left"
)
df_cmd = df_cmd[[
    "id_commande_client","N° Cde","Ref cde client",
    "Date demandée client","Date accusée AMCO","id_client"
]]
df_cmd.columns = [
    "id_commande_client","num_commande","ref_commande_client",
    "date_demandee","date_accusee","id_client_fk"
]
df_cmd.to_csv(dossier_datalake_processed / "ventes" / "CommandesClients.csv", index=False)

# Préparer les maps pour clients et commandes
map_client   = dict(zip(df_clients["code_client"], df_clients["id_client"]))
map_commande = dict(zip(df_cmd["num_commande"], df_cmd["id_commande_client"]))

# 1.5 – Factures
df_fac = ventes[[
    "N° facture","date facture","N° BL","Date BL",
    "Code client","N° Cde"
]].drop_duplicates().reset_index(drop=True)
df_fac.insert(0, "id_facture", df_fac.index + 1)
df_fac["condition_livraison"] = pd.NA
df_fac["id_client_fk"] = df_fac["Code client"].map(map_client).astype("Int64")
df_fac["id_commande_client_fk"] = df_fac["N° Cde"].map(map_commande).astype("Int64")
df_fac = df_fac[[
    "id_facture","N° facture","date facture","N° BL","Date BL",
    "condition_livraison","id_client_fk","id_commande_client_fk"
]]
df_fac.columns = [
    "id_facture","num_facture","date_facture",
    "num_bl","date_bl","condition_livraison",
    "id_client_fk","id_commande_client_fk"
]
df_fac.to_csv(dossier_datalake_processed / "ventes" / "Factures.csv", index=False)

# 1.6 – LignesFacture
map_facture = dict(zip(df_fac["num_facture"], df_fac["id_facture"]))
map_article = dict(zip(df_art["code_article"], df_art["id_article"]))

df_lf = ventes[[
    "Qté fact","Prix Unitaire","Tot HT","N° facture","code article"
]].drop_duplicates().reset_index(drop=True)
df_lf.insert(0, "id_ligne_facture", df_lf.index + 1)
df_lf["id_facture_fk"] = df_lf["N° facture"].map(map_facture).astype("Int64")
df_lf["id_article_fk"] = df_lf["code article"].map(map_article).astype("Int64")
df_lf = df_lf[[
    "id_ligne_facture","Qté fact","Prix Unitaire",
    "id_facture_fk","id_article_fk"
]]
df_lf.columns = [
    "id_ligne_facture","qte_vendue","prix_unitaire_vente",
    "id_facture_fk","id_article_fk"
]
df_lf.to_csv(dossier_datalake_processed / "ventes" / "LignesFacture.csv", index=False)

# -------------------------------------------------------------------
# 2) SCHÉMA 'achats' (3FN)
# -------------------------------------------------------------------

# 2.1 – Fournisseurs
df_fr = achats[[
    "Ref cde fournisseur","Raison sociale",
    "famille client","responsable du dossier","représentant"
]].drop_duplicates().reset_index(drop=True)
df_fr.insert(0, "id_fournisseur", df_fr.index + 1)
df_fr.columns = [
    "id_fournisseur","code_fournisseur","raison_sociale",
    "famille_fournisseur","responsable_dossier","representant"
]
df_fr.to_csv(dossier_datalake_processed / "achats" / "Fournisseurs.csv", index=False)

# 2.2 – FamillesArticles
df_fa2 = achats[[
    "famille article libellé","sous-famille article libellé"
]].drop_duplicates().reset_index(drop=True)
df_fa2.insert(0, "id_famille", df_fa2.index + 1)
df_fa2["code_famille"] = df_fa2["famille article libellé"]
df_fa2 = df_fa2[[
    "id_famille","code_famille",
    "famille article libellé","sous-famille article libellé"
]]
df_fa2.columns = [
    "id_famille","code_famille",
    "libelle_famille","libelle_sous_famille"
]
df_fa2.to_csv(dossier_datalake_processed / "achats" / "FamillesArticles.csv", index=False)

# 2.3 – Articles
df_a2 = achats[[
    "code article","Désignation","Numéro de plan"
]].drop_duplicates().reset_index(drop=True)
df_a2.insert(0, "id_article", df_a2.index + 1)
df_a2 = df_a2.merge(
    df_fa2[["code_famille","id_famille"]],
    left_on="code article", right_on="code_famille", how="left"
)
df_a2 = df_a2[[
    "id_article","code article","Désignation",
    "Numéro de plan","id_famille"
]]
df_a2.columns = [
    "id_article","code_article","designation",
    "numero_plan","id_famille_fk"
]
df_a2.to_csv(dossier_datalake_processed / "achats" / "Articles.csv", index=False)

# Préparer les maps pour fournisseurs et commandes fournisseurs
map_fournisseur   = dict(zip(df_fr["code_fournisseur"], df_fr["id_fournisseur"]))

# 2.4 – ArticlesFournisseurs
df_af2 = achats[[
    "code article","Ref cde fournisseur"
]].drop_duplicates().reset_index(drop=True)
df_af2.insert(0, "id_art_fourn", df_af2.index + 1)
df_af2["id_article_fk"]    = df_af2["code article"].map(map_article).astype("Int64")
df_af2["id_fournisseur_fk"] = df_af2["Ref cde fournisseur"].map(map_fournisseur).astype("Int64")
df_af2 = df_af2[[
    "id_art_fourn","Ref cde fournisseur",
    "id_article_fk","id_fournisseur_fk"
]]
df_af2.columns = [
    "id_art_fourn","ref_article_fournisseur",
    "id_article_fk","id_fournisseur_fk"
]
df_af2.to_csv(dossier_datalake_processed / "achats" / "ArticlesFournisseurs.csv", index=False)

# 2.5 – CommandesFournisseurs
df_cf = achats[[
    "N° Cde","Date BL","Ref cde fournisseur"
]].drop_duplicates().reset_index(drop=True)
df_cf.insert(0, "id_commande_fourn", df_cf.index + 1)
df_cf["id_fournisseur_fk"] = df_cf["Ref cde fournisseur"].map(map_fournisseur).astype("Int64")
df_cf = df_cf[[
    "id_commande_fourn","N° Cde","Date BL","id_fournisseur_fk"
]]
df_cf.columns = [
    "id_commande_fourn","num_commande","date_commande","id_fournisseur_fk"
]
df_cf.to_csv(dossier_datalake_processed / "achats" / "CommandesFournisseurs.csv", index=False)

# 2.6 – FacturesFournisseurs
map_commande_fourn = dict(zip(df_cf["num_commande"], df_cf["id_commande_fourn"]))
df_ff = achats[[
    "N° facture","date facture","N° BL","Ref cde fournisseur","N° Cde"
]].drop_duplicates().reset_index(drop=True)
df_ff.insert(0, "id_facture_fourn", df_ff.index + 1)
df_ff["id_fournisseur_fk"]       = df_ff["Ref cde fournisseur"].map(map_fournisseur).astype("Int64")
df_ff["id_commande_fourn_fk"]    = df_ff["N° Cde"].map(map_commande_fourn).astype("Int64")
df_ff = df_ff[[
    "id_facture_fourn","N° facture","date facture","N° BL",
    "id_fournisseur_fk","id_commande_fourn_fk"
]]
df_ff.columns = [
    "id_facture_fourn","num_facture","date_facture","num_bl",
    "id_fournisseur_fk","id_commande_fourn_fk"
]
df_ff.to_csv(dossier_datalake_processed / "achats" / "FacturesFournisseurs.csv", index=False)

# 2.7 – LignesFactureFournisseur
df_lf2 = achats[[
    "Qté fact","Prix Unitaire","N° facture","code article"
]].drop_duplicates().reset_index(drop=True)
df_lf2.insert(0, "id_ligne_facture_fourn", df_lf2.index + 1)
map_facture_fourn = dict(zip(df_ff["num_facture"], df_ff["id_facture_fourn"]))

df_lf2["qte_achetee"]            = pd.to_numeric(df_lf2["Qté fact"], errors="coerce")
df_lf2["prix_unitaire_achat"]    = pd.to_numeric(df_lf2["Prix Unitaire"], errors="coerce")
df_lf2["id_facture_fourn_fk"]     = df_lf2["N° facture"].map(map_facture_fourn).astype("Int64")
df_lf2["id_article_fk"]           = df_lf2["code article"].map(map_article).astype("Int64")

df_lf2 = df_lf2[[
    "id_ligne_facture_fourn","qte_achetee",
    "prix_unitaire_achat","id_facture_fourn_fk","id_article_fk"
]]
df_lf2.to_csv(dossier_datalake_processed / "achats" / "LignesFactureFournisseur.csv", index=False)
