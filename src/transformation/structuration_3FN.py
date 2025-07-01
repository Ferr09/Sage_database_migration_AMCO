#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from pathlib import Path
import pandas as pd

try:
    from src.outils.chemins import dossier_datalake_processed
except ImportError:
    projet_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(projet_root / "src"))
    from outils.chemins import dossier_datalake_processed

os.makedirs(dossier_datalake_processed / "ventes", exist_ok=True)
os.makedirs(dossier_datalake_processed / "achats", exist_ok=True)

# Chargement des CSV généraux
ventes = pd.read_csv(dossier_datalake_processed / "tabla_generale_ventes.csv", dtype=str, encoding="utf-8-sig")
achats = pd.read_csv(dossier_datalake_processed / "tabla_generale_achats.csv", dtype=str, encoding="utf-8-sig")

# --- VENTES ---
# Clients
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

# FamillesArticles
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

# Articles
df_art = ventes[[
    "code article","Désignation",
    "Numéro de plan","Ref art client","famille article libellé"
]].drop_duplicates().reset_index(drop=True)
df_art.insert(0, "id_article", df_art.index + 1)
df_art = df_art.merge(
    df_fam[["code_famille","id_famille"]],
    left_on="famille article libellé",
    right_on="code_famille", how="left"
)
df_art = df_art[[
    "id_article","code article","Désignation",
    "Numéro de plan","Ref art client","id_famille"
]]
df_art.columns = [
    "id_article","code_article","designation",
    "numero_plan","ref_article_client","id_famille_fk"
]
df_art.to_csv(dossier_datalake_processed / "ventes" / "Articles.csv", index=False)

# CommandesClients
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

# Factures
df_fac = ventes[[
    "N° facture","date facture","N° BL",
    "Date BL","condition_livraison",
    "Code client","N° Cde"
]].drop_duplicates().reset_index(drop=True)
df_fac.insert(0, "id_facture", df_fac.index + 1)
df_fac = df_fac.merge(
    df_clients[["code_client","id_client"]],
    left_on="Code client", right_on="code_client", how="left"
).merge(
    df_cmd[["num_commande","id_commande_client"]],
    left_on="N° Cde", right_on="num_commande", how="left"
)
df_fac = df_fac[[
    "id_facture","N° facture","date facture",
    "N° BL","Date BL","condition_livraison",
    "id_client","id_commande_client"
]]
df_fac.columns = [
    "id_facture","num_facture","date_facture",
    "num_bl","date_bl","condition_livraison",
    "id_client_fk","id_commande_client_fk"
]
df_fac.to_csv(dossier_datalake_processed / "ventes" / "Factures.csv", index=False)

# LignesFacture
df_lf = ventes[[
    "Qté fact","Prix Unitaire","Tot HT","N° facture","code article"
]].drop_duplicates().reset_index(drop=True)
df_lf.insert(0, "id_ligne_facture", df_lf.index + 1)
df_lf = df_lf.merge(
    df_fac[["num_facture","id_facture"]],
    left_on="N° facture", right_on="num_facture", how="left"
).merge(
    df_art[["code_article","id_article"]],
    left_on="code article", right_on="code_article", how="left"
)
df_lf = df_lf[[
    "id_ligne_facture","Qté fact","Prix Unitaire",
    "id_facture","id_article"
]]
df_lf.columns = [
    "id_ligne_facture","qte_vendue","prix_unitaire_vente",
    "id_facture_fk","id_article_fk"
]
df_lf.to_csv(dossier_datalake_processed / "ventes" / "LignesFacture.csv", index=False)

# --- ACHATS ---
# Fournisseurs
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

# FamillesArticles (achats)
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

# Articles (achats)
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

# ArticlesFournisseurs
df_af2 = achats[[
    "code article","Ref cde fournisseur"
]].drop_duplicates().reset_index(drop=True)
df_af2.insert(0, "id_art_fourn", df_af2.index + 1)
df_af2 = df_af2.merge(
    df_a2[["code_article","id_article"]],
    left_on="code article", right_on="code_article", how="left"
).merge(
    df_fr[["code_fournisseur","id_fournisseur"]],
    left_on="Ref cde fournisseur", right_on="code_fournisseur", how="left"
)
df_af2 = df_af2[[
    "id_art_fourn","Ref cde fournisseur","id_article","id_fournisseur"
]]
df_af2.columns = [
    "id_art_fourn","ref_article_fournisseur",
    "id_article_fk","id_fournisseur_fk"
]
df_af2.to_csv(dossier_datalake_processed / "achats" / "ArticlesFournisseurs.csv", index=False)

# CommandesFournisseurs
df_cf = achats[[
    "N° Cde","Ref cde fournisseur","Date BL"
]].drop_duplicates().reset_index(drop=True)
df_cf.insert(0, "id_commande_fourn", df_cf.index + 1)
df_cf = df_cf.merge(
    df_fr[["code_fournisseur","id_fournisseur"]],
    left_on="Ref cde fournisseur", right_on="code_fournisseur", how="left"
)
df_cf = df_cf[[
    "id_commande_fourn","N° Cde","Date BL","id_fournisseur"
]]
df_cf.columns = [
    "id_commande_fourn","num_commande","date_commande","id_fournisseur_fk"
]
df_cf.to_csv(dossier_datalake_processed / "achats" / "CommandesFournisseurs.csv", index=False)

# FacturesFournisseurs
df_ff = achats[[
    "N° facture","date facture","N° BL",
    "Ref cde fournisseur","N° Cde"
]].drop_duplicates().reset_index(drop=True)
df_ff.insert(0, "id_facture_fourn", df_ff.index + 1)
df_ff = df_ff.merge(
    df_fr[["code_fournisseur","id_fournisseur"]],
    left_on="Ref cde fournisseur", right_on="code_fournisseur", how="left"
).merge(
    df_cf[["num_commande","id_commande_fourn"]],
    left_on="N° Cde", right_on="num_commande", how="left"
)
df_ff = df_ff[[
    "id_facture_fourn","N° facture","date facture","N° BL",
    "id_fournisseur","id_commande_fourn"
]]
df_ff.columns = [
    "id_facture_fourn","num_facture","date_facture","num_bl",
    "id_fournisseur_fk","id_commande_fourn_fk"
]
df_ff.to_csv(dossier_datalake_processed / "achats" / "FacturesFournisseurs.csv", index=False)

# LignesFactureFournisseur
df_lf2 = achats[[
    "Qté fact","Prix Unitaire","N° facture","code article"
]].drop_duplicates().reset_index(drop=True)
df_lf2.insert(0, "id_ligne_facture_fourn", df_lf2.index + 1)
df_lf2 = df_lf2.merge(
    df_ff[["num_facture","id_facture_fourn"]],
    left_on="N° facture", right_on="num_facture", how="left"
).merge(
    df_a2[["code_article","id_article"]],
    left_on="code article", right_on="code_article", how="left"
)
df_lf2 = df_lf2[[
    "id_ligne_facture_fourn","Qté fact","Prix Unitaire",
    "id_facture_fourn","id_article"
]]
df_lf2.columns = [
    "id_ligne_facture_fourn","qte_achetee",
    "prix_unitaire_achat","id_facture_fourn_fk","id_article_fk"
]
df_lf2.to_csv(dossier_datalake_processed / "achats" / "LignesFactureFournisseur.csv", index=False)
