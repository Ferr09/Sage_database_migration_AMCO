#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path

# --------------------------------------------------------------------
# Importation des chemins absolus depuis outils.chemins
# --------------------------------------------------------------------
from outils.chemins import racine_projet, dossier_statistiques, dossier_csv_extraits

# --------------------------------------------------------------------
# Groupes métiers et tables associées
# --------------------------------------------------------------------
groupes_tables = {
    "articles":   ["f_article", "f_artclient", "f_artfourniss", "f_artgamme", "f_artprix", "f_artstock", "f_artstockempl", "f_artcompo"],
    "clients":    ["p_tiers", "f_famclient", "f_artclient"],
    "commandes":  ["f_docentete", "f_docligne", "f_docligneempl", "f_docregl", "f_piece", "p_cmddetail"],
    "prix":       ["f_artprix", "f_tarif", "f_tarifcond", "f_tarifqte", "f_tarifremise"],
    "quantité":   ["f_docligne", "f_artstock", "f_artstockempl", "f_artcompo"],
    "chiffre":    ["f_docentete", "f_docligne", "f_caisse"],
}

# --------------------------------------------------------------------
# Chemins absolus des fichiers de statistiques
# --------------------------------------------------------------------
chemin_dossier_tables = dossier_statistiques / "tables"
f_vides   = chemin_dossier_tables / "tables_vides.txt"
f_10      = chemin_dossier_tables / "tables_plus_de_10_lignes.txt"
f_100     = chemin_dossier_tables / "tables_plus_de_100_lignes.txt"

# --------------------------------------------------------------------
# Lecture d’une liste à partir d’un fichier texte
# --------------------------------------------------------------------
def lire_liste(path: Path) -> set:
    if not path.is_file():
        return set()
    with path.open(encoding="utf-8") as f:
        return {l.strip() for l in f if l.strip()}

tables_vides      = lire_liste(f_vides)
tables_plus10     = lire_liste(f_10)
tables_plus100    = lire_liste(f_100)

# --------------------------------------------------------------------
# Détecte le(s) groupe(s) métiers d’une table donnée
# --------------------------------------------------------------------
def trouver_groupes(nom_table: str) -> list:
    nom = nom_table.lower()
    return [g for g, tbls in groupes_tables.items() if nom in tbls]

# --------------------------------------------------------------------
# Détermine le statut d’une table selon son nombre de lignes
# --------------------------------------------------------------------
def statut_table(nom_table: str) -> str:
    if nom_table in tables_vides:
        return "Vide"
    if nom_table in tables_plus100:
        return "> 100 lignes"
    if nom_table in tables_plus10:
        return "> 10 lignes"
    return "1–10 lignes"

# --------------------------------------------------------------------
# Recherche un mot-clé dans les fichiers d’en-têtes
# --------------------------------------------------------------------
def rechercher_dans_fichiers(dossier: Path, mot_cle: str) -> None:
    résultats = []
    for fichier in dossier.iterdir():
        if not fichier.name.endswith("_entetes.txt"):
            continue
        with fichier.open("r", encoding="utf-8") as f:
            colonnes = [l.strip() for l in f if l.strip()]
        trouves = [c for c in colonnes if mot_cle.lower() in c.lower()]
        if trouves:
            table = fichier.name.removesuffix("_entetes.txt")
            grps  = trouver_groupes(table)
            stat  = statut_table(table)
            résultats.append((table, stat, grps, trouves))

    if not résultats:
        print("Aucune colonne trouvée avec ce mot-clé.")
        return

    for table, stat, grps, cols in résultats:
        print("\n============================")
        print(f"Table   : {table}")
        print(f"Statut  : {stat}")
        print("Groupes : " + (", ".join(grps) if grps else "Aucun"))
        print("Colonnes correspondantes :")
        for c in cols:
            print(f"- {c}")

# --------------------------------------------------------------------
# Programme principal
# --------------------------------------------------------------------
if __name__ == "__main__":
    # Dossier où se trouvent les fichiers « _entetes.txt »
    dossier_txt = dossier_csv_extraits

    if not dossier_txt.exists():
        print(f"Le dossier d’en-têtes n’existe pas : {dossier_txt}")
        exit(1)

    while True:
        motcle = input("Mot-clé à rechercher (ou 'exit') : ").strip()
        if motcle.lower() == "exit":
            print("Bye !")
            break
        if not motcle:
            print("Entrée vide, réessayez.")
            continue
        rechercher_dans_fichiers(dossier_txt, motcle)
