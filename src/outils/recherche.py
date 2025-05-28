import os

# === Groupes métiers ===
groupes_tables = {
    "articles":   ["f_article", "f_artclient", "f_artfourniss", "f_artgamme", "f_artprix", "f_artstock", "f_artstockempl", "f_artcompo"],
    "clients":    ["p_tiers", "f_famclient", "f_artclient"],
    "commandes":  ["f_docentete", "f_docligne", "f_docligneempl", "f_docregl", "f_piece", "p_cmddetail"],
    "prix":       ["f_artprix", "f_tarif", "f_tarifcond", "f_tarifqte", "f_tarifremise"],
    "quantité":   ["f_docligne", "f_artstock", "f_artstockempl", "f_artcompo"],
    "chiffre":    ["f_docentete", "f_docligne", "f_caisse"],
}

# === Chemins des fichiers de stats ===
dossier_statistiques = r"statistiques\tables"
f_vides   = os.path.join(dossier_statistiques, "tables_vides.txt")
f_10      = os.path.join(dossier_statistiques, "tables_plus_de_10_lignes.txt")
f_100     = os.path.join(dossier_statistiques, "tables_plus_de_100_lignes.txt")

# Lire les listes de stats
def lire_liste(path):
    if not os.path.isfile(path):
        return set()
    return {l.strip() for l in open(path, encoding="utf-8") if l.strip()}

tables_vides      = lire_liste(f_vides)
tables_plus10     = lire_liste(f_10)
tables_plus100    = lire_liste(f_100)

# Détecte le(s) groupe(s) métiers
def trouver_groupes(nom_table):
    nom = nom_table.lower()
    return [g for g, tbls in groupes_tables.items() if nom in tbls]

# Détermine le statut en priorité
def statut_table(nom_table):
    if nom_table in tables_vides:
        return "Vide"
    if nom_table in tables_plus100:
        return "> 100 lignes"
    if nom_table in tables_plus10:
        return "> 10 lignes"
    return "1–10 lignes"

# Recherche un mot-clé dans les entêtes
def rechercher_dans_fichiers(dossier, mot_cle):
    resultats = []
    for fname in os.listdir(dossier):
        if not fname.endswith("_entetes.txt"):
            continue
        chemin = os.path.join(dossier, fname)
        with open(chemin, "r", encoding="utf-8") as f:
            colonnes = [l.strip() for l in f if l.strip()]
        trouves = [c for c in colonnes if mot_cle.lower() in c.lower()]
        if trouves:
            table = fname.removesuffix("_entetes.txt")
            grps  = trouver_groupes(table)
            stat  = statut_table(table)
            resultats.append((table, stat, grps, trouves))

    if not resultats:
        print("Aucune colonne trouvée avec ce mot-clé.")
        return

    for table, stat, grps, cols in resultats:
        print("\n============================")
        print(f"Table   : {table}")
        print(f"Statut  : {stat}")
        print("Groupes : " + (", ".join(grps) if grps else "Aucun"))
        print("Colonnes correspondantes :")
        for c in cols:
            print(f"- {c}")

# === Programme principal ===
if __name__ == "__main__":
    dossier_txt = os.path.join(r"extraits", "entetes_csv")
    while True:
        motcle = input("Mot-clé à rechercher (ou 'exit') : ").strip()
        if motcle.lower() == "exit":
            print("Bye !")
            break
        if not motcle:
            print("Entrée vide, réessayez.")
            continue
        rechercher_dans_fichiers(dossier_txt, motcle)
