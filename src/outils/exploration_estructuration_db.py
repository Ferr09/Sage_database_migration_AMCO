import os
from collections import defaultdict

import matplotlib.pyplot as plt
import matplotlib.cm as cm
import networkx as nx

from src.outils.chemins import (
    dossier_txt_entetes,
    dossier_tables_statistiques
)

# ==============================================================================
# 1) CHARGER LA LISTE DES TABLES IMPORTANTES (≥100 LIGNES)
# ==============================================================================
fichier_plus100 = dossier_tables_statistiques / "tables_plus_de_100_lignes.txt"
with open(fichier_plus100, "r", encoding="utf-8") as f:
    tables_importantes = {line.strip() for line in f if line.strip()}

# ==============================================================================
# 2) LIRE ET NORMALISER LES EN-TÊTES DE CHAQUE TABLE
# ==============================================================================
def charger_entetes(dossier, tables_cibles):
    colonnes_par_table = {}
    for fichier in os.listdir(dossier):
        if fichier.endswith("_entetes.txt"):
            nom_table = fichier.replace("_entetes.txt", "")
            if nom_table in tables_cibles:
                chemin = dossier / fichier
                with open(chemin, "r", encoding="utf-8") as ff:
                    # strip + lower pour chaque en-tête
                    colonnes = [l.strip().lower() for l in ff if l.strip()]
                    colonnes_par_table[nom_table] = colonnes
    return colonnes_par_table

colonnes_par_table = charger_entetes(dossier_txt_entetes, tables_importantes)

# ==============================================================================
# 3) CLASSIFICATION DES TABLES PAR FAMILLE (PRIORISATION DES TABLES)
# ==============================================================================
def classifier_famille(table, colonnes):
    """
    Retourne la famille de la table selon ses colonnes. Override possible pour
    forcer positionnement élevé (priorité) sur certaines tables clés.
    """
    nom = table.upper()
    # Override : forcer certaines tables dans des familles prioritaires
    if nom == "F_DOCLIGNE":
        return "commandes"
    if nom == "F_DOCENTETE":
        return "commandes"
    if nom == "F_ARTFOURNISS":
        return "achats"
    if nom == "F_ARTCLIENT":
        return "ventes"

    # Heuristique par mots-clés
    familles = {
        "articles":     ["ar_ref", "libelle", "designation", "prix", "gamme"],
        "clients":      ["ct_num", "nom", "raison", "adresse"],
        "commandes":    ["dl_no", "doc_no", "commande", "date", "quantite"],
        "prix":         ["prix", "tarif", "remise"],
        "chiffre":      ["montant", "ca", "tva"],
        "quantite":     ["quantite", "stock", "unite"],
        "fournisseurs": ["af_reffourniss", "codefourniss"],
        "logistique":   ["lotserie", "lotfinfo", "lotfimo", "lotfdlc"],
        "production":   ["fo_codeprod", "fo_dateprod"],
        "stockage":     ["st_codestock", "st_emplacement"],
        "livraison":    ["f_li_no", "f_li_date"],
        "qualité":      ["qual_code", "qual_statut"]
    }
    scores = defaultdict(int)
    for col in colonnes:
        for famille, mots_cles in familles.items():
            if any(mot in col for mot in mots_cles):
                scores[famille] += 1
    if scores:
        return max(scores, key=scores.get)
    return "autres"

groupes_tables = {tbl: classifier_famille(tbl, cols) for tbl, cols in colonnes_par_table.items()}

# Priorités numériques pour l’ordonnancement : plus petit = priorité haute
priorité_familles = {
    "commandes":  0,
    "achats":     0,
    "ventes":     0,
    "articles":   1,
    "clients":    1,
    "prix":       2,
    "chiffre":    2,
    "quantite":   3,
    "logistique": 4,
    "production": 4,
    "stockage":   4,
    "livraison":  4,
    "qualité":    4,
    "autres":     5
}

# ==============================================================================
# 4) DÉFINIR LA PRIORITÉ DES COLONNES (PRIORISATION DES COLONNES)
# ==============================================================================
colonnes_prios = ["ar_ref", "fa_codefamille", "ct_num", "dl_no", "af_reffourniss"]
# On utilisera l’ordre dans cette liste pour ordonner les arêtes

# ==============================================================================
# 5) CONSTRUCTION DU GRAPHE ET ÉTIQUETAGE DES ARÊTES
#    Chaque arête porte l’attribut “colonne” = nom de la colonne qui relie les tables
# ==============================================================================
def construction_graphe(colonnes_par_table):
    graphe = defaultdict(list)
    labels_arcs = {}
    connexions_faites = set()

    # Phase 1 : connexions pour colonnes prioritaires
    for src, cols in colonnes_par_table.items():
        for col in colonnes_prios:
            if col in cols:
                for tgt, tgt_cols in colonnes_par_table.items():
                    if tgt != src and col in tgt_cols and (src, tgt) not in connexions_faites:
                        graphe[src].append(tgt)
                        labels_arcs[(src, tgt)] = col
                        connexions_faites.add((src, tgt))

    # Phase 2 : connexions pour toutes les autres colonnes
    for src, cols in colonnes_par_table.items():
        for col in cols:
            if col in colonnes_prios:
                continue
            for tgt, tgt_cols in colonnes_par_table.items():
                if tgt != src and col in tgt_cols and (src, tgt) not in connexions_faites:
                    graphe[src].append(tgt)
                    labels_arcs[(src, tgt)] = col
                    connexions_faites.add((src, tgt))

    return graphe, labels_arcs

graphe, labels_arcs = construction_graphe(colonnes_par_table)

# ==============================================================================
# 6) CHERCHER PLUSIEURS CHEMINS PAR DFS AVEC PRIORITÉS
#    - Priorité sur les colonnes (visiter d’abord arêtes issues de colonnes prioritaires)
#    - Priorité sur les tables (visiter d’abord voisins de familles prioritaires)
#    - On limite à max_chemins et profondeur ≤ profondeur_max pour éviter explosion
# ==============================================================================
def chercher_chemins_priorises(graphe, labels_arcs, groupes_tables,
                              max_chemins=5, profondeur_max=6):
    chemins = []
    explorations = 0

    def dfs(noeud, chemin, vus):
        nonlocal explorations
        if explorations >= max_chemins:
            return
        if len(chemin) - 1 > profondeur_max:
            return
        # On enregistre chaque chemin non trivial (au moins 2 nœuds)
        if len(chemin) >= 2:
            chemins.append(list(chemin))
            explorations += 1
            if explorations >= max_chemins:
                return
        # Préparer la liste des voisins triés par priorité (table puis colonne)
        voisins = []
        for tgt in graphe.get(noeud, []):
            if tgt not in vus:
                col = labels_arcs.get((noeud, tgt), "")
                # index de la colonne dans colonnes_prios (plus petit = meilleure priorité, sinon très grand)
                idx_col = colonnes_prios.index(col) if col in colonnes_prios else len(colonnes_prios)
                # priorité de la famille de la table cible
                prio_fam = priorité_familles.get(groupes_tables.get(tgt, "autres"), len(priorité_familles))
                voisins.append((prio_fam, idx_col, tgt))
        # Trier d’abord par prio_fam, puis par idx_col
        voisins.sort(key=lambda x: (x[0], x[1]))
        # Explorer dans cet ordre
        for _, _, voisin in voisins:
            chemin.append(voisin)
            vus.add(voisin)
            dfs(voisin, chemin, vus)
            vus.remove(voisin)
            chemin.pop()
            if explorations >= max_chemins:
                return

    # Lancer DFS depuis chaque table jusqu’à max_chemins
    for source in graphe.keys():
        if explorations >= max_chemins:
            break
        dfs(source, [source], {source})

    return chemins

chemins = chercher_chemins_priorises(graphe, labels_arcs, groupes_tables,
                                     max_chemins=5, profondeur_max=6)

print(">>> Chemins prioritaires trouvés :")
for idx, chemin in enumerate(chemins, 1):
    print(f"  {idx:02d}. {' → '.join(chemin)}")

# ==============================================================================
# 7) VISUALISATION : SOUS-GRAPHE RESTREINT + CHEMINS COLORÉS AVEC ÉTIQUETTES
#    - On regroupe tous les nœuds des chemins et leurs voisins directs
#    - Afficher en gris le sous-graphe, puis superposer chaque chemin en couleur
# ==============================================================================
def visualiser_chemins_final(graphe, labels_arcs, chemins):
    if not chemins:
        print(">>> Aucun chemin à afficher.")
        return

    # 7.1) Rassembler tous les nœuds qui apparaissent dans les chemins
    noeuds_chemins = set()
    for seq in chemins:
        noeuds_chemins.update(seq)

    # 7.2) Ajouter les voisins directs pour conserver un contexte minimal
    noeuds_contextes = set(noeuds_chemins)
    for n in list(noeuds_chemins):
        for voisin in graphe.get(n, []):
            noeuds_contextes.add(voisin)
        for src, tgt_list in graphe.items():
            if n in tgt_list:
                noeuds_contextes.add(src)

    # 7.3) Construire le sous-graphe contenant uniquement ces nœuds et arêtes
    G_sub = nx.DiGraph()
    for src in noeuds_contextes:
        for tgt in graphe.get(src, []):
            if tgt in noeuds_contextes:
                G_sub.add_edge(src, tgt, label=labels_arcs.get((src, tgt), ""))

    # 7.4) Calculer un layout rapide pour ce sous-graphe
    pos = nx.spring_layout(G_sub, k=0.5, iterations=30)

    plt.figure(figsize=(14, 8))

    # 7.5) Dessiner tous les nœuds + étiquettes de nœuds
    nx.draw_networkx_nodes(G_sub, pos,
                           node_size=1000,
                           node_color="lightblue",
                           edgecolors="black")
    nx.draw_networkx_labels(G_sub, pos,
                            font_size=9,
                            font_weight="bold")

    # 7.6) Dessiner toutes les arêtes du sous-graphe en gris (contexte global)
    toutes_sub_arcs = list(G_sub.edges())
    nx.draw_networkx_edges(G_sub, pos,
                           edgelist=toutes_sub_arcs,
                           width=1.0,
                           edge_color="lightgray")

    # 7.7) Superposer chaque chemin en couleur avec étiquettes de colonnes
    cmap = cm.get_cmap("tab10")
    for idx, seq in enumerate(chemins):
        couleur = cmap(idx % cmap.N)
        # Liste des arêtes (src, tgt) du chemin
        edges_chemin = [(seq[i], seq[i+1]) for i in range(len(seq)-1)]
        # Filtrer pour ne garder que celles présentes dans le sous-graphe
        edges_chemin = [edge for edge in edges_chemin if edge in G_sub.edges()]

        # Dessiner ces arêtes en couleur épaisse
        nx.draw_networkx_edges(G_sub, pos,
                               edgelist=edges_chemin,
                               width=3.0,
                               edge_color=[couleur] * len(edges_chemin))

        # Étiquettes : nom de la colonne qui relie src→tgt
        labels_path = {
            (src, tgt): labels_arcs.get((src, tgt), "")
            for (src, tgt) in edges_chemin
        }
        nx.draw_networkx_edge_labels(G_sub, pos,
                                     edge_labels=labels_path,
                                     font_size=7,
                                     label_pos=0.5)

    plt.title("Visualisation des chemins prioritaires (sous-graphe restreint)", fontsize=14)
    plt.axis("off")
    plt.tight_layout()
    plt.show()

# Affichage final
visualiser_chemins_final(graphe, labels_arcs, chemins)
