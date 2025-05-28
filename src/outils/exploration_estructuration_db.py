import os
import time
from collections import defaultdict

import matplotlib.cm as cm
import matplotlib.pyplot as plt
import networkx as nx

# === Configuration des chemins ===
base_dir = r"extraits"
base_dir_stats = r"statistiques"
dossier_entetes     = os.path.join(base_dir, "entetes_csv")
dossier_statistiques = os.path.join(base_dir_stats, "tables")
dossier_analyse     = os.path.join(base_dir_stats, "analyse_structure_db")
os.makedirs(dossier_analyse, exist_ok=True)

# === Lire la liste des tables à modéliser (≥10 lignes) ===
fichier_plus10 = os.path.join(dossier_statistiques, "tables_plus_de_10_lignes.txt")
with open(fichier_plus10, "r", encoding="utf-8") as f:
    tables_importantes = {line.strip() for line in f if line.strip()}

print(f"Tables à modéliser (≥10 lignes) : {sorted(tables_importantes)}\n")

# === 1) Construire le graphe de relations d'entêtes ===
def construction_graphe_relations_entetes(dossier, tables_cibles):
    print(f"DEBUG: tables_importantes = {tables_cibles}")
    tous_fichiers = [f for f in os.listdir(dossier) if f.endswith("_entetes.txt")]
    print(f"DEBUG: fichiers entetes trouvés = {tous_fichiers}")

    graphe = defaultdict(list)
    colonnes_par_table = {}

    for fichier in tous_fichiers:
        nom_table = os.path.splitext(fichier)[0]  # e.g. "F_DOCENTETE_entetes"
        # para extraer la tabla: quitar sufijo "_entetes"
        nom_table_brut = nom_table.replace("_entetes","")
        print(f"  -> candidat: {nom_table_brut}")
        if nom_table_brut not in tables_cibles:
            print(f"     ignoré (no en tables_cibles)")
            continue

        chemin = os.path.join(dossier, fichier)
        with open(chemin, 'r', encoding='utf-8') as f:
            colonnes = [l.strip().lower() for l in f if l.strip()]
        colonnes_par_table[nom_table_brut] = colonnes

    print(f"DEBUG: colonnes_par_table keys = {list(colonnes_par_table.keys())}")

    # ahora construimos los arcos
    for src, cols in colonnes_par_table.items():
        for col in cols:
            for tgt, tgt_cols in colonnes_par_table.items():
                if tgt != src and col in tgt_cols and tgt not in graphe[src]:
                    graphe[src].append(tgt)

    return graphe

# === 2) Enregistrer le graphe dans un txt ===
def enregistrer_graphe_dans_fichier(graphe, nom_fichier):
    chemin = os.path.join(dossier_analyse, nom_fichier)
    with open(chemin, 'w', encoding='utf-8') as f:
        for src in sorted(graphe):
            f.write(f"Table : {src}\n{'-'*(8+len(src))}\n")
            if graphe[src]:
                for tgt in sorted(graphe[src]):
                    f.write(f"{src} -> {tgt}\n")
            else:
                f.write("(aucun lien détecté)\n")
            f.write("\n")
    print(f"Graphe enregistré dans : {chemin}")

# === 3) Trouver chemins groupés ===
groupes_tables = {
    "articles":     ["f_article","f_artclient","f_artfourniss","f_artgamme","f_artprix","f_artstock","f_artstockempl","f_artcompo"],
    "clients":      ["p_tiers","f_famclient","f_artclient"],
    "commandes":    ["f_docentete","f_docligne","f_docligneempl","f_docregl","f_piece","p_cmddetail"],
    "prix":         ["f_artprix","f_tarif","f_tarifcond","f_tarifqte","f_tarifremise"],
    "quantite":     ["f_docligne","f_artstock","f_artstockempl","f_artcompo"],
    "chiffre":      ["f_docentete","f_docligne","f_caisse"],
}

def groupe_de_table(nom):
    for g, tbls in groupes_tables.items():
        if nom in tbls:
            return g
    return None

def enregistrer_chemins_groupes_valides(graphe, nombre_max=10, profondeur_max=30,
                                         rep_limite=5, prefix_limite=2, min_groupes=4,
                                         nom_fichier="chemins_groupes.txt"):
    G = nx.DiGraph()
    for src, tgts in graphe.items():
        for tgt in tgts:
            G.add_edge(src, tgt)

    chemins = []
    prefix_counter = defaultdict(int)

    def dfs(noeud, chemin, groupes_us, vus):
        if len(chemins) >= nombre_max:
            return
        chemin.append(noeud)
        grp = groupe_de_table(noeud)
        if grp:
            groupes_us.add(grp)
        prefix = tuple(chemin[:prefix_limite])
        if prefix_counter[prefix] >= rep_limite:
            return
        if len(groupes_us) >= min_groupes:
            prefix_counter[prefix] += 1
            chemins.append((list(chemin[:profondeur_max]), set(groupes_us)))
        if len(chemin) >= profondeur_max:
            return
        for v in G.successors(noeud):
            if (noeud, v) not in vus:
                dfs(v, chemin.copy(), groupes_us.copy(), vus | {(noeud, v)})

    start = time.time()
    for n in G.nodes():
        if groupe_de_table(n):
            dfs(n, [], set(), set())
        if len(chemins) >= nombre_max:
            break
    duration = time.time() - start

    chemin = os.path.join(dossier_analyse, nom_fichier)
    with open(chemin, "w", encoding="utf-8") as f:
        f.write(f"Chemins valides (max {nombre_max}) – durée : {duration:.2f}s\n\n")
        for i, (seq, grps) in enumerate(chemins, 1):
            f.write(f"{i:02d}. {' → '.join(seq)}   [groupes : {', '.join(sorted(grps))}]\n")
    print(f"Chemins enregistrés dans : {chemin}")
    return chemins

# === 4) Visualiser ===
def visualiser(graphe, chemins, groupes, titre="Relations entre tables importantes"):
    import matplotlib.pyplot as plt
    import matplotlib.cm as cm
    import networkx as nx
    from collections import defaultdict

    # Construire le DiGraph
    G = nx.DiGraph()
    for src, tgts in graphe.items():
        for tgt in tgts:
            G.add_edge(src, tgt)

    # Préparer un mapping table -> groupe, en normalisant en minuscules
    table_to_groupe = {}
    for g, tbls in groupes.items():
        for t in tbls:
            table_to_groupe[t.lower()] = g
    default_group = "autres"

    # Rassembler les nœuds par groupe en comparant en lowercase
    groupes_positions = defaultdict(list)
    for n in G.nodes():
        grp = table_to_groupe.get(n.lower(), default_group)
        groupes_positions[grp].append(n)

    # Déterminer les positions (colonnes par groupe, lignes par index)
    pos = {}
    x_gap, y_gap = 4, 1.5
    for i, (grp, nodes) in enumerate(sorted(groupes_positions.items())):
        for j, node in enumerate(sorted(nodes)):
            pos[node] = (i * x_gap, -j * y_gap)
        pos[f"label_{grp}"] = (i * x_gap, y_gap)

    # Tracé
    plt.figure(figsize=(16, 10))
    nx.draw_networkx_nodes(G, pos, node_size=1800, node_color="lightblue")
    nx.draw_networkx_labels(G, pos, font_size=10, font_weight="bold")
    nx.draw_networkx_edges(G, pos, edgelist=G.edges(), width=1.0, edge_color="gray")

    cmap = cm.get_cmap("tab10")
    for idx, (seq, _) in enumerate(chemins):
        edges = list(zip(seq, seq[1:]))
        color = cmap(idx % cmap.N)
        nx.draw_networkx_edges(
            G, pos,
            edgelist=edges,
            width=4.0,
            edge_color=[color] * len(edges),
        )

    # Étiquettes de groupes
    for grp in groupes_positions:
        x, y = pos[f"label_{grp}"]
        plt.text(
            x, y, grp.upper(),
            fontsize=14, fontweight="bold",
            ha="center", va="bottom",
            bbox=dict(facecolor="white", edgecolor="black", boxstyle="round,pad=0.3")
        )

    plt.title(titre, fontsize=16)
    plt.axis("off")
    plt.tight_layout()
    plt.show()

# === Main ===
if __name__=="__main__":
    graphe = construction_graphe_relations_entetes(dossier_entetes, tables_importantes)
    enregistrer_graphe_dans_fichier(graphe, "graphe_relations.txt")

    chemins = enregistrer_chemins_groupes_valides(graphe, nombre_max=5)
    visualiser(graphe, chemins, groupes_tables, titre="Relations entre tables importantes")

