import os
import pandas as pd
from collections import defaultdict

from src.outils.chemins import dossier_resultats_jonctions, dossier_stats_resultats_jonctions

# --- Configuration ---
# Carpeta donde están los CSV de intersección (salida del primer script)
dossier_source = dossier_resultats_jonctions

# Carpeta donde vamos a guardar el informe de estadísticas
dossier_stats = dossier_stats_resultats_jonctions
fichier_rapport = os.path.join(dossier_stats, "rapport_intersections.txt")

# Crear carpeta de estadísticas si no existe
os.makedirs(dossier_stats, exist_ok=True)

# --- Función de extracción desde nombre de fichero ---
def extraire_infos_intersect(nom_fichier: str):
    """
    Desde "intersect_ARTICLE_DO_PIECE.csv" devuelve ("ARTICLE", "DO_PIECE")
    """
    base = os.path.splitext(nom_fichier)[0]
    prefix = "intersect_"
    if not base.startswith(prefix):
        return (None, None)
    reste = base[len(prefix):]
    parts = reste.split("_", 1)
    if len(parts) != 2:
        return (None, None)
    return parts[0], parts[1]

# --- Listar los CSV de intersección en la carpeta fuente ---
fichiers = [
    f for f in os.listdir(dossier_source)
    if f.lower().endswith(".csv") and f.startswith("intersect_")
]

# --- Calcular palmarés top 20 por número de filas ---
stats = []
for f in fichiers:
    chemin = os.path.join(dossier_source, f)
    try:
        # Contar líneas sin cargar todo
        n_rows = sum(1 for _ in open(chemin, 'r', encoding='utf-8')) - 1
    except Exception:
        try:
            df = pd.read_csv(chemin, dtype=str)
            n_rows = df.shape[0]
        except Exception:
            n_rows = 0
    stats.append((f, n_rows))

# Ordenar descendentemente y quedarnos con las 20 primeras
stats_sorted = sorted(stats, key=lambda x: x[1], reverse=True)
palmares = stats_sorted[:20]

# --- Agrupar por tabla comparada ---
groupes = defaultdict(list)
for f, n in stats:
    table_clean, col_cible = extraire_infos_intersect(f)
    if table_clean:
        groupes[table_clean].append((f, col_cible, n))

# --- Escribir el informe en un archivo de texto ---
with open(fichier_rapport, 'w', encoding='utf-8') as rapport:
    # Palmarés
    rapport.write("=== Palmarès des 20 fichiers ayant le plus de lignes ===\n")
    for rang, (f, n) in enumerate(palmares, start=1):
        table_clean, col_cible = extraire_infos_intersect(f)
        rapport.write(f"{rang:2d}. Table : {table_clean:<20} Colonne : {col_cible:<15} Lignes : {n}\n")
    rapport.write("\n")

    # Informe detallado por sección
    for table, fichiers_table in groupes.items():
        rapport.write(f"=== Section : Table comparée → {table} ===\n\n")
        for nom_f, col_cible, n_rows_est in fichiers_table:
            chemin = os.path.join(dossier_source, nom_f)
            try:
                df = pd.read_csv(chemin, dtype=str)
                n_rows, n_cols = df.shape
            except Exception as e:
                rapport.write(f"  [Erreur chargement] {nom_f} : {e}\n\n")
                continue

            rapport.write(f"  Fichier : {nom_f}\n")
            rapport.write(f"    - Colonne clé : {col_cible}\n")
            rapport.write(f"    - Nombre de lignes  : {n_rows}\n")
            rapport.write(f"    - Nombre de colonnes: {n_cols}\n")
            rapport.write(f"    - Duplications par colonne :\n")
            for col in df.columns:
                total   = n_rows
                uniques = df[col].nunique(dropna=False)
                dupli   = total - uniques
                rapport.write(f"        • {col} : {dupli} valeur(s) dupliquée(s)\n")
            rapport.write("\n")

print(f"Rapport généré : {fichier_rapport}")
