import os
import pandas as pd

from src.outils.chemins import dossier_datalake_staging_sage, dossier_statistiques

# --- Configuration ---
dossier_travail    = dossier_datalake_staging_sage
nom_base          = "F_DOCENTETE_staging"
colonnes_base     = ["CT_NUMPAYEUR", "DO_PIECE", "DO_REF"]
dossier_resultats = os.path.join(dossier_statistiques, "resultats_intersections")
os.makedirs(dossier_resultats, exist_ok=True)

# --- Fonction de chargement selon l'extension ---
def charger_table(chemin):
    ext = os.path.splitext(chemin)[1].lower()
    if ext == ".csv":
        return pd.read_csv(chemin, dtype=str)
    if ext in [".xls", ".xlsx"]:
        return pd.read_excel(chemin, dtype=str)
    raise ValueError(f"Extension non supportée : {ext}")

# --- Chargement de la table DOCENTETE ---
fichier_base = next(
    (
        os.path.join(dossier_travail, f)
        for f in os.listdir(dossier_travail)
        if os.path.splitext(f)[0] == nom_base
        and os.path.splitext(f)[1].lower() in [".csv", ".xls", ".xlsx"]
    ),
    None
)
if not fichier_base:
    raise FileNotFoundError(f"Table de base {nom_base} introuvable dans {dossier_travail}")
df_base = charger_table(fichier_base)

# --- Nettoyage du nom de la table comparée pour les fichiers de sortie ---
def nettoyer_nom_table(nom):
    if nom.startswith(("F_", "P_")):
        nom = nom.split("_", 1)[1]
    if nom.endswith("_staging"):
        nom = nom[:-len("_staging")]
    return nom

# --- Traitement des tables cibles ---
log = []

for f in os.listdir(dossier_travail):
    base, ext = os.path.splitext(f)
    if ext.lower() not in [".csv", ".xls", ".xlsx"] or base == nom_base:
        continue

    # Chargement de la table cible
    try:
        df_cible = charger_table(os.path.join(dossier_travail, f))
    except Exception as e:
        log.append({
            "table_cible": base,
            "col_base": None,
            "col_cible": None,
            "statut": "erreur_chargement",
            "detail": str(e)
        })
        continue

    table_clean = nettoyer_nom_table(base)

    # Pour chaque colonne clé de DOCENTETE
    for col_base in colonnes_base:
        if col_base not in df_base.columns:
            log.append({
                "table_cible": base,
                "col_base": col_base,
                "col_cible": None,
                "statut": "col_base_absente",
                "detail": ""
            })
            continue

        # Pour chaque colonne de la table cible
        for col_cible in df_cible.columns:
            try:
                # 1. Calculer l'intersection des valeurs non-nulles
                valeurs_base  = set(df_base[col_base].dropna().astype(str))
                valeurs_cible = set(df_cible[col_cible].dropna().astype(str))
                inter = valeurs_base & valeurs_cible

                if not inter:
                    log.append({
                        "table_cible": base,
                        "col_base": col_base,
                        "col_cible": col_cible,
                        "statut": "sin_interseccion",
                        "detail": "0 valores comunes"
                    })
                    continue

                # 2. Filtrer les deux tables sur ces valeurs
                dfb = df_base[df_base[col_base].astype(str).isin(inter)].copy()
                dfc = df_cible[df_cible[col_cible].astype(str).isin(inter)].copy()

                # 3. Ajouter un rang pour chaque groupe de clé (rk)
                dfb["rk"] = dfb.groupby(col_base).cumcount()
                dfc["rk"] = dfc.groupby(col_cible).cumcount()

                # 4. Renommer la colonne de DOCENTETE pour qu'elle ait le même nom
                dfb.rename(columns={col_base: col_cible}, inplace=True)

                # 5. Merge sur [col_cible, rk] pour un emparement 1-à-1
                df_joint = pd.merge(
                    dfb,
                    dfc,
                    on=[col_cible, "rk"],
                    how="inner",
                    suffixes=("_doc", "_cmp")
                ).drop(columns="rk")

                # 6. Sauvegarde du résultat
                nom_csv = f"intersect_{table_clean}_{col_cible}.csv"
                df_joint.to_csv(os.path.join(dossier_resultats, nom_csv), index=False)

                log.append({
                    "table_cible": base,
                    "col_base": col_base,
                    "col_cible": col_cible,
                    "statut": "reussite",
                    "detail": (f"{len(inter)} valores comunes → "
                               f"{df_joint.shape[0]} filas, {df_joint.shape[1]} columnas")
                })

            except Exception as e:
                # En cas d'erreur, on enregistre et on continue
                log.append({
                    "table_cible": base,
                    "col_base": col_base,
                    "col_cible": col_cible,
                    "statut": "erreur_intersection",
                    "detail": str(e)
                })
                continue

# --- Écriture du log global ---
df_log = pd.DataFrame(log)
df_log.to_csv(os.path.join(dossier_resultats, "log_intersections.csv"), index=False)

print(f"Traitement terminé. Résultats et log dans {dossier_resultats}")
