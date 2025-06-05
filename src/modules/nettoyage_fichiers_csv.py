import os
import pandas as pd
import re

# === Dossiers ===
dossier_source = r"extraits/csv_extraits"  # Dossier contenant les .csv        
dossier_sortie = r"extraits/xlsx_propres"             # Dossier pour stocker les .xlsx

# Vérification du dossier source
if not os.path.isdir(dossier_source):
    raise FileNotFoundError(f"Le dossier source n'existe pas : {os.path.abspath(dossier_source)}")

# Création du dossier de sortie s’il n’existe pas
os.makedirs(dossier_sortie, exist_ok=True)



# === Configuration facultative (types, monnaies, dtype personnalisés)
types_tables = {
    "F_DOCENTETE": {
        "DO_DATE": "datetime",
        "DO_DATELIVR": "datetime",
        "FNT_TOTALHT": "float"
    },
    "F_DOCLIGNE": {
        "DL_DATEBL": "datetime",
        "DL_DATEBC": "datetime",
        "DL_DATEPL": "datetime",
        "DL_QTE": "float",
        "DL_PRIXUNITAIRE": "float",
        "DL_MONTANTHT": "float"
    }
}

colonnes_monnaie_tables = {
    "F_DOCENTETE": ["FNT_TOTALHT"],
    "F_DOCLIGNE": ["DL_PRIXUNITAIRE", "DL_MONTANTHT"]
}

dtype_tables = {
    "F_DOCLIGNE": {
        "AC_REFCLIENT": str,
        "AF_REFFOURNISS": str
    }
}


def convertir_col_excel(index):
    """Convertit un index de colonne (0-based) en lettre Excel (A, B, ..., AA, AB...)"""
    result = ""
    while index >= 0:
        result = chr(index % 26 + ord('A')) + result
        index = index // 26 - 1
    return result

# === Fonction principale
def nettoyer_et_exporter_csv(chemin_csv, nom_table):
    try:
        # Lecture CSV
        dtype = dtype_tables.get(nom_table, None)
        df = pd.read_csv(chemin_csv, encoding="utf-8-sig", dtype=dtype, low_memory=False)

        # Nettoyage : suppression lignes vides ou nulles/0
        df_clean = df.dropna(how='all')
        df_clean = df_clean.loc[~(df_clean.isna() | (df_clean == 0)).all(axis=1)]

        # Cas particulier : on supprime les lignes sans AF_REFFOURNISS
        if nom_table == "F_ARTFOURNISS":
            before = len(df_clean)
            df_clean = df_clean[df_clean["AF_REFFOURNISS"].notna()]
            removed = before - len(df_clean)
            print(f"{nom_table} : {removed} ligne(s) sans AF_REFFOURNISS supprimée(s)")


        # Cas particulier : F_DOCLIGNE extraction du n° de BL
        if nom_table == "F_DOCLIGNE":
            if "DL_PIECEBL" in df_clean.columns and "DL_DESIGN" in df_clean.columns:
                def extraire_bl(row):
                    val = row["DL_PIECEBL"]
                    texte = str(row["DL_DESIGN"]).replace('\xa0', ' ')
                    texte = re.sub(r"\s+", " ", texte).strip()
                    if pd.isna(val) or str(val).strip() == "":
                        match = re.search(r"LIVREE?S?\s+PAR\s+BL\s*(?:N°?|N)?\s*(\d+)", texte, re.IGNORECASE)
                        if match:
                            return match.group(1)
                    return val
                ancienne = df_clean["DL_PIECEBL"].copy()
                df_clean["DL_PIECEBL"] = df_clean.apply(extraire_bl, axis=1)
                lignes_modifiees = (ancienne.isna() | (ancienne == "")) & (ancienne != df_clean["DL_PIECEBL"])
                print(f"🔧 {nom_table} : {lignes_modifiees.sum()} ligne(s) mises à jour automatiquement dans DL_PIECEBL")

        # Application des types si définis
        types = types_tables.get(nom_table, None)
        if types:
            for col, dtype in types.items():
                if col in df_clean.columns:
                    try:
                        if dtype == "datetime":
                            df_clean[col] = pd.to_datetime(df_clean[col], errors="coerce")
                        else:
                            df_clean[col] = df_clean[col].astype(dtype)
                    except Exception as e:
                        print(f"Erreur de conversion dans {nom_table}.{col} : {e}")

        # Ne rien exporter si aucune ligne n'est présente après nettoyage
        if df_clean.shape[0] == 0:
            print(f"Ignoré : {nom_table} (aucune ligne après nettoyage)")
            return

        # Export vers Excel
        chemin_excel = os.path.join(dossier_sortie, f"{nom_table}_propre.xlsx")
        with pd.ExcelWriter(chemin_excel, engine="xlsxwriter") as writer:
            df_clean.to_excel(writer, index=False, sheet_name="Données")
            workbook = writer.book
            worksheet = writer.sheets["Données"]

            # Format monétaire si applicable
            colonnes_monnaie = colonnes_monnaie_tables.get(nom_table, [])
            if colonnes_monnaie:
                format_monnaie = workbook.add_format({'num_format': '#,##0.00 €'})
                for col in colonnes_monnaie:
                    if col in df_clean.columns:
                        idx = df_clean.columns.get_loc(col)
                        excel_col = convertir_col_excel(idx)
                        worksheet.set_column(f"{excel_col}:{excel_col}", 18, format_monnaie)

        print(f"Exporté : {nom_table} ({len(df_clean)} lignes)")

    except Exception as e:
        print(f"Erreur pour {nom_table} : {e}")

# === Exécution pour tous les CSV du dossier
fichiers = [f for f in os.listdir(dossier_source) if f.endswith(".csv")]
print(f"Détection de {len(fichiers)} fichiers CSV")

for fichier in fichiers:
    nom_table = os.path.splitext(fichier)[0]
    chemin_csv = os.path.join(dossier_source, fichier)
    nettoyer_et_exporter_csv(chemin_csv, nom_table)
