#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import getpass
from pathlib import Path
from postgrest import APIError
import pandas as pd
from supabase import create_client, Client

# --- Configuration Standard ---
try:
    from src.outils.chemins import dossier_datalake_processed, dossier_config
except ImportError:
    projet_root = Path(__file__).resolve().parents[2]
    import sys
    sys.path.insert(0, str(projet_root))
    from src.outils.chemins import dossier_datalake_processed, dossier_config

# --- DÉFINITION CENTRALE DE LA "TRADUCTION" CSV -> BDD ---
TABLE_CONFIGS = {
    # === Schéma Ventes ===
    'dim_client': {
        'schema': 'ventes', 'natural_key_db': 'code_client',
        'rename_map': None,
        # CORRIGÉ : Ajout de 'dim_client_id'
        'final_db_columns': ['dim_client_id', 'code_client', 'raison_sociale', 'famille_client', 'responsable_dossier', 'representant']
    },
    'dim_famillesarticles': {
        'schema': 'ventes', 'natural_key_db': 'code_famille',
        'rename_map': None,
        # CORRIGÉ : Ajout de 'id_famille'
        'final_db_columns': ['id_famille', 'code_famille', 'libelle_famille', 'libelle_sous_famille']
    },
    'dim_article_ventes': {
        'table_name': 'dim_article', 'schema': 'ventes', 'natural_key_db': 'code_article',
        'rename_map': None,
        # CORRECT : 'dim_article_id' était déjà présent
        'final_db_columns': ['dim_article_id', 'code_article', 'numero_plan', 'ref_article_client', 'designation', 'id_famille']
    },
    'dim_temps': {
        'schema': 'ventes', 'natural_key_db': 'date_cle',
        'rename_map': None,
        # CORRIGÉ : Ajout de 'dim_temps_id'
        'final_db_columns': ['dim_temps_id', 'date_cle', 'annee', 'mois', 'jour']
    },
    'fact_ventes': {
        'schema': 'ventes', 'natural_key_db': 'dl_no', # Assurez-vous que 'dl_no' est unique par ligne ou envisagez une clé composite.
        'rename_map': None,
        # CORRECT : Les tables de faits n'ont pas leur propre ID, seulement des clés étrangères (qui étaient déjà présentes)
        'final_db_columns': ['dl_no', 'num_cde', 'date_bl', 'num_bl', 'condition_livraison', 'date_demandee_client', 'date_accusee_amco', 'num_facture', 'date_facture', 'qte_vendue', 'prix_unitaire', 'montant_ht', 'dim_client_id', 'dim_article_id', 'dim_temps_id']
    },
    
    # === Schéma Achats ===
    'dim_fournisseur': {
        'schema': 'achats', 'natural_key_db': 'ct_numpayeur',
        'rename_map': {'code_fournisseur': 'ct_numpayeur'},
        # CORRIGÉ : Ajout de 'fournisseur_id'
        'final_db_columns': ['fournisseur_id', 'ct_numpayeur', 'raison_sociale', 'contact', 'adresse', 'complement', 'code_postal', 'ville', 'telephone', 'fax']
    },
    'dim_famille_article': {
        'schema': 'achats', 'natural_key_db': 'fa_codef',
        'rename_map': {'code_famille': 'fa_codef', 'libelle_famille': 'fa_central', 'libelle_sous_famille': 'fa_intitule'},
        # CORRIGÉ : Ajout de 'famille_id'
        'final_db_columns': ['famille_id', 'fa_codef', 'fa_central', 'fa_intitule']
    },
    
    'dim_article_achats': {
        'table_name': 'dim_article', 
        'schema': 'achats', 
        'natural_key_db': 'ar_ref',
        'rename_map': {
            # On ne mappe QUE les colonnes qui existent dans le CSV et la table de destination
            'ar_ref': 'ar_ref', 
            'ar_designation': 'ar_designation', # Cette colonne existe bien dans dim_article
            'famille_id': 'famille_id'
        },
        
        'final_db_columns': [
            'article_id', 
            'ar_ref', 
            'ar_designation', # On garde la désignation qui appartient bien à l'article
            'famille_id'      # On garde la clé étrangère vers la table des familles
        ]
    },

    'dim_date': {
        'schema': 'achats', 'natural_key_db': 'date_full',
        'rename_map': None,
        # CORRIGÉ : Ajout de 'date_id'
        'final_db_columns': ['date_id', 'date_full', 'annee', 'mois', 'jour', 'trimestre']
    },
    'dim_mode_expedition': {
        'schema': 'achats', 'natural_key_db': 'code_expedit',
        'rename_map': None,
        # CORRIGÉ : Ajout de 'mode_id'
        'final_db_columns': ['mode_id', 'code_expedit', 'libelle']
    },
    'docligne':{
        'schema': 'achats', 'natural_key_db': 'dl_piece', # En supposant que c'est la clé naturelle.
        'rename_map': None,
        # CORRIGÉ : Ajout de 'docligne_id' (En supposant que cette table a une PK générée)
        # Si 'docligne' n'a pas son propre ID et n'est qu'une table de passage, vous pouvez retirer 'docligne_id'
        'final_db_columns': ['docligne_id', 'dl_piece', 'dl_design', 'fa_codef', 'fa_central', 'fa_intitule']
    },
    # En tu diccionario TABLE_CONFIGS
    'fact_achats': {
        'schema': 'achats',
        # Clave de conflicto que coincide con la nueva PRIMARY KEY de la tabla normal
        'natural_key_db': 'date_id,bon_de_commande', 
        'rename_map': None,
        # La lista completa de columnas que tu script intentará cargar
        'final_db_columns': [
            'date_id', 
            'fournisseur_id', 
            'docligne_id', 
            'article_id', 
            'mode_id', 
            'do_ref', 
            'bon_de_commande', 
            'qte_fact', 
            'total_tva', 
            'total_ht', 
            'total_ttc', 
            'net_a_payer'
        ]
    }
}

def load_supabase_config() -> dict:
    cfg_file = dossier_config / "supabase_config.json"
    if cfg_file.exists():
        try:
            config = json.loads(cfg_file.read_text(encoding="utf-8"))
            if config.get("url") and config.get("key"):
                 use = input(f"{cfg_file.name} détecté. L’utiliser ? (o/n) : ").strip().lower()
                 if use.startswith("o"): return config
        except json.JSONDecodeError: print(f"AVERTISSEMENT: {cfg_file.name} est corrompu.")
    print("Création de supabase_config.json")
    url = input("URL Supabase : ").strip()
    key = getpass.getpass("Clé API : ").strip()
    conf = {"url": url, "key": key}
    dossier_config.mkdir(exist_ok=True)
    cfg_file.write_text(json.dumps(conf, indent=2), encoding="utf-8")
    return conf

def connect_supabase(conf: dict) -> Client:
    return create_client(conf["url"], conf["key"])

def upload_table(supabase: Client, config_key: str):
    """Charge une table en utilisant sa configuration définie dans TABLE_CONFIGS."""
    config = TABLE_CONFIGS.get(config_key)
    if not config:
        print(f"AVERTISSESEMENT: Clé de configuration '{config_key}' non trouvée. Ignorée.")
        return

    table_name = config.get('table_name', config_key)
    schema = config['schema']
    
    # --- Carga de datos y preparación ---
    subfolder = schema
    csv_path = dossier_datalake_processed / subfolder / f"{table_name}.csv"
    
    print(f"Traitement de {csv_path} vers la table {schema}.{table_name}...")
    if not csv_path.exists():
        print(f"  AVERTISSEMENT : Fichier non trouvé. Étape ignorée.")
        return

    df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig").replace('', pd.NA).where(pd.notnull, None)

    if 'rename_map' in config and config['rename_map']:
        df.rename(columns=config['rename_map'], inplace=True)
        
    final_cols = config['final_db_columns']
    cols_existantes_dans_df = [col for col in final_cols if col in df.columns]
    df_to_upload = df[cols_existantes_dans_df]

    # --- Limpieza de duplicados ANTES de la carga ---
    # Esto es crucial ahora que no usaremos ON CONFLICT
    if 'natural_key_db' in config:
        key_cols_config = config['natural_key_db']
        key_cols_for_cleaning = key_cols_config.split(',') if isinstance(key_cols_config, str) and ',' in key_cols_config else [key_cols_config]
        
        print(f"  Nettoyage des doublons basé sur la clé : {key_cols_for_cleaning}")
        original_rows = len(df_to_upload)
        df_to_upload.dropna(subset=key_cols_for_cleaning, inplace=True)
        df_to_upload.drop_duplicates(subset=key_cols_for_cleaning, keep='first', inplace=True)
        rows_after_cleaning = len(df_to_upload)
        if original_rows > rows_after_cleaning:
            print(f"  INFO : {original_rows - rows_after_cleaning} doublons ont été supprimés.")

    if df_to_upload.empty:
        print(f"  INFO : Aucune donnée valide à charger pour {table_name}.")
        return

    records = df_to_upload.to_dict(orient='records')
    
    try:
        # --- CAMBIO DE ESTRATEGIA ---
        # Si es fact_achats, hacemos un INSERT simple. Para las demás, un UPSERT.
        if config_key == 'fact_achats':
            print("  → Stratégie : INSERT simple (sans ON CONFLICT).")
            supabase.schema(schema).table(table_name).insert(records).execute()
        else:
            print(f"  → Stratégie : UPSERT (avec ON CONFLICT).")
            natural_key_config = config['natural_key_db']
            on_conflict_cols = natural_key_config.split(',') if isinstance(natural_key_config, str) and ',' in natural_key_config else natural_key_config
            supabase.schema(schema).table(table_name).upsert(records, on_conflict=on_conflict_cols).execute()

        print(f"  → Succès : {len(records)} enregistrements traités pour {schema}.{table_name}.")

    except APIError as e:
        print(f"  ERREUR lors de l'opération pour {schema}.{table_name}: {e}")
        if records: 
            print(f"  Exemple de ligne: {records[0]}")
        raise e
        
def main():
    """Fonction principale pour orchestrer le chargement des données."""
    conf = load_supabase_config()
    supabase = connect_supabase(conf)

    # Ordre de chargement explicite pour gérer les dépendances de clés étrangères
    ordre_chargement_ventes = [
        "dim_client", "dim_temps", "dim_famillesarticles", 
        "dim_article_ventes", # Utilise la clé unique de config
        "fact_ventes"
    ]
    ordre_chargement_achats = [
        "dim_date", "dim_fournisseur", "dim_mode_expedition", "dim_famille_article",
        "dim_article_achats", # Utilise la clé unique de config
        "docligne",
        "fact_achats"
    ]

    print("\n--- DÉBUT DU CHARGEMENT DU SCHÉMA 'VENTES' ---")
    for config_key in ordre_chargement_ventes:
        upload_table(supabase, config_key)

    print("\n--- DÉBUT DU CHARGEMENT DU SCHÉMA 'ACHATS' ---")
    for config_key in ordre_chargement_achats:
        upload_table(supabase, config_key)

    print("\n→ Chargement en modèle étoile terminé avec succès !")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nL'OPÉRATION A ÉCHOUÉ. Erreur non capturée : {e}")