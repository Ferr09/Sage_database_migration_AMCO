#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Point d’entrée principal pour le pipeline ETL orienté Supabase (PostgreSQL).

Interactions :
1. Afficher optionnellement le README
2. Choisir l’étape à exécuter :
   1) Pipeline ETL complet
   2) Extraction seule
   3) Transformation seule
   4) Chargement seule
   5) Quitter
3. Boucler ou quitter
"""

import os
import sys
import subprocess
import json
import getpass
from pathlib import Path

from src.outils.chemins import (
    racine_projet,
    dossier_config,
    dossier_db_access,
    dossier_datalake_raw,
    dossier_datalake_staging,
    dossier_datalake_processed,
    chemin_requirements_extraction,
    chemin_requirements_supabase
)

# -----------------------------------------------------------------------------
# Fonctions utilitaires
# -----------------------------------------------------------------------------
def ensure_venv():
    venv_dir = racine_projet / ".venv"
    if not venv_dir.exists():
        print("Création de l’environnement virtuel…")
        os.system(f"{sys.executable} -m venv \"{venv_dir}\"")
    print("Mise à jour de pip, setuptools et wheel…")
    subprocess.run([sys.executable, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"], check=True)

def install_requirements(path: Path):
    if not path.is_file():
        print(f"Fichier de dépendances introuvable : {path}")
        sys.exit(1)
    print(f"Installation des dépendances depuis {path.name}…")
    subprocess.run([sys.executable, "-m", "pip", "install", "-r", str(path)], check=True)

def run_module(module: str, args: list = None):
    cmd = [sys.executable, "-m", module] + (args or [])
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise RuntimeError(f"Module {module} a échoué (code {result.returncode})")

def show_readme():
    readme = racine_projet / "README.md"
    if not readme.exists():
        print("README.md non trouvé.")
        return
    print("\n" + "="*60)
    print(readme.read_text(encoding="utf-8"))
    print("="*60 + "\n")
    input("Appuyez sur Entrée pour continuer…")

def ensure_supabase_config():
    """
    Vérifie l'existence de supabase_config.json.
    Si présent, demande si on réutilise ; sinon, recrée.
    Si absent, crée le fichier en demandant les infos.
    """
    cfg_file = dossier_config / "supabase_config.json"
    dossier_config.mkdir(exist_ok=True)
    if cfg_file.exists():
        print(f"⚙️  Fichier {cfg_file.name} détecté.")
        if input("Voulez-vous l’utiliser ? (o/n) : ").strip().lower() in ("o","oui"):
            return
        print(f"Suppression de l’ancien {cfg_file.name}…")
        cfg_file.unlink()
    # création du fichier
    print("Configuration Supabase/PostgreSQL :")
    cfg = {
        "db_host":   input("Hôte (ex : db.supabase.co) : ").strip(),
        "db_port":   input("Port (par défaut 5432) : ").strip() or "5432",
        "db_name":   input("Nom de la base : ").strip(),
        "db_user":   input("Utilisateur (developer) : ").strip(),
        "db_password": getpass.getpass("Mot de passe : ").strip()
    }
    with open(cfg_file, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=4)
    print(f"✅  {cfg_file.name} créé.")

def load_supabase_config():
    cfg_file = dossier_config / "supabase_config.json"
    if not cfg_file.exists():
        ensure_supabase_config()
    with open(cfg_file, "r", encoding="utf-8") as f:
        return json.load(f)

def check_access_file():
    cand = list(dossier_db_access.glob("*.accdb"))
    if cand:
        print(f"Base Access trouvée : {cand[0].name}")
        return cand[0]
    print("Aucun .accdb dans db_sage_access.")
    path = input("Chemin vers .accdb ou 'sortir' : ").strip()
    if path.lower()=="sortir":
        sys.exit(0)
    p = Path(path)
    if not p.is_file():
        print("Fichier introuvable.")
        return check_access_file()
    return p

# -----------------------------------------------------------------------------
# Séquences de pipeline
# -----------------------------------------------------------------------------
def etl_complete():
    extraction()
    transformation()
    chargement()

def extraction():
    print("=== Extraction ===")
    install_requirements(chemin_requirements_extraction)
    accdb = check_access_file()
    os.environ["ACCESS_FILE"] = str(accdb.resolve())
    run_module("src.extraction.extraction_complete_access")
    run_module("src.extraction.extraction_entetes")

def transformation():
    print("=== Transformation ===")
    run_module("src.staging.nettoyage_fichiers_bruts_sage")
    run_module("src.chargement.vers_csv")
    run_module("src.transformation.structuration_etoile")

def chargement():
    print("=== Chargement en Supabase/PostgreSQL ===")
    cfg = load_supabase_config()
    install_requirements(chemin_requirements_supabase)
    # On transmet les identifiants en variable d'env pour le module charger_supabase
    os.environ["SUPABASE_HOST"]     = cfg["db_host"]
    os.environ["SUPABASE_PORT"]     = cfg["db_port"]
    os.environ["SUPABASE_DB"]       = cfg["db_name"]
    os.environ["SUPABASE_USER"]     = cfg["db_user"]
    os.environ["SUPABASE_PASSWORD"] = cfg["db_password"]
    run_module("src.chargement.vers_bdd")

# -----------------------------------------------------------------------------
# Main interactif
# -----------------------------------------------------------------------------
def main():
    ensure_venv()
    while True:
        if input("Afficher le manuel (README) ? (o/n) : ").strip().lower() in ("o","oui"):
            show_readme()

        print("\nÉtapes disponibles :")
        print("1) Pipeline ETL complet")
        print("2) Extraction seule")
        print("3) Transformation seule")
        print("4) Chargement seule")
        print("5) Quitter")
        choix = input("Votre choix (1-5) : ").strip()

        try:
            if choix == "1":
                etl_complete()
            elif choix == "2":
                extraction()
            elif choix == "3":
                transformation()
            elif choix == "4":
                chargement()
            elif choix == "5":
                print("Fin du programme.")
                break
            else:
                print("Choix invalide.")
                continue
        except Exception as e:
            print(f"Erreur : {e}", file=sys.stderr)

        if input("\nRevenir au menu principal ? (o/n) : ").strip().lower() not in ("o","oui"):
            print("Fin du programme.")
            break

if __name__ == "__main__":
    main()
