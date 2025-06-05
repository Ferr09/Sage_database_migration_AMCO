#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import subprocess
import venv
import json
from pathlib import Path

# --------------------------------------------------------------------
# 1. Importer la racine du projet et les chemins absolus via outils.chemins
# --------------------------------------------------------------------
# (on part du principe que src/outils/chemins.py définit racine_projet, dossier_config, etc.)
#
from src.outils.chemins import (
    racine_projet,
    dossier_config,
    dossier_modules,
    dossier_outils,
    dossier_db_access,
    dossier_csv_extraits,
    dossier_xlsx_propres
)

# --------------------------------------------------------------------
# 2. Fonctions utilitaires pour l'environnement virtuel et l'installation
# --------------------------------------------------------------------
def ensure_venv():
    """
    Crée l’environnement virtuel `.venv` à la racine du projet si inexistant.
    Pour Python ≥ 3.8, pas d’activation manuelle nécessaire.
    """
    venv_dir = racine_projet / ".venv"
    activate_script = venv_dir / "Scripts" / "activate_this.py"

    if not venv_dir.is_dir():
        # Création de l’environnement virtuel
        os.system(f"{sys.executable} -m venv \"{venv_dir}\"")
        print("Environnement virtuel créé.")
    else:
        print("Environnement virtuel déjà présent.")

    version = sys.version_info
    if version >= (3, 8):
        print("Python 3.8 ou version ultérieure détectée : aucune activation automatique requise.")
        return
    else:
        if activate_script.exists():
            print("Activation de l’environnement virtuel (Python < 3.8)…")
            exec(
                open(activate_script, "r", encoding="utf-8").read(),
                {"__file__": str(activate_script)}
            )
        else:
            print("Attention : 'activate_this.py' introuvable pour Python < 3.8.")
            sys.exit("Arrêt du script pour éviter une exécution hors venv.")

def mise_a_jour_systeme():
    """
    Met à jour pip, setuptools et wheel avant d’installer les dépendances.
    """
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"],
        check=True
    )

def installer_requirements():
    """
    Installe les dépendances globales du projet depuis requirements.txt à la racine.
    """
    fichier_req = racine_projet / "requirements.txt"
    if fichier_req.is_file():
        print("Installation des dépendances depuis requirements.txt…")
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "-r", str(fichier_req)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )
        if result.returncode != 0:
            print("Erreur : échec de l'installation des dépendances.", file=sys.stderr)
            print(result.stdout.decode("utf-8"), file=sys.stderr)
            sys.exit(result.returncode)

def installer_requirements_chargement(db_type: str):
    """
    Installe les dépendances nécessaires au chargement en base, selon db_type :
    - 'postgresql' → requirements_postgres.txt
    - 'mysql'      → requirements_mysql.txt
    """
    if db_type == "postgresql":
        fichier_req = racine_projet / "requirements_postgres.txt"
    elif db_type == "mysql":
        fichier_req = racine_projet / "requirements_mysql.txt"
    else:
        print(f"Type de base non géré : {db_type}", file=sys.stderr)
        sys.exit(1)

    if fichier_req.is_file():
        print(f"Installation des dépendances pour le chargement ({fichier_req.name})…")
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "-r", str(fichier_req)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )
        if result.returncode != 0:
            print("Erreur : échec de l’installation des dépendances de chargement.", file=sys.stderr)
            print(result.stdout.decode("utf-8"), file=sys.stderr)
            sys.exit(result.returncode)
    else:
        print(f"Fichier {fichier_req.name} introuvable dans {racine_projet}.", file=sys.stderr)
        sys.exit(1)

# --------------------------------------------------------------------
# 3. Fonctions de vérification / configuration
# --------------------------------------------------------------------
def verifier_driver_access():
    """
    Vérifie que le pilote ODBC pour Access est installé (pyodbc + driver Access).
    """
    try:
        import pyodbc
    except ImportError:
        print("Erreur : le module pyodbc n'est pas installé.", file=sys.stderr)
        sys.exit(1)

    drivers = [d.lower() for d in pyodbc.drivers()]
    cible = "microsoft access driver (*.mdb, *.accdb)"
    if not any(cible in d for d in drivers):
        print("Erreur : le driver Access n'est pas trouvé.", file=sys.stderr)
        sys.exit(1)

def verifier_fichier(path_fichier: Path) -> bool:
    """
    Retourne True si le fichier existe physiquement.
    """
    return path_fichier.is_file()

def demander_chemin(prompt: str) -> str:
    """
    Invite l’utilisateur à saisir un chemin, retourne la chaîne entrée.
    """
    return input(f"{prompt}").strip()

def config_bdd() -> (str, bool):  # type: ignore
    """
    Interroge l’utilisateur pour savoir s’il veut charger dans PostgreSQL ou MySQL,
    lit (ou crée) le fichier de configuration JSON adéquat.
    Renvoie (db_type, True) si config OK, (None, False) sinon.
    """
    # 1) Choix du type de base
    print("Dans quelle base souhaitez-vous injecter les données ?")
    print("1. PostgreSQL")
    print("2. MySQL")
    choix = input("Votre choix (1/2) : ").strip()

    if choix not in ("1", "2"):
        return None, False

    db_type = "postgresql" if choix == "1" else "mysql"
    config_filename = f"{db_type}_config.json"
    chemin_cfg = dossier_config / config_filename

    # 2) Si le config existe déjà, demande si on le recrée
    if chemin_cfg.exists():
        overwrite = input(
            f"Le fichier {config_filename} existe déjà. Le supprimer et recréer ? (oui/non) : "
        ).strip().lower()
        if overwrite in ("oui", "o"):
            chemin_cfg.unlink()
            print(f"Ancien {config_filename} supprimé.")
        else:
            print("Utilisation du fichier de configuration existant.")
            return db_type, True

    # 3) Sinon, on crée le JSON
    print(f"Création de {config_filename} pour {db_type}…")
    cfg = {}
    cfg["db_host"] = input("Hôte de la base (ex : localhost) : ").strip()
    default_port = "5432" if db_type == "postgresql" else "3306"
    port_saisi = input(f"Port (ex : {default_port}) : ").strip()
    cfg["db_port"] = port_saisi or default_port
    cfg["db_name"] = input("Nom de la base : ").strip()
    cfg["db_user"] = input("Utilisateur : ").strip()
    cfg["db_password"] = input("Mot de passe : ").strip()

    # S’assure que le répertoire de config existe
    if not dossier_config.exists():
        os.makedirs(dossier_config, exist_ok=True)

    with open(chemin_cfg, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=4)
    print(f"{config_filename} créé dans {dossier_config}.")
    return db_type, True

# --------------------------------------------------------------------
# 4. Fonction pour lancer un module externe EN MODE « - m »
# --------------------------------------------------------------------
def lancer_module(command: list):
    """
    Exécute un sous-processus (module Python ou script). 
    Lève RuntimeError si échec.
    """
    result = subprocess.run(command)
    if result.returncode != 0:
        raise RuntimeError(f"Échec du module : {' '.join(command)}")

# --------------------------------------------------------------------
# 5. Lecture optionnelle du README
# --------------------------------------------------------------------
def afficher_readme():
    """
    Affiche le contenu du README.md à la racine si l’utilisateur le souhaite.
    """
    readme_path = racine_projet / "README.md"
    if not readme_path.exists():
        print("Le fichier README.md est introuvable à la racine.")
        return
    contenu = readme_path.read_text(encoding="utf-8")
    print("\n" + "=" * 50)
    print(" CONTENU DU README ")
    print("=" * 50)
    print(contenu)
    print("=" * 50 + "\n")

# --------------------------------------------------------------------
# 6. Fonction principale
# --------------------------------------------------------------------
def main():
    # 1) Création et activation éventuelle du venv, mise à jour système, installation globale
    ensure_venv()
    mise_a_jour_systeme()
    installer_requirements()

    # 2) Lecture optionnelle du README
    lire_readme = input("Voulez-vous consulter le README avant de continuer ? (oui/non) : ").strip().lower()
    if lire_readme in ("oui", "o"):
        afficher_readme()
        input("Appuyez sur Entrée pour continuer…")

    # 3) Choix de l’étape de départ (1 : extraction Access → CSV, 2 : injection en base)
    print("Choisissez une option :")
    print("1. Exporter la base Access vers fichiers CSV intermédiaires")
    print("2. Charger directement dans la base (si fichiers déjà générés)")
    choix_etape = input("Votre choix (1/2) : ").strip()

    # ------------------------------------------------------------
    # 4) Étape 1 : extraction Access
    # ------------------------------------------------------------
    if choix_etape == "1":
        verifier_driver_access()

        # 4.a) Optionnel : argument -a ou --access-file
        parser = argparse.ArgumentParser(description="Pipeline Access – chemin facultatif")
        parser.add_argument("-a", "--access-file", help="Chemin vers le .accdb")
        args, _ = parser.parse_known_args()

        if args.access_file:
            path_access = Path(args.access_file)
            if not verifier_fichier(path_access):
                print(f"Le fichier {path_access} n'existe pas. Abandon.")
                sys.exit(1)
        else:
            # Chercher un .accdb dans le dossier_db_access
            if dossier_db_access.is_dir():
                liste_accdb = [f for f in dossier_db_access.iterdir() if f.suffix.lower() == ".accdb"]
                if liste_accdb:
                    path_access = liste_accdb[0]
                    print(f"Fichier .accdb trouvé : {path_access}")
                else:
                    # Aucune base trouvée, on demande à l’utilisateur
                    print("Aucun fichier .accdb trouvé dans", dossier_db_access)
                    while True:
                        chemin_saisi = Path(demander_chemin("Chemin vers .accdb : "))
                        if chemin_saisi.name.lower() == "sortir":
                            print("Sortie.")
                            sys.exit(0)
                        if verifier_fichier(chemin_saisi):
                            path_access = chemin_saisi
                            break
                        else:
                            print("Fichier introuvable, réessayez :")
            else:
                # Le dossier n’existe pas : on invite l’utilisateur
                print(f"Le dossier {dossier_db_access} n'existe pas. Tapez 'sortir' pour abandonner.")
                while True:
                    chemin_saisi = Path(demander_chemin("Chemin vers .accdb : "))
                    if chemin_saisi.name.lower() == "sortir":
                        print("Sortie.")
                        sys.exit(0)
                    if verifier_fichier(chemin_saisi):
                        path_access = chemin_saisi
                        break
                    else:
                        print("Fichier introuvable, réessayez :")

        # On stocke la variable d’environnement pour les modules suivants
        os.environ["ACCESS_FILE"] = str(path_access.resolve())

        # 4.b) Appel de chaque module VIA `-m src.modules.*` ou `-m src.outils.*`
        # ─────────────────────────────────────────────────────────────────
        print("Lancement du module : src.modules.extraction_complete_access…")
        lancer_module([
            sys.executable,
            "-m", "src.modules.extraction_complete_access"
        ])

        print("Lancement du module : src.modules.extraction_entetes…")
        lancer_module([
            sys.executable,
            "-m", "src.modules.extraction_entetes"
        ])

        print("Lancement du module : src.outils.generer_statistiques_tables…")
        lancer_module([
            sys.executable,
            "-m", "src.outils.generer_statistiques_tables"
        ])

        print("Lancement du module : src.modules.nettoyage_fichiers_csv…")
        lancer_module([
            sys.executable,
            "-m", "src.modules.nettoyage_fichiers_csv"
        ])

    # ------------------------------------------------------------
    # 5) Étape 2 : injection en base (PostgreSQL ou MySQL)
    # ------------------------------------------------------------
    while True:
        db_type, config_ok = config_bdd()
        if not config_ok or db_type is None:
            print("Fin sans injection en base.")
            break

        # 5.a) Installer seulement les dépendances de chargement
        installer_requirements_chargement(db_type)

        # 5.b) Lancer le module de construction de la BDD en MODE `-m src.modules.construction_bdd_sql`
        print(f"Lancement du module : src.modules.construction_bdd_sql – db_type = {db_type}")
        module_cci = "src.modules.construction_bdd_sql"
        try:
            lancer_module([
                sys.executable,
                "-m", module_cci,
                "--db-type", db_type
            ])
            print("Injection réussie.")
            break
        except RuntimeError as e:
            print(f"Erreur injection : {e}")
            retry = input("Recréer le fichier de configuration et réessayer ? (oui/non) : ").strip().lower()
            if retry not in ("oui", "o"):
                print("Abandon injection.")
                break

if __name__ == "__main__":
    main()
