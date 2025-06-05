#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import subprocess
import venv
import json

# --------------------------------------------------------------------
# Fonctions utilitaires pour la gestion du venv et des requirements
# --------------------------------------------------------------------

def ensure_venv():
    venv_dir = '.venv'
    activate_script = os.path.join(venv_dir, 'Scripts', 'activate_this.py')

    if not os.path.isdir(venv_dir):
        os.system(f'{sys.executable} -m venv {venv_dir}')
        print("Environnement virtuel créé.")
    else:
        print("Environnement virtuel déjà présent.")

    version = sys.version_info
    if version >= (3, 8):
        print("Python 3.8 ou version ultérieure détectée : aucune activation automatique requise.")
        return
    else:
        if os.path.exists(activate_script):
            print("Activation de l’environnement virtuel (Python < 3.8)...")
            exec(open(activate_script).read(), {'__file__': activate_script})
        else:
            print("Attention : 'activate_this.py' est introuvable et requis pour Python < 3.8.")
            sys.exit("Arrêt du script pour éviter une exécution dans un environnement non activé.")

def mise_a_jour_systeme():
    subprocess.run([sys.executable, "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])

def installer_requirements_extraction():
    """
    Installe uniquement les dépendances nécessaires à l'extraction depuis Access.
    """
    fichier_req = "requirements_extraction.txt"
    if os.path.isfile(fichier_req):
        print("Installation des dépendances pour l’extraction (requirements_extraction.txt)...")
        result = subprocess.run([sys.executable, "-m", "pip", "install", "-r", fichier_req])
        if result.returncode != 0:
            print("Erreur : échec de l'installation des dépendances d'extraction.", file=sys.stderr)
            sys.exit(result.returncode)
    else:
        print("Fichier requirements_extraction.txt introuvable. Impossible d’installer les dépendances d’extraction.", file=sys.stderr)
        sys.exit(1)

def installer_requirements_chargement(db_type: str):
    """
    Installe les dépendances nécessaires au chargement dans la base choisie :
    - 'postgresql' -> requirements_postgres.txt
    - 'mysql'      -> requirements_mysql.txt
    """
    if db_type == "postgresql":
        fichier_req = "requirements_postgres.txt"
    elif db_type == "mysql":
        fichier_req = "requirements_mysql.txt"
    else:
        print(f"Type de base non géré : {db_type}", file=sys.stderr)
        sys.exit(1)

    if os.path.isfile(fichier_req):
        print(f"Installation des dépendances pour le chargement ({fichier_req})...")
        result = subprocess.run([sys.executable, "-m", "pip", "install", "-r", fichier_req])
        if result.returncode != 0:
            print("Erreur : échec de l'installation des dépendances de chargement.", file=sys.stderr)
            sys.exit(result.returncode)
    else:
        print(f"Fichier {fichier_req} introuvable. Impossible d’installer les dépendances de chargement.", file=sys.stderr)
        sys.exit(1)

# --------------------------------------------------------------------
# Fonctions existantes (légèrement adaptées)
# --------------------------------------------------------------------

def afficher_readme():
    readme_path = "README.md"
    if not os.path.exists(readme_path):
        print("Le fichier README.md est introuvable.")
        return
    with open(readme_path, "r", encoding="utf-8") as f:
        contenu = f.read()
        print("\n" + "="*50)
        print(" CONTENU DU README ")
        print("="*50)
        print(contenu)
        print("="*50 + "\n")

def verifier_driver_access():
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

def verifier_fichier(path_fichier):
    return os.path.isfile(path_fichier)

def demander_chemin(prompt):
    return input(f"{prompt}").strip()

def lancer_module(command):
    result = subprocess.run(command)
    if result.returncode != 0:
        raise RuntimeError(f"Échec du module : {' '.join(command)}")

def config_bdd():
    """
    Demande à l'utilisateur le type de base (postgresql ou mysql) et construit
    le fichier de configuration JSON correspondant. Retourne (db_type, True/False).
    """
    cfg_dir = os.path.join(os.getcwd(), 'config')
    os.makedirs(cfg_dir, exist_ok=True)

    choix_type = input("Choisissez le type de base pour l’injection (postgresql/mysql) : ").strip().lower()
    if choix_type not in ("postgresql", "mysql"):
        print("Type invalide. Fin.")
        return None, False

    nom_fichier = "postgres_config.json" if choix_type == "postgresql" else "mysql_config.json"
    cfg_path = os.path.join(cfg_dir, nom_fichier)
    if os.path.isfile(cfg_path):
        overwrite = input(f"Un {nom_fichier} existe déjà. Supprimer et recréer ? (oui/non) : ").strip().lower()
        if overwrite in ('oui', 'o'):
            os.remove(cfg_path)
            print(f"Ancien {nom_fichier} supprimé.")
        else:
            print(f"Utilisation du {nom_fichier} existant.")
            return choix_type, True

    print(f"--- Configuration pour {choix_type} ---")
    cfg = {
        'db_host':     input("Hôte (ex: localhost) : ").strip(),
        'db_port':     input("Port (ex: 5432 ou 3306) : ").strip(),
        'db_name':     input("Nom de la base : ").strip(),
        'db_user':     input("Utilisateur : ").strip(),
        'db_password': input("Mot de passe : ").strip()
    }

    with open(cfg_path, 'w', encoding="utf-8") as f:
        json.dump(cfg, f, indent=4)
    print(f"{nom_fichier} créé.")
    return choix_type, True

# --------------------------------------------------------------------
# Fonction principale
# --------------------------------------------------------------------

def main():
    # 1. Création du venv et mise à jour pip/setuptools/wheel
    ensure_venv()
    mise_a_jour_systeme()

    # 2. Installation des dépendances d'extraction uniquement
    installer_requirements_extraction()

    # 3. Lecture optionnelle du README
    lire_readme = input("Voulez-vous consulter le README avant de continuer ? (oui/non) : ").strip().lower()
    if lire_readme in ('oui', 'o'):
        afficher_readme()
        input("Appuyez sur Entrée pour continuer...")

    # 4. Choix de l'étape de départ
    print("Choisissez une option :")
    print("1. Exporter la base Access vers fichiers intermédiaires (CSV/Excel)")
    print("2. Charger directement dans une base (si fichiers déjà générés)")
    choix_etape = input("Votre choix (1/2) : ").strip()

    # === Étape 1 : export Access ===
    if choix_etape == '1':
        verifier_driver_access()
        parser = argparse.ArgumentParser(description="Pipeline Access - chemin facultatif")
        parser.add_argument("-a", "--access-file", help="Chemin vers .accdb")
        args = parser.parse_args()
        if args.access_file and verifier_fichier(args.access_file):
            path_access = args.access_file
        else:
            dossier_access = os.path.join(os.getcwd(), "db_sage_access")
            if os.path.isdir(dossier_access):
                fichiers = [f for f in os.listdir(dossier_access) if f.endswith(".accdb")]
                if fichiers:
                    path_access = os.path.join(dossier_access, fichiers[0])
                    print(f"Fichier .accdb trouvé : {path_access}")
                else:
                    print("Aucun fichier .accdb trouvé dans db_sage_access.")
                    sys.exit(1)
            else:
                print("Aucune configuration trouvée. Tapez 'sortir' pour abandonner.")
                while True:
                    chemin = demander_chemin("Chemin vers .accdb : ")
                    if chemin.lower() == 'sortir':
                        print("Sortie.")
                        sys.exit(0)
                    if verifier_fichier(chemin):
                        path_access = chemin
                        break
        os.environ["ACCESS_FILE"] = os.path.abspath(path_access)
        lancer_module([sys.executable, "src/modules/extraction_complete_access.py"])
        lancer_module([sys.executable, "src/modules/extraction_entetes.py"])
        lancer_module([sys.executable, "src/outils/generer_statistiques_tables.py"])
        lancer_module([sys.executable, "src/modules/nettoyage_fichiers_csv.py"])
        print("\nExtraction terminée. Vous pouvez maintenant lancer l’étape 2 pour l’injection.")

    # === Étape 2 : injection vers une base (PostgreSQL ou MySQL) ===
    while True:
        db_type, config_ok = config_bdd()
        if not config_ok or db_type is None:
            print("Fin sans injection en base.")
            break

        # 2.1. Installer les dépendances de chargement en fonction du type choisi
        installer_requirements_chargement(db_type)

        # 2.2. Lancer le script de construction de la BDD
        try:
            # On passe le type de base en argument pour choisir le driver dans construction_bdd_sql.py
            lancer_module([sys.executable, "src/modules/construction_bdd_sql.py", "--db-type", db_type])
            print("Injection réussie.")
            break
        except RuntimeError as e:
            print(f"Erreur injection : {e}")
            retry = input("Recréer le fichier de configuration et réessayer ? (oui/non) : ").strip().lower()
            if retry not in ('oui', 'o'):
                print("Abandon injection.")
                break

if __name__ == "__main__":
    main()
