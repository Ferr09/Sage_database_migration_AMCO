#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import subprocess
import venv
import json

def ensure_venv():
    """
    Crée et active un venv local (« .venv ») si aucun venv n'est détecté.
    Relance le script dans le venv.
    """
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        return
    chemin_venv = os.path.join(os.getcwd(), '.venv')
    if not os.path.isdir(chemin_venv):
        print("Création du venv dans .venv...")
        venv.create(chemin_venv, with_pip=True)
    # Activation et relance
    if os.name == 'nt':
        activate = os.path.join(chemin_venv, 'Scripts', 'activate_this.py')
    else:
        activate = os.path.join(chemin_venv, 'bin', 'activate_this.py')
    exec(open(activate).read(), {'__file__': activate})
    os.execv(sys.executable, [sys.executable] + sys.argv)

def installer_requirements():
    """
    Installe les dépendances depuis requirements.txt.
    """
    fichier_req = os.path.join(os.getcwd(), "requirements.txt")
    if os.path.isfile(fichier_req):
        print("Installation des dépendances depuis requirements.txt...")
        result = subprocess.run([sys.executable, "-m", "pip", "install", "-r", fichier_req])
        if result.returncode != 0:
            print("Erreur : échec de l'installation des dépendances.", file=sys.stderr)
            sys.exit(result.returncode)

def verifier_driver_access():
    """
    Vérifie la présence du driver Access via pyodbc.
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

def verifier_fichier(path_fichier):
    """
    Vérifie que le fichier spécifié existe.
    """
    return os.path.isfile(path_fichier)

def demander_chemin(prompt):
    """
    Demande interactivement une entrée utilisateur avec un prompt donné.
    """
    return input(f"{prompt}").strip()

def lancer_module(command):
    """
    Lance un module Python via subprocess et interrompt si erreur.
    """
    result = subprocess.run(command)
    if result.returncode != 0:
        raise RuntimeError(f"Échec du module : {' '.join(command)}")

def config_postgres():
    """
    Demande les paramètres PostgreSQL et crée ou recrée un fichier config.json.
    """
    cfg_path = os.path.join(os.getcwd(), 'config.json')
    if os.path.isfile(cfg_path):
        overwrite = input("Un config.json existe déjà. Supprimer et recréer ? (oui/non) : ").strip().lower()
        if overwrite in ('oui', 'o'):
            os.remove(cfg_path)
            print("Ancien config.json supprimé.")
        else:
            print("Utilisation du config.json existant.")
            return True

    choix = input("Souhaitez-vous injecter les données dans PostgreSQL ? (oui/non) : ").strip().lower()
    if choix not in ('oui', 'o'):
        return False

    cfg = {
        'host':     input("Hôte PostgreSQL (ex: localhost) : ").strip(),
        'port':     input("Port (ex: 5432) : ").strip(),
        'dbname':   input("Nom de la base : ").strip(),
        'user':     input("Utilisateur : ").strip(),
        'password': input("Mot de passe : ").strip()
    }
    with open(cfg_path, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, indent=4)
    print("config.json créé.")
    return True

def main():
    # 1. venv et dépendances
    ensure_venv()
    installer_requirements()

    # 2. Lecture optionnelle du README
    lire_readme = input("Voulez-vous consulter le README avant de continuer ? (oui/non) : ").strip().lower()
    if lire_readme in ('oui', 'o'):
        print("Ouvrez README.md et relancez ce script une fois prêt.")
        sys.exit(0)

    # 3. Choix de l'étape de départ
    print("Choisissez une option :")
    print("1. Exporter la base Access vers fichiers intermédiaires")
    print("2. Charger directement dans PostgreSQL (si fichiers déjà générés)")
    choix_etape = input("Votre choix (1/2) : ").strip()

    # 4. Étape 1 : export Access
    if choix_etape == '1':
        verifier_driver_access()
        parser = argparse.ArgumentParser(description="Pipeline Access - chemin facultatif")
        parser.add_argument("-a", "--access-file", help="Chemin vers .accdb")
        args = parser.parse_args()
        if args.access_file and verifier_fichier(args.access_file):
            path_access = args.access_file
        else:
            default = os.path.join(os.getcwd(), "tables_sage_hyperix.accdb")
            if os.path.isfile(default) and verifier_fichier(default):
                path_access = default
                print(f"Fichier .accdb trouvé : {default}")
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
        lancer_module([sys.executable, "src/modules/extraction_complete_fichiers_csv.py"])
        lancer_module([sys.executable, "src/modules/extraction_entetes.py"])
        lancer_module([sys.executable, "src/outils/generer_statistiques_tables.py"])
        lancer_module([sys.executable, "src/modules/nettoyage_fichiers_csv.py"])

    # 5. Étape 2 : injection PostgreSQL
    while True:
        if not config_postgres():
            print("Fin sans injection PostgreSQL.")
            break
        try:
            lancer_module([sys.executable, "construction_bdd_sql.py"])
            print("Injection réussie.")
            break
        except RuntimeError as e:
            print(f"Erreur injection : {e}")
            retry = input("Recréer le config.json et réessayer ? (oui/non) : ").strip().lower()
            if retry not in ('oui', 'o'):
                print("Abandon injection.")
                break

if __name__ == "__main__":
    main()
