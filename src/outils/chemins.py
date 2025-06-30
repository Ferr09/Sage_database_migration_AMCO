# -*- coding: utf-8 -*-
from pathlib import Path

# ------------------------------------------------------------------------------
# Ce fichier doit être nommé « chemins.py » et placé dans le dossier :
#    mon_projet/src/outils/chemins.py
#
# L’idée est que __file__ pointe toujours vers ce code source, et que parents[n]
# remonte jusqu’à la racine du projet.
# ------------------------------------------------------------------------------

# 1. Récupérer le chemin absolu du fichier courant (chemins.py)
chemin_actuel = Path(__file__).resolve()

# 2. Remonter jusqu’à la racine du projet
#    Exemple : si ce fichier est dans src/outils/chemins.py, alors parents[2] =
#    mon_projet/ (2 niveaux au-dessus de « outils/ »)
racine_projet = chemin_actuel.parents[2]

# 3. Définition de tous les chemins « absolus » utilisés dans le projet

# 3.1 Fichiers de configuration à la racine
chemin_readme = racine_projet / "README.md"
chemin_makefile = racine_projet / "Makefile"


# 3.2 Dossier contenant les bases Access (.accdb)
dossier_db_access = racine_projet / "db_sage_access"

#3.3 Dossier contenant les fichiers de base de données du Datalake
dossier_datalake = racine_projet / "data_lake"
dossier_datalake_raw = dossier_datalake / "raw"  # pour les données brutes
dossier_datalake_raw_sage = dossier_datalake_raw / "sage"  # pour les données brutes
dossier_datalake_raw_proalpha = dossier_datalake_raw / "proalpha"  # pour les données brutes ProAlpha
dossier_datalake_entetes = dossier_datalake_raw_sage / "entetes_csv"  # pour les en-têtes extraites
dossier_datalake_staging_sage = dossier_datalake / "staging"  # pour les données intermédiaires
dossier_datalake_processed = dossier_datalake / "processed"  # pour les données traitées

# 3.3.1 Dossier contenant les fichiers des bibliothèques requises pour l'environnement virtuel python 
dossier_requirements = racine_projet / "requirements"

# 3.3.2 Fichier requirements.txt pour les dépendances Python pour l'extraction
chemin_requirements_extraction = dossier_requirements / "requirements-extraction.txt"
chemin_requirements_mysql = dossier_requirements / "requirements-mysql.txt"  # pour compatibilité MySQL
chemin_requirements_postgresql = dossier_requirements / "requirements-postgresql.txt"  # pour compatibilité PostgreSQL

# 3.4 Dossier « src/ » et ses sous-dossiers
dossier_src = racine_projet / "src"
dossier_modules = dossier_src / "modules"
dossier_outils = dossier_src / "outils"
dossier_db = dossier_src / "db"  # si un sous-dossier « db » existe dans src

# 3.5 Scripts principaux à exécuter depuis « src/modules »
chemin_script_extraction_complete = dossier_modules / "extraction_complete_access.py"
chemin_script_extraction_entetes = dossier_modules / "extraction_entetes.py"
chemin_script_generer_statistiques = dossier_outils / "generer_statistiques_tables.py"
chemin_script_nettoyage = dossier_modules / "nettoyage_fichiers_csv.py"
chemin_script_construction_bdd = dossier_modules / "construction_bdd_sql.py"

# 3.6 Dossier de statistiques (si utilisé)
dossier_statistiques = racine_projet / "statistiques"
dossier_analyse_structure = dossier_statistiques / "analyse_structure_db"
dossier_tables_statistiques = dossier_statistiques / "tables"

# 3.7 Fichiers JSON de configuration pour les bases
dossier_config = racine_projet / "config"
chemin_config_postgres = dossier_config / "postgres_config.json"
chemin_config_mysql = dossier_config / "mysql_config.json"  # compatible MySQL

# 4. Fonctions utilitaires pour s’assurer que les dossiers existent
def creer_dossier_s_il_n_existe_pas(chemin: Path) -> None:
    """
    Crée le répertoire (et ses parents) si celui-ci n’existe pas encore.
    """
    if not chemin.exists():
        chemin.mkdir(parents=True, exist_ok=True)

# 5. Création automatique des répertoires de sortie la première fois
#    (utile pour éviter les erreurs si les dossiers n’existent pas)
creer_dossier_s_il_n_existe_pas(dossier_datalake_raw_sage)
creer_dossier_s_il_n_existe_pas(dossier_datalake_staging_sage)
creer_dossier_s_il_n_existe_pas(dossier_datalake_entetes)

# 6. (Optionnel) Pour le débogage : afficher toutes les routes définies
if __name__ == "__main__":
    print("Racine du projet           :", racine_projet)
    print("README.md                  :", chemin_readme)
    print("Makefile                   :", chemin_makefile)
    print("requirements               :", dossier_requirements)
    print("requirements_extraction.txt:", chemin_requirements_extraction)
    print("requirements_mysql.txt     :", chemin_requirements_mysql)
    print("requirements_postgresql.txt:", chemin_requirements_postgresql)
    print("db_sage_access             :", dossier_db_access)
    print("extraits/csv_extraits      :", dossier_datalake_raw_sage)
    print("extraits/xlsx_propres      :", dossier_datalake_staging_sage)
    print("extraits/entetes_csv       :", dossier_datalake_entetes)
    print("src/modules                :", dossier_modules)
    print("src/outils                 :", dossier_outils)
    print("src/db                     :", dossier_db)
    print("statistiques               :", dossier_statistiques)
    print("analyse_structure_db       :", dossier_analyse_structure)
    print("tables                     :", dossier_tables_statistiques)
    print("config/postgres_config.json:", chemin_config_postgres)
    print("config/mysql_config.json   :", chemin_config_mysql)
