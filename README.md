# Projet Python – Extraction Access → PostgreSQL

Ce projet Python permet d'extraire des données depuis une base Microsoft Access (.accdb), de les transformer, et de les charger dans une base de données PostgreSQL, dans un environnement isolé et reproductible.

## 📌 Objectifs

- Extraire des données depuis Access
- Traiter et transformer les données (nettoyage, normalisation)
- Les injecter dans une base PostgreSQL
- Fournir une structure de projet claire et portable

## 🛠 Technologies utilisées

- Python 3
- `pyodbc` (connexion Access)
- `psycopg2` (connexion PostgreSQL)
- PostgreSQL
- pgAdmin 4 (interface d’administration de la base PostgreSQL)
- Git + Git Bash (sous Windows)
- Environnement virtuel `.venv`
- Fichiers de configuration `.json`

## ⚙️ Prérequis

- Python ≥ 3.8
- Git
- `make` (Linux/macOS ou Git Bash sous Windows)
- PostgreSQL installé et accessible
- pgAdmin installé et configuré
- Fichier `.accdb` (base Access d'origine)


Pour exécuter ce projet, vous devez avoir **Python 3.8 ou supérieur installé** sur votre machine.

### 🔧 Vérification

Dans un terminal, exécutez :

```bash
python --version
```

ou

```bash
python3 --version
```


### 📥 Installation de Python

1. Téléchargez Python depuis le site officiel :  
   https://www.python.org/downloads/

2. Sous Windows (version ≥ 8), cochez l’option « Add Python to PATH » pendant l’installation.  
   Redémarrez le terminal si nécessaire.

3. Sous Windows 7, seules les versions Python 3.8.x sont compatibles :  
   a. Téléchargez Python 3.8.14 depuis  
      https://www.python.org/downloads/release/python-3814/  
   b. Cochez « Add Python to PATH » pendant l’installation.  
   c. Vérifiez la version installée :  
      ```bash
      python --version  # doit renvoyer Python 3.8.14
      ```

4. Créez un environnement virtuel et installez les dépendances :  
   ```bash
   python -m venv .venv
   # sous macOS/Linux
   source .venv/bin/activate
   # sous Windows
   .venv\Scripts\activate

   pip install -r requirements.txt


## 📥 Installation du pilote Microsoft Access (obligatoire)

Pour permettre la connexion à la base `.accdb`, vous devez installer le pilote Access Database Engine :

1. Télécharger depuis le site officiel de Microsoft :  
   [https://www.microsoft.com/fr-fr/download/details.aspx?id=54920](https://www.microsoft.com/en-us/download/details.aspx?id=54920)

2. Choisir la version 64 bits si votre Python est 64 bits.

3. Installer le pilote (fichier `.exe`) avec les droits administrateur.

4. Redémarrer l'ordinateur si nécessaire.

> 🔁 **Remarque importante** : si, après l'installation, la connexion échoue avec une erreur du type "ODBC Driver not found", vous devrez peut-être ajouter le dossier contenant `ACEODBC.DLL` à la variable d’environnement `PATH`.  
> Par défaut, il peut se trouver ici :  
> `C:\Program Files\Common Files\Microsoft Shared\OFFICE14\`

Ajoutez ce chemin manuellement dans :
- Panneau de configuration → Système → Paramètres système avancés → Variables d’environnement
- Modifier la variable `Path` dans les variables utilisateur ou système


## 📂 Placement de la base Access

Le fichier `.accdb` doit obligatoirement être placé dans le dossier `db_sage_access/` à la racine du projet pour que l’exportation fonctionne correctement.

Exemple :
mon_projet/
├── db_sage_access/
│   └── base_origine.accdb


## 🔐 Création des rôles PostgreSQL recommandés

Avant de lancer l’application, créer manuellement les rôles suivants dans PostgreSQL :

1. Se connecter avec pgAdmin en tant qu’utilisateur `postgres`
2. Créer un rôle `admin` avec tous les droits sur la base du projet :
   CREATE ROLE admin WITH LOGIN PASSWORD 'mot_de_passe_admin';
   GRANT ALL PRIVILEGES ON DATABASE ma_base TO admin;
3. Créer un rôle `user` en lecture seule :
   CREATE ROLE user WITH LOGIN PASSWORD 'mot_de_passe_user';
   GRANT CONNECT ON DATABASE ma_base TO user;
   \c ma_base
   GRANT USAGE ON SCHEMA public TO user;
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO user;
   ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO user;

Ainsi :
- Le rôle `admin` pourra insérer, modifier, supprimer et consulter
- Le rôle `user` ne pourra que consulter les données (`SELECT` uniquement)


## 🧰 Installation de PostgreSQL et pgAdmin

1. Télécharger PostgreSQL et pgAdmin depuis le site officiel : https://www.postgresql.org/download/
2. Pendant l'installation, noter le mot de passe de l'utilisateur `postgres`
3. Lancer pgAdmin, créer une connexion serveur (clic droit > Create > Server)
   - Nom : MonServeur
   - Onglet Connection : hôte `localhost`, port `5432`, utilisateur `postgres`
4. Créer la base de données du projet : `ma_base`
5. Créer les rôles comme décrit ci-dessus


## 📦 Vérification et installation de `jq` (outil requis pour les scripts de maintenance de la base PostgreSQL)

Les scripts `backup.sh` et `restore.sh` utilisent `jq` pour lire la configuration depuis le fichier JSON.

### Vérifier si `jq` est installé

Dans un terminal, exécutez :

```bash
jq --version
```

### Installation de jq 
- Sur Linux (Debian/Ubuntu) :  
  `sudo apt install jq`

- Sur macOS (avec Homebrew) :  
  `brew install jq`

- Sur Windows (Git Bash) :  
  Souvent inclus, sinon [installer jq](https://stedolan.github.io/jq/download/)


## 🚀 Installation rapide du projet

1. Cloner le projet :
   git clone https://github.com/Ferr09/Sage_database_migration_AMCO.git 
   cd Sage_database_migration_AMCO

### Si vous n'avez pas Linux ou une interface Linux sur Windows, alors n'utilisez pas les modules make
### Passez par l'execution main.py
2. Installer l’environnement :
   make installer

3. Lancer l’application :
   make lancer


## 🪟 Utilisation sous Windows

Option recommandée : Git Bash

1. Télécharger Git for Windows : https://gitforwindows.org/
2. Clic droit dans le dossier du projet → "Git Bash Here"
3. Exécuter :
   make installer
   make lancer

## 🚀 Usage
1. Positionnez-vous à la racine du projet :

bash
`python src/main.py`
Répondez oui ou non pour consulter le README avant de continuer.

2. Choisissez l’étape de démarrage :

- Exporter la base Access vers des fichiers CSV/Excel
- Charger directement les fichiers existants dans PostgreSQL

### Option 1 – Export Access

#### Exécution simple :

bash
`python main.py 1`
Avec chemin personnalisé vers le fichier .accdb :

bash
`python main.py 1 -a "/chemin/vers/access.accdb"`
Le script :

> Recherche access.accdb à la racine
> Sinon, invite à saisir le chemin (tapez sortir pour abandonner)

Modules exécutés, dans l’ordre :
- Sage/src/modules/extraction_complete_fichiers_csv.py
- Sage/src/modules/extraction_entetes.py
- Sage/src/outils/generer_statistiques_tables.py
- Sage/src/modules/nettoyage_fichiers_csv.py

### Option 2 – Injection PostgreSQL

#### Exécution :

bash
`python main.py 2`
Le script :

Crée ou met à jour config.json en demandant :
- Hôte (ex. localhost)
- Port (ex. 5432)
- Nom de la base
- Utilisateur
- Mot de passe

Exécute construction_bdd_sql.py pour insérer les données

#### Annulation : à tout moment lors d’une invite, tapez :
`sortir`


## 📁 Structure du projet

mon_projet/
├── config/
│   └── postgres_config.json
├── data/
│   └── ma_base.accdb
├── src/
│   ├── main.py
│   ├── db/
│   └── modules/
├── .venv/                 ← créé automatiquement
├── requirements.txt
├── Makefile
└── README.md


## 🔐 Fichiers de configuration `.json`

Exemple config/postgres_config.json :
{
  "host": "localhost",
  "port": 5432,
  "user": "admin",
  "password": "mot_de_passe_admin",
  "dbname": "ma_base"
}

⚠ Ces fichiers ne doivent jamais être versionnés avec Git s’ils contiennent des données sensibles. Utilisez un fichier `.gitignore`.


## 🧪 Commandes disponibles

make installer     → Crée l’environnement virtuel et installe les dépendances
make lancer        → Lance le script principal (src/main.py)
make nettoyer      → Supprime l’environnement virtuel


## 🔍 Bonnes pratiques

- Utiliser un environnement virtuel local `.venv/`
- Ne pas versionner les identifiants (utiliser `.gitignore`)
- Documenter la configuration dans `.json.example`
- Lancer le projet uniquement depuis la racine avec `make`


## 👤 Auteur / Crédits
- Développé par Fernando Rojas
- Projet réalisé chez AMCO dans le cadre du stage de deuxième année, spécialisé en Informatique en Systèmes d'Information à l'École Centrale de Nantes 


