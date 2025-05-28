#!/bin/bash

CONFIG_FILE="config/postgres_config.json"

if ! command -v jq >/dev/null 2>&1; then
    echo "jq n'est pas installé. Veuillez l'installer avant d'exécuter ce script."
    exit 1
fi

HOST=$(jq -r '.host' "$CONFIG_FILE")
PORT=$(jq -r '.port' "$CONFIG_FILE")
USER=$(jq -r '.user' "$CONFIG_FILE")
PASS=$(jq -r '.password' "$CONFIG_FILE")
DBNAME=$(jq -r '.dbname' "$CONFIG_FILE")

FICHIER=$1
if [ -z "$FICHIER" ]; then
  echo "Veuillez spécifier un fichier .sql à restaurer."
  exit 1
fi

export PGPASSWORD="$PASS"
psql -U "$USER" -h "$HOST" -p "$PORT" -d "$DBNAME" < "$FICHIER"
echo "✅ Base restaurée depuis : $FICHIER"
