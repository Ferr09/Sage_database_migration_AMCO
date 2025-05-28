.PHONY: setup installer perms backups extraer maintenance backup restore vacuum reindex check

# Instalación y preparación del entorno y estructura
setup: perms backups installer
	@echo "✔ Setup completado"

installer:
	@echo "🔧 Creando entorno virtual..."
	python3 -m venv .venv
	@echo "📦 Instalando dependencias..."
	. .venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt

perms:
	@echo "🔐 Actualizando permisos scripts..."
	chmod +x scripts/*.sh

backups:
	@echo "📁 Creando carpeta backups si no existe..."
	mkdir -p backups

# Ejecutar extracción (única vez)
extraer:
	@echo "📥 Ejecutando extracción desde Access..."
	. .venv/bin/activate && python src/extract_access.py

# Mantenimiento habitual
maintenance: backup vacuum reindex check
	@echo "🛠 Mantenimiento de base de datos completado"

backup:
	@echo "💾 Ejecutando backup..."
	bash scripts/backup.sh

restore:
	@echo "♻ Restaurando base de datos (especificar archivo):"
	@echo "make restore FILE=backups/backup_2023-01-01.sql"
	if [ -z "$(FILE)" ]; then echo "❌ ERROR: Debe especificar FILE"; exit 1; fi
	bash scripts/restore.sh $(FILE)

vacuum:
	@echo "🧹 Ejecutando VACUUM ANALYZE..."
	bash scripts/vacuum_analyze.sh

reindex:
	@echo "🧩 Ejecutando REINDEX..."
	bash scripts/reindex.sh

check:
	@echo "🔎 Verificando conexiones activas..."
	bash scripts/check_connections.sh
