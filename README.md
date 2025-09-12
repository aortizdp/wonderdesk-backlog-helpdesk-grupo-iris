# Wonderdesk - Backlog Helpdesk Grupo Iris

Automatiza la extracción de métricas de **WonderDesk** por agencia y su volcado en **Google Sheets**.

Incluye tres scripts principales:

* `wonderdesk_all_reports_async.py`: genera informes completos y CSVs para múltiples agencias.
* `wonderdesk_daily_to_sheet.py`: inserta **cada día laborable** una fila por agencia en la hoja `DATOS-Daily` (con lógica especial de lunes = viernes→lunes).
* `wonderdesk_daily_to_sheet_backfill.py`: permite **recuperar días pasados** (uno o rango) y rellenar la hoja `DATOS-Daily` sin alterar el flujo diario.

---

## 1) Descripción de cada script

### 1.1. `wonderdesk_all_reports_async.py`

**Propósito**: ejecutar un barrido completo multi‑agencia y producir:

* Métricas por agencia: `Tickets Abiertos`, `Tickets Cerrados`, `Abiertos Última Semana`, `Cerrados Última Semana`, `DS` (ocurrencias) y `P3` (ocurrencias).
* CSVs:

  * `agencias_wonderdesk_stats.csv` (resumen por agencia + totales al final)
  * `tickets_wonderdesk.csv` (listado de tickets abiertos con campos clave)
  * `ds_summary.csv` (resumen de *bugs* por `DSnnnn` con agencias afectadas)

**Cómo funciona (resumen)**:

1. Inicia sesión en WonderDesk por agencia.
2. `Home/Inicio` → extrae tabla, cuenta `DS`/`P3` por **ocurrencia** y calcula abiertos en ventana (por defecto últimos 7 días; en tu versión actual ya está ajustado a la lógica que acordamos).
3. `List Closed` → salta a **\[>>]** y recorre hacia atrás para contar **Cerrados Última Semana**.
4. Escribe los CSV y opcionalmente sube a Google Sheets si está configurado.

---

### 1.2. `wonderdesk_daily_to_sheet.py`

**Propósito**: ejecutar **cada día** y añadir una fila por agencia en la hoja de Google `DATOS-Daily`.

**Columnas escritas (A–N):**
A `Fecha (DD/MM/YYYY)` · B `AGENCIA` · C `Tickets Abiertos` · D `Tickets Cerrados` · E `Abiertos Última Semana`\* · F `Cerrados Última Semana`\* · G `DS` · H `P3` · I `Total (=C+D)` · J `LW total (=E+F)` · K `Δ Cerrados(-22d)` (con `IFNA(...;0)`) · L `Semana (YYYY-SS)` · M `Mes (YYYY-MM)` · N `Año (YYYY)`.

\* **Compatibilidad**: aunque las cabeceras se llaman “Última Semana”, el script rellena **una ventana diaria** con esta lógica:

* L–V (no lunes): **ayer** `[00:00, 24:00)`
* **Lunes**: **viernes 00:00 → lunes 00:00** (incluye fin de semana)

**Notas**:

* Cuenta `DS`/`P3` por ocurrencia en la columna *Subject/Category*.
* `Cerrados` se computa recorriendo páginas desde `[>>]` hacia atrás hasta salir de la ventana.

---

### 1.3. `wonderdesk_daily_to_sheet_backfill.py`

**Propósito**: recuperar festivos o periodos pasados y rellenar `DATOS-Daily`.

**Modos**:

* Un día: `--date YYYY-MM-DD`
* Rango: `--start YYYY-MM-DD --end YYYY-MM-DD` (ambos inclusive)

**Diferencia clave**: en *backfill* cada día se calcula como **ventana exacta del propio día (00:00→24:00)**, **sin** la regla especial de los lunes.

Las mismas columnas A–N que el diario y mismas métricas.

---

## 2) Configuración (`.env`)

Crea un `.env` en la raíz del proyecto (no lo subas al repo) con:

```ini
# WonderDesk
HELPDESK_BASE_URL=https://helpdesk.grupoiris.net
HEADFUL=false

# Opción multi‑agencia
AGENCIES=ACVIAJES,ADRIANO,...
ACVIAJES_NOMBRE=AC Viajes
ACVIAJES_USUARIO=usuario1
ACVIAJES_PASSWORD=********
ADRIANO_NOMBRE=Adriano Travel
ADRIANO_USUARIO=usuario2
ADRIANO_PASSWORD=********

# Opción alternativa (una sola)
# COMPANY=ACVIAJES
# ACVIAJES_USERNAME=usuario1
# ACVIAJES_PASSWORD=********
# ACVIAJES_NOMBRE=AC Viajes

# Google Sheets
GOOGLE_SHEETS_SPREADSHEET_ID=1AbCdEf...           # ID del doc
GOOGLE_APPLICATION_CREDENTIALS=/ruta/.login-gsheets.json
SHEET_DAILY_TAB=DATOS-Daily
```

**Credenciales de Google**:

1. Crea/usa un *Service Account* con **Google Sheets API** habilitada.
2. Descarga el JSON y guarda la ruta en `GOOGLE_APPLICATION_CREDENTIALS`.
3. **Comparte** la hoja de cálculo con el email del Service Account (rol Editor).

---

## 3) Instalación

Requisitos: Python 3.9+, Playwright Chromium, acceso a Internet hacia WonderDesk y Google.

```bash
python -m venv .venv
source .venv/bin/activate  # (Windows: .venv\\Scripts\\activate)

pip install -r requirements.txt
python -m playwright install chromium
```

**`requirements.txt` sugerido**

```txt
python-dotenv
playwright
python-dateutil
gspread
google-auth
nest_asyncio
```

**Seguridad**: añade `.env` y el JSON de credenciales al `.gitignore`.

---

## 4) Uso

### 4.1. Informes completos

```bash
python scripts/wonderdesk_all_reports_async.py
# → genera: agencias_wonderdesk_stats.csv, tickets_wonderdesk.csv, ds_summary.csv
```

### 4.2. Diario a Google Sheets

```bash
python scripts/wonderdesk_daily_to_sheet.py
# → añade una fila por agencia en DATOS-Daily (ventana diaria/lunes extendido)
```

### 4.3. Backfill de días

```bash
# Un día
python scripts/wonderdesk_daily_to_sheet_backfill.py --date 2025-08-15

# Rango
python scripts/wonderdesk_daily_to_sheet_backfill.py --start 2025-08-15 --end 2025-08-18
```

---

## 5) Planificación (cron)

Ejecutar cada día laborable a las **09:00** (zona Europa/Madrid):

```cron
# m h  dom mon dow   command
0 9 * * 1-5  /usr/bin/env bash -lc 'cd /RUTA/AL/PROYECTO && source .venv/bin/activate && python scripts/wonderdesk_daily_to_sheet.py >> logs/daily.log 2>&1'
```

> Consejo: crea el directorio `logs/` y revisa permisos.

---

## 6) Estructura del proyecto

```
wonderdesk-backlog-helpdesk-grupo-iris/
├─ README.md
├─ LICENSE
├─ .gitignore
├─ .env.example
├─ requirements.txt
├─ scripts/
│  ├─ wonderdesk_all_reports_async.py
│  ├─ wonderdesk_daily_to_sheet.py
│  └─ wonderdesk_daily_to_sheet_backfill.py
└─ docs/
   └─ CHANGELOG.md (opcional)
```

**`.env.example`** (para compartir sin secretos)

```ini
HELPDESK_BASE_URL=https://helpdesk.grupoiris.net
HEADFUL=false
AGENCIES=ACVIAJES,ADRIANO
ACVIAJES_NOMBRE=
ACVIAJES_USUARIO=
ACVIAJES_PASSWORD=
ADRIANO_NOMBRE=
ADRIANO_USUARIO=
ADRIANO_PASSWORD=
GOOGLE_SHEETS_SPREADSHEET_ID=
GOOGLE_APPLICATION_CREDENTIALS=
SHEET_DAILY_TAB=DATOS-Daily
```

**`.gitignore`** sugerido

```
# Python
__pycache__/
*.pyc
.venv/

# Credenciales y config local
.env
*.json
!package.json
*.csv
logs/
playwright-report/
```

**`LICENSE`** (MIT — opcional)

```
MIT License

Copyright (c) 2025

Permission is hereby granted, free of charge, to any person obtaining a copy
... (texto MIT estándar)
```

---

## 7) Crear el repositorio en GitHub

> **Nombre del repositorio**: GitHub no admite espacios; usa el *slug* `wonderdesk-backlog-helpdesk-grupo-iris` y como **descripción** pon “Wonderdesk - Backlog Helpdesk Grupo Iris”.

### Opción A) con GitHub CLI (`gh`)

```bash
# 1) crear carpeta del proyecto
mkdir wonderdesk-backlog-helpdesk-grupo-iris && cd $_

# 2) estructura base
mkdir -p scripts docs logs
printf "python-dotenv\nplaywright\npython-dateutil\ngspread\ngoogle-auth\nnest_asyncio\n" > requirements.txt
cp /ruta/a/tus/scripts/wonderdesk_all_reports_async.py scripts/
cp /ruta/a/tus/scripts/wonderdesk_daily_to_sheet.py scripts/
cp /ruta/a/tus/scripts/wonderdesk_daily_to_sheet_backfill.py scripts/
# crea README.md con este contenido (o copia-pega desde este documento)

# 3) git init + primer commit
git init
printf "# Ignora secretos\n.env\n.venv/\n*.json\n__pycache__/\n*.pyc\nlogs/\n" > .gitignore
git add .
git commit -m "Initial commit: scripts + docs"

# 4) crear repo remoto y subir
gh repo create wonderdesk-backlog-helpdesk-grupo-iris --public \
  --source=. --remote=origin \
  --description "Wonderdesk - Backlog Helpdesk Grupo Iris"

git push -u origin main
```

### Opción B) manual (sin `gh`)

```bash
git init
git add .
git commit -m "Initial commit: scripts + docs"
git branch -M main
# crea el repo vacío en GitHub y copia la URL SSH/HTTPS
git remote add origin git@github.com:TU_USUARIO/wonderdesk-backlog-helpdesk-grupo-iris.git
# o: https://github.com/TU_USUARIO/wonderdesk-backlog-helpdesk-grupo-iris.git
git push -u origin main
```

---

## 8) Buenas prácticas y notas

* **No subas** `.env` ni el JSON de credenciales. Usa `GOOGLE_APPLICATION_CREDENTIALS` con ruta local segura.
* Si WonderDesk cambia HTML/labels, ajusta los selectores (los scripts ya incluyen heurísticas y *fallbacks*).
* Para grandes volúmenes de agencias, considera usar Playwright en **headless** y limitar concurrencia si añadieras paralelismo.
* Revisa periódicamente el *banner* de `Calls` y la paginación (`[>>]`, `[<]`, `Siguiente >`).

---

## 9) Ejemplos rápidos

```bash
# Informe semanal completo (CSV)
python scripts/wonderdesk_all_reports_async.py

# Diario (09:00 en cron)
python scripts/wonderdesk_daily_to_sheet.py

# Backfill del puente
python scripts/wonderdesk_daily_to_sheet_backfill.py --start 2025-08-15 --end 2025-08-18
```

---

¿Dudas o quieres que deje preparado un **Makefile** con comandos (`make install`, `make daily`, `make backfill`)? Puedo añadirlo en un momento.
