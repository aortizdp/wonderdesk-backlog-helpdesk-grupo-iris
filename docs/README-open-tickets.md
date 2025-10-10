# Addenda al README — Script `wonderdesk_open_tickets_to_sheet.py`

Este addenda complementa el README principal del proyecto **Wonderdesk - Backlog Helpdesk Grupo Iris**.

## Nuevo script

* `wonderdesk_open_tickets_to_sheet.py`: crea una **pestaña nueva** en Google Sheets con **todos los tickets abiertos** por agencia. Recorre la paginación de *Home/Inicio* y escribe **una fila por ticket** con las columnas:

  * `AGENCIA`
  * `ID`
  * `Fecha` (formato **DD/MM/YYYY**)
  * `Mes` (formato **YYYY-MM**)
  * `Año` (formato **YYYY**)
  * `Subject`

> Nota: en esta extracción ya **no se exporta `Category`**; el `Subject` es suficiente.

## Uso

```bash
# Pestaña con fecha: OPEN-TICKETS-YYYYMMDD
python scripts/wonderdesk_open_tickets_to_sheet.py

# Pestaña fija
python scripts/wonderdesk_open_tickets_to_sheet.py --tab-name "OPEN-TICKETS"
```

## Requisitos

* Misma configuración `.env` y credenciales de Google Sheets que el resto del proyecto (ver README principal).
* Dependencias: `python-dotenv`, `playwright`, `python-dateutil`, `gspread`, `google-auth`. Instala Chromium para Playwright si no lo has hecho:

  ```bash
  python -m playwright install chromium
  ```

## Estructura del proyecto (actualización)

Añade este archivo al árbol `scripts/`:

```
scripts/
│  ├─ wonderdesk_all_reports_async.py
│  ├─ wonderdesk_daily_to_sheet.py
│  ├─ wonderdesk_daily_to_sheet_backfill.py
│  └─ wonderdesk_open_tickets_to_sheet.py   ← NUEVO
```

---

# Pasos para subir el script y el README actualizado a GitHub

> Repo: `wonderdesk-backlog-helpdesk-grupo-iris`

### Si ya tienes el repo clonado

1. Copia el script (ajusta la ruta fuente si lo tienes fuera):

```bash
cd /ruta/al/repo/wonderdesk-backlog-helpdesk-grupo-iris
mkdir -p scripts
cp /Users/cio/Helpdesk/wonderdesk_open_tickets_to_sheet.py scripts/
```

2. Actualiza el README principal o conserva este addenda. Si quieres añadir este addenda como doc:

```bash
mkdir -p docs
# copia el contenido de este addenda (canvas) y pégalo:
pbpaste > docs/README-open-tickets.md
```

3. Commit + push:

```bash
git add scripts/wonderdesk_open_tickets_to_sheet.py docs/README-open-tickets.md
git commit -m "feat(open-tickets): script de exportación de abiertos a Google Sheets + docs"
git push
```

### Si todavía no tienes el repo local

```bash
# clona
gh repo clone EL_TEU_USUARI/wonderdesk-backlog-helpdesk-grupo-iris
cd wonderdesk-backlog-helpdesk-grupo-iris

# añade el script
mkdir -p scripts
cp /Users/cio/Helpdesk/wonderdesk_open_tickets_to_sheet.py scripts/

# añade el addenda de docs
mkdir -p docs
pbpaste > docs/README-open-tickets.md   # primero copia este texto desde el canvas

# commit + push
git add scripts/wonderdesk_open_tickets_to_sheet.py docs/README-open-tickets.md
git commit -m "feat(open-tickets): script de exportación de abiertos a Google Sheets + docs"
git push -u origin main
```
