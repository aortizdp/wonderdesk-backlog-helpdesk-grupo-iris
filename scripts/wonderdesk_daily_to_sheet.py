#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
wonderdesk_daily_to_sheet.py

Genera métricas diarias por agencia desde WonderDesk y las APPENDEA a Google Sheets (hoja: DATOS-Daily).

Columnas (A..N):
A Fecha (DD/MM/YYYY)  -> fecha de ejecución (Europe/Madrid)
B AGENCIA
C Tickets Abiertos (total "Calls" en Home/Inicio)
D Tickets Cerrados (total "Calls" en List Closed/Cerrados)
E Abiertos Ventana (ayer; si hoy es lunes: desde viernes 00:00 incluyendo fin de semana, hasta lunes 00:00)
F Cerrados Ventana (misma ventana)
G DS (cuenta ocurrencias de DS+IS con número en el Subject/Título; si un ticket tiene 2 códigos, suma 2)
H P3 (cuenta tickets cuyo subject contiene "P3" o "P3.")
I Total = C + D
J LW total = E + F
K =IFNA(D[fila]-D[fila-22];0)   (locale con ';')
L Semana ISO (YYYY-SS)
M Mes (YYYY-MM)
N Año (YYYY)

Fix adicional:
- Si D (Tickets Cerrados) sale 0 para una agencia, se reintenta leer 2 veces.
  Si sigue siendo 0, se copia el valor de D de 22 filas arriba (misma agencia) si existe.

Requisitos:
  pip install -U python-dotenv playwright python-dateutil gspread google-auth nest_asyncio
  python -m playwright install chromium

.env requerido:
  HELPDESK_BASE_URL=https://helpdesk.grupoiris.net
  AGENCIES=AG1,AG2,AG3
  AG1_NOMBRE=Nombre Agencia 1
  AG1_USUARIO=usuario1
  AG1_PASSWORD=clave1
  ...

  Google Sheets:
  GOOGLE_SHEETS_SPREADSHEET_ID=xxxxxxxxxxxxxxxxxxxxxxxxxxxx
  GOOGLE_APPLICATION_CREDENTIALS=/Users/cio/Helpdesk/.login-gsheets.json
  GOOGLE_SHEETS_WORKSHEET=DATOS-Daily   (opcional; default DATOS-Daily)

Opcionales:
  HEADFUL=false
  DEBUG=false
"""

from __future__ import annotations

import os
import re
import sys
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
from collections import defaultdict

from dotenv import load_dotenv
from dateutil import parser as dtparse
from playwright.async_api import async_playwright, Page

# ------------------------------- Regex / parsing --------------------------------

RX_ISSUE = re.compile(r"(?i)\b(?:DS|IS)\s*-?\s*\d+")
RX_P3 = re.compile(r"(?i)\bP\s*3(\b|\.)")

def extract_issue_codes(text: str) -> List[str]:
    """Devuelve códigos normalizados DS123 / IS123 encontrados en un texto."""
    s = (text or "").strip()
    out: List[str] = []
    for m in re.finditer(r"(?i)\b(DS|IS)\s*-?\s*(\d+)", s):
        out.append(f"{m.group(1).upper()}{m.group(2)}")
    return out

def parse_date_any(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return dtparse.parse(s, dayfirst=False, fuzzy=True)
    except Exception:
        return None

def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

# ------------------------------- Window diario ----------------------------------

def daily_window(now_dt: datetime) -> Tuple[datetime, datetime]:
    """
    Retorna (start,end) naive para ventana:
      - Si hoy es lunes: viernes 00:00 -> lunes 00:00
      - Resto: ayer 00:00 -> hoy 00:00
    now_dt debe venir en TZ Europe/Madrid.
    """
    today = now_dt.date()
    if today.weekday() == 0:  # Monday
        start_d = today - timedelta(days=3)  # viernes
    else:
        start_d = today - timedelta(days=1)  # ayer
    start = datetime.combine(start_d, datetime.min.time())
    end = datetime.combine(today, datetime.min.time())
    return start, end

# ------------------------------- Config / agencias --------------------------------

@dataclass
class Agency:
    code: str
    nombre: str
    usuario: str
    password: str

def load_config() -> Tuple[str, bool, bool, str, str, List[Agency]]:
    load_dotenv()

    base_url = os.getenv("HELPDESK_BASE_URL", "https://helpdesk.grupoiris.net").rstrip("/")
    headful = env_bool("HEADFUL", False)
    debug = env_bool("DEBUG", False)

    sheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    worksheet_name = os.getenv("GOOGLE_SHEETS_WORKSHEET", "DATOS-Daily").strip() or "DATOS-Daily"

    agencies: List[Agency] = []
    raw = os.getenv("AGENCIES", "").strip()
    if raw:
        codes = [c.strip().upper() for c in raw.split(",") if c.strip()]
        for code in codes:
            nombre = os.getenv(f"{code}_NOMBRE") or os.getenv(f"{code}_NOM") or code
            usuario = os.getenv(f"{code}_USUARIO") or os.getenv(f"{code}_USERNAME") or ""
            password = os.getenv(f"{code}_PASSWORD") or ""
            if not usuario or not password:
                raise SystemExit(f"Faltan credenciales para {code}: {code}_USUARIO/_USERNAME y/o {code}_PASSWORD")
            agencies.append(Agency(code=code, nombre=nombre, usuario=usuario, password=password))
    else:
        company = os.getenv("COMPANY", "").strip().upper()
        if not company:
            raise SystemExit("Falta AGENCIES=... o COMPANY=... en el .env")
        nombre = os.getenv(f"{company}_NOMBRE") or company
        usuario = os.getenv(f"{company}_USUARIO") or os.getenv(f"{company}_USERNAME") or ""
        password = os.getenv(f"{company}_PASSWORD") or ""
        if not usuario or not password:
            raise SystemExit(f"Faltan credenciales {company}_USUARIO/_USERNAME y/o {company}_PASSWORD en el .env")
        agencies.append(Agency(code=company, nombre=nombre, usuario=usuario, password=password))

    return base_url, headful, debug, sheet_id, creds, agencies, worksheet_name

# ------------------------------- Login -------------------------------------------

async def login(page: Page, base_url: str, usuario: str, password: str) -> None:
    await page.goto(f"{base_url}/wonderdesk.cgi", wait_until="load")

    async def attempt(ctx) -> bool:
        try:
            pw_loc = ctx.locator("input[type='password']")
            if await pw_loc.count() > 0:
                pw = pw_loc.first
                # usuario
                for sel in [
                    "input[name*='user' i]",
                    "input[name*='login' i]",
                    "input[type='email']",
                    "input[type='text']",
                    "input:not([type])",
                ]:
                    u_loc = ctx.locator(sel)
                    if await u_loc.count() > 0:
                        u = u_loc.first
                        await u.fill(usuario)
                        await pw.fill(password)

                        clicked = False
                        for b in [
                            "input[type='submit']",
                            "button[type='submit']",
                            "input[type='image']",
                            "button:has-text('Login')",
                            "button:has-text('Entrar')",
                            "button:has-text('Acceder')",
                        ]:
                            b_loc = ctx.locator(b)
                            if await b_loc.count() > 0:
                                await b_loc.first.click()
                                clicked = True
                                break
                        if not clicked:
                            await pw.press("Enter")

                        await ctx.wait_for_load_state("networkidle", timeout=15000)
                        return True
        except Exception:
            pass
        return False

    if not await attempt(page):
        for fr in page.frames:
            try:
                if await attempt(fr):
                    break
            except Exception:
                continue

    # si sigue el password visible, no logueó
    if await page.locator("input[type='password']").count() > 0:
        raise RuntimeError("No se pudo iniciar sesión (revisa credenciales o cambios del formulario)")

# ------------------------------- UI helpers --------------------------------------

async def click_menu(page: Page, names: List[str]) -> None:
    for n in names:
        try:
            await page.get_by_role("link", name=re.compile(rf"^{re.escape(n)}$", re.I)).click(timeout=2500)
            await page.wait_for_load_state("networkidle", timeout=15000)
            return
        except Exception:
            try:
                await page.get_by_text(re.compile(n, re.I), exact=False).click(timeout=2500)
                await page.wait_for_load_state("networkidle", timeout=15000)
                return
            except Exception:
                continue
    raise RuntimeError(f"No se encontró enlace de menú: {names}")

async def read_calls_banner_any(page: Page) -> int:
    """Lee el número 'XXXX Calls' desde página + frames y devuelve el mayor."""
    texts: List[str] = []
    try:
        texts.append(await page.evaluate("() => document.body ? document.body.innerText : document.documentElement.innerText"))
    except Exception:
        pass

    for fr in page.frames:
        try:
            texts.append(await fr.evaluate("() => document.body ? document.body.innerText : document.documentElement.innerText"))
        except Exception:
            continue

    vals: List[int] = []
    for txt in texts:
        for m in re.finditer(r"(\d+)\s+Calls", txt or "", flags=re.I):
            vals.append(int(m.group(1)))

    return max(vals) if vals else 0

# ------------------------------- Table parsing (Categoría + Título) --------------

JS_FIND_ROWS = r"""
() => {
  const norm = (s) => (s||'').replace(/\s+/g,' ').trim();
  const result = { ok:false, rows:[] };
  const tables = Array.from(document.querySelectorAll('table'));
  const good = [];

  for (const t of tables){
    const rows = Array.from(t.querySelectorAll(':scope > tbody > tr, :scope > tr'));
    if (!rows.length) continue;

    let headerIdx = -1, heads = [];
    for (let r=0; r<Math.min(4, rows.length); r++){
      const cells = Array.from(rows[r].querySelectorAll('th,td'));
      const h = cells.map(c => norm(c.innerText));
      const hasID   = h.some(x => /^id$/i.test(x));
      const hasDate = h.some(x => /(fecha|date)/i.test(x));
      const hasCat  = h.some(x => /(categor[ií]a|category)/i.test(x));
      const hasTit  = h.some(x => /(t[ií]tulo|title|asunto|subject)/i.test(x));
      if (cells.length >= 6 && hasID && hasDate && (hasCat || hasTit)) { headerIdx=r; heads=h; break; }
    }
    if (headerIdx < 0) continue;

    const idx = {};
    heads.forEach((h,i)=>{ idx[h.toLowerCase()] = i; });
    const findIdx = (rx) => {
      const k = Object.keys(idx).find(k => rx.test(k));
      return (k!=null) ? idx[k] : -1;
    };

    let idIx    = findIdx(/^\s*id\s*$/i);
    let dateIx  = findIdx(/(fecha|date)/i);
    let catIx   = findIdx(/(categor[ií]a|category)/i);
    let titleIx = findIdx(/(t[ií]tulo|title|asunto|subject)/i);

    if (idIx < 0 && heads.length >= 2) idIx = 1;
    if (dateIx < 0 && heads.length >= 3) dateIx = 2;

    const data = [];
    for (let r = headerIdx + 1; r < rows.length; r++){
      const cells = Array.from(rows[r].querySelectorAll('td'));
      if (!cells.length) continue;
      const get = (i) => (i>=0 && i<cells.length) ? cells[i] : null;

      const idTxt   = get(idIx)   ? norm(get(idIx).innerText)   : '';
      const dateTxt = get(dateIx) ? norm(get(dateIx).innerText) : '';

      let categoryTxt = '';
      const catCell = get(catIx);
      if (catCell) categoryTxt = norm(catCell.innerText);

      let subjectTxt = '';
      const titleCell = get(titleIx);
      if (titleCell){
        const a = titleCell.querySelector('a');
        subjectTxt = a ? norm(a.textContent) : norm(titleCell.innerText);
      } else if (catCell){
        const a = catCell.querySelector('a');
        subjectTxt = a ? norm(a.textContent) : categoryTxt;
      } else {
        const a = rows[r].querySelector('a');
        subjectTxt = a ? norm(a.textContent) : '';
      }

      if (!idTxt && !dateTxt && !subjectTxt && !categoryTxt) continue;

      data.push({ id:idTxt, date:dateTxt, category:categoryTxt, subject:subjectTxt });
    }

    const ok = data.filter(r => (r.subject || r.category) && r.date).length >= 3;
    if (ok) good.push({ data, n:data.length });
  }

  if (good.length){
    good.sort((a,b)=> b.n - a.n);
    return { ok:true, rows:good[0].data };
  }
  return result;
}
"""

async def extract_table_any(page: Page) -> List[Dict[str, str]]:
    try:
        res = await page.evaluate(JS_FIND_ROWS)
        if res and res.get("ok") and res.get("rows"):
            return res["rows"]
    except Exception:
        pass
    for fr in page.frames:
        try:
            res = await fr.evaluate(JS_FIND_ROWS)
            if res and res.get("ok") and res.get("rows"):
                return res["rows"]
        except Exception:
            continue
    return []

# ------------------------------- Paginación Closed -------------------------------

async def click_last_page(page: Page) -> bool:
    for sel in ["a:has-text('>>')", "a:has-text('[>>]')", "a:has-text('»»')"]:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.first.click(timeout=2500)
                await page.wait_for_load_state("networkidle", timeout=15000)
                return True
        except Exception:
            pass
    try:
        await page.get_by_role("link", name=re.compile(r"»»|>>", re.I)).first.click(timeout=2500)
        await page.wait_for_load_state("networkidle", timeout=15000)
        return True
    except Exception:
        return False

async def click_prev_page(page: Page) -> bool:
    for sel in ["a:has-text('[<]')", "a:has-text('<<')", "a:has-text('<')", "a:has-text('««')"]:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.last.click(timeout=2500)
                await page.wait_for_load_state("networkidle", timeout=15000)
                return True
        except Exception:
            pass
    return False

async def click_next_page(page: Page) -> bool:
    for rx in [r"Next", r"Siguiente", r"Avanzar", r"^\s*>\s*$", r"»"]:
        try:
            await page.get_by_role("link", name=re.compile(rx, re.I)).click(timeout=2500)
            await page.wait_for_load_state("networkidle", timeout=15000)
            return True
        except Exception:
            continue
    return False

# ------------------------------- Métricas ----------------------------------------

async def read_home_metrics(page: Page, start: datetime, end: datetime) -> Dict[str, int]:
    await click_menu(page, ["Home", "Inicio"])

    open_calls = await read_calls_banner_any(page)
    rows = await extract_table_any(page)

    ds_is = 0
    p3 = 0
    open_in_window = 0

    for r in rows:
        subj = (r.get("subject") or "").strip()
        cat = (r.get("category") or "").strip()
        target = subj or cat

        # DS+IS: contar ocurrencias por ticket (si hay 2 códigos en el subject, suma 2)
        codes = extract_issue_codes(target)
        ds_is += len(codes)

        # P3: 1 por ticket si coincide
        if RX_P3.search(target):
            p3 += 1

        d = parse_date_any(r.get("date", ""))
        if d:
            # Comparamos naive: start/end son naive; d puede ser naive -> OK
            if start <= d < end:
                open_in_window += 1

    return {
        "open_calls": open_calls,
        "open_window": open_in_window,
        "ds_is": ds_is,
        "p3": p3,
    }

async def read_closed_metrics(page: Page, start: datetime, end: datetime) -> Dict[str, int]:
    await click_menu(page, ["List Closed", "List Cerrado", "List Cerrados"])

    # Total cerrados (banner)
    closed_calls = await read_calls_banner_any(page)

    # Ventana: contar cerrados en [start,end)
    count_window = 0

    # estrategia rápida: ir a última página y retroceder
    used_last = await click_last_page(page)

    def page_max_date(rows: List[Dict[str, str]]) -> Optional[datetime]:
        mx = None
        for r in rows:
            d = parse_date_any(r.get("date", ""))
            if d and (mx is None or d > mx):
                mx = d
        return mx

    def page_min_date(rows: List[Dict[str, str]]) -> Optional[datetime]:
        mn = None
        for r in rows:
            d = parse_date_any(r.get("date", ""))
            if d and (mn is None or d < mn):
                mn = d
        return mn

    if used_last:
        for _ in range(300):
            rows = await extract_table_any(page)
            if not rows:
                break

            mx = page_max_date(rows)
            if mx is not None and mx < start:
                break

            for r in rows:
                d = parse_date_any(r.get("date", ""))
                if d and start <= d < end:
                    count_window += 1

            if not await click_prev_page(page):
                break
    else:
        # fallback: avanzar hacia delante hasta que la página sea más vieja que start
        for _ in range(300):
            rows = await extract_table_any(page)
            if not rows:
                break

            mn = page_min_date(rows)

            for r in rows:
                d = parse_date_any(r.get("date", ""))
                if d and start <= d < end:
                    count_window += 1

            if mn is not None and mn < start:
                break

            if not await click_next_page(page):
                break

    return {
        "closed_calls": closed_calls,
        "closed_window": count_window,
    }

async def read_closed_calls_with_retry(page: Page, start: datetime, end: datetime) -> Dict[str, int]:
    """
    Lee cerrados (total y ventana). Reintenta si el total sale 0.
    """
    metrics = await read_closed_metrics(page, start, end)
    if metrics["closed_calls"] != 0:
        return metrics

    # retry 1 (pequeño delay + reload)
    try:
        await asyncio.sleep(0.8)
        await page.reload(wait_until="networkidle")
    except Exception:
        pass
    metrics2 = await read_closed_metrics(page, start, end)
    if metrics2["closed_calls"] != 0:
        return metrics2

    # retry 2: volver a entrar al menú cerrados
    try:
        await asyncio.sleep(0.8)
        await click_menu(page, ["Home", "Inicio"])
        await click_menu(page, ["List Closed", "List Cerrado", "List Cerrados"])
    except Exception:
        pass
    metrics3 = await read_closed_metrics(page, start, end)
    return metrics3

# ------------------------------- Google Sheets -----------------------------------

def get_sheet_client(creds_path: str):
    import gspread
    return gspread.service_account(filename=os.path.expanduser(creds_path))

def ensure_rows(ws, needed_rows: int) -> None:
    """
    Asegura que la hoja tenga al menos needed_rows filas (para evitar grid limits).
    """
    if needed_rows <= ws.row_count:
        return
    add = needed_rows - ws.row_count
    # añade margen
    ws.add_rows(add + 50)

def find_last_row(ws) -> int:
    """
    Devuelve el último row con algo en columna A.
    """
    col_a = ws.col_values(1)  # incluye cabecera
    # col_values devuelve hasta el último no vacío
    return len(col_a)

def build_k_formula(row_num: int) -> str:
    """
    Columna K: =IFNA(Drow - D(row-22);0)
    """
    prev = row_num - 22
    if prev < 2:
        # si no hay 22 filas arriba, devuelve 0
        return "=0"
    return f"=IFNA(D{row_num}-D{prev};0)"

def closed_fallback_from_22_above(ws, agency_name: str, target_row_num: int) -> Optional[int]:
    """
    Si Cerrados=0, intenta tomar D de 22 filas arriba para la MISMA agencia.
    Reglas:
      - target_row_num es la fila donde iría el dato (1-index)
      - mira fila target_row_num-22 en columna B (agencia) y D (cerrados)
    """
    prev_row = target_row_num - 22
    if prev_row < 2:
        return None
    try:
        prev_ag = ws.acell(f"B{prev_row}").value or ""
        if prev_ag.strip() != agency_name.strip():
            return None
        prev_closed = ws.acell(f"D{prev_row}").value
        if prev_closed is None or str(prev_closed).strip() == "":
            return None
        # normaliza a int
        s = str(prev_closed).strip().replace(".", "").replace(",", "")
        return int(float(s))
    except Exception:
        return None

def append_daily_rows_to_sheet(
    sheet_id: str,
    creds_path: str,
    worksheet_name: str,
    rows: List[List[object]],
) -> None:
    import gspread

    gc = get_sheet_client(creds_path)
    sh = gc.open_by_key(sheet_id)

    try:
        ws = sh.worksheet(worksheet_name)
    except Exception:
        ws = sh.add_worksheet(title=worksheet_name, rows="200", cols="26")

    # headers si vacío
    if not ws.acell("A1").value:
        headers = ["Fecha","Agencia","Tickets Abiertos","Tickets Cerrados","Abiertos Ventana","Cerrados Ventana","DS","P3","Total","LW total","Delta 22","Semana","Mes","Año"]
        ws.update(range_name="A1:N1", values=[headers])
        try:
            ws.freeze(rows=1)
        except Exception:
            pass

    last = find_last_row(ws)  # incluye header
    start_row = last + 1
    end_row = start_row + len(rows) - 1
    ensure_rows(ws, end_row)

    # APPEND (sin rango fijo)
    ws.append_rows(rows, value_input_option="USER_ENTERED")
    print(f"[+] Google Sheets: append {len(rows)} filas en '{worksheet_name}' (desde {start_row}).")

# ------------------------------- Ejecución por agencia ---------------------------

async def run_agency(page: Page, base_url: str, ag: Agency, start: datetime, end: datetime) -> Dict[str, int]:
    await login(page, base_url, ag.usuario, ag.password)

    home = await read_home_metrics(page, start, end)

    # cerrados: reintenta si closed_calls = 0
    closed = await read_closed_calls_with_retry(page, start, end)

    return {
        "open_calls": home["open_calls"],
        "closed_calls": closed["closed_calls"],
        "open_window": home["open_window"],
        "closed_window": closed["closed_window"],
        "ds_is": home["ds_is"],
        "p3": home["p3"],
    }

# ------------------------------- Main --------------------------------------------

async def amain():
    base_url, headful, debug, sheet_id, creds, agencies, worksheet_name = load_config()

    if not sheet_id or not creds:
        raise SystemExit("[!] Falta GOOGLE_SHEETS_SPREADSHEET_ID o GOOGLE_APPLICATION_CREDENTIALS en el .env")

    tz = ZoneInfo("Europe/Madrid")
    now_dt = datetime.now(tz)
    report_date = now_dt.date()

    start, end = daily_window(now_dt)  # naive
    # para mostrar
    win_label = f"{start.date().isoformat()} -> {end.date().isoformat()}"
    print(f"[*] Ventana diaria: {win_label} (Europe/Madrid)")

    # Semana / Mes / Año (sobre fecha de ejecución)
    iso = report_date.isocalendar()
    week_str = f"{iso.year}-{iso.week:02d}"
    month_str = f"{report_date.year}-{report_date.month:02d}"
    year_str = f"{report_date.year}"
    date_str = report_date.strftime("%d/%m/%Y")

    results: Dict[str, Dict[str, int]] = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not headful)
        for ag in agencies:
            print(f"[*] {ag.nombre}: iniciando…")
            ctx = await browser.new_context()
            page = await ctx.new_page()
            try:
                metrics = await run_agency(page, base_url, ag, start, end)
                results[ag.nombre] = metrics
                print(
                    f"    ✓ Abiertos={metrics['open_calls']} | Cerrados={metrics['closed_calls']} | "
                    f"OpenWin={metrics['open_window']} | ClosedWin={metrics['closed_window']} | "
                    f"DS+IS={metrics['ds_is']} | P3={metrics['p3']}"
                )
            except Exception as e:
                print(f"    ! Error en {ag.nombre}: {e}")
                results[ag.nombre] = {
                    "open_calls": 0,
                    "closed_calls": 0,
                    "open_window": 0,
                    "closed_window": 0,
                    "ds_is": 0,
                    "p3": 0,
                }
            finally:
                await ctx.close()
        await browser.close()

    # Preparar filas a appendear (una por agencia)
    # OJO: para la fórmula K necesitamos saber el número de fila destino.
    # Como usamos append_rows, calculamos start_row aproximado leyendo last_row antes (con gspread).
    import gspread
    gc = get_sheet_client(creds)
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(worksheet_name)
    except Exception:
        ws = sh.add_worksheet(title=worksheet_name, rows="200", cols="26")
    if not ws.acell("A1").value:
        headers = ["Fecha","Agencia","Tickets Abiertos","Tickets Cerrados","Abiertos Ventana","Cerrados Ventana","DS","P3","Total","LW total","Delta 22","Semana","Mes","Año"]
        ws.update(range_name="A1:N1", values=[headers])
        try:
            ws.freeze(rows=1)
        except Exception:
            pass

    last = find_last_row(ws)          # incluye header
    start_row = last + 1              # primera fila de datos a insertar

    batch_rows: List[List[object]] = []

    # Orden estable por nombre agencia
    agency_names = [a.nombre for a in agencies]

    for i, name in enumerate(agency_names):
        m = results.get(name, {})
        open_calls = int(m.get("open_calls", 0))
        closed_calls = int(m.get("closed_calls", 0))
        open_win = int(m.get("open_window", 0))
        closed_win = int(m.get("closed_window", 0))
        ds_is = int(m.get("ds_is", 0))
        p3 = int(m.get("p3", 0))

        row_num = start_row + i  # fila real donde se appendeará
        total = open_calls + closed_calls
        lw_total = open_win + closed_win

        # Si cerrados = 0, aplicar fallback desde 22 filas arriba (misma agencia)
        if closed_calls == 0:
            fb = closed_fallback_from_22_above(ws, name, row_num)
            if fb is not None:
                closed_calls = fb
                total = open_calls + closed_calls
                # Nota: SOLO cerrados total (col D). No tocamos el resto.

        k_formula = build_k_formula(row_num)

        batch_rows.append([
            date_str,          # A
            name,              # B
            open_calls,         # C
            closed_calls,       # D
            open_win,           # E
            closed_win,         # F
            ds_is,              # G
            p3,                 # H
            total,              # I
            lw_total,           # J
            k_formula,          # K (fórmula)
            week_str,           # L
            month_str,          # M
            year_str,           # N
        ])

    # Append final (asegurando grid rows antes)
    # Reusamos la misma ws que ya abrimos
    end_row = start_row + len(batch_rows) - 1
    ensure_rows(ws, end_row)
    ws.append_rows(batch_rows, value_input_option="USER_ENTERED")
    print(f"[+] OK: appended {len(batch_rows)} filas a '{worksheet_name}' ({start_row}..{end_row}).")

def main():
    try:
        asyncio.run(amain())
    except RuntimeError as e:
        # Para entornos con loop ya activo (p.ej. notebooks)
        if "asyncio.run() cannot be called from a running event loop" in str(e):
            import nest_asyncio
            nest_asyncio.apply()
            loop = asyncio.get_event_loop()
            loop.run_until_complete(amain())
        else:
            raise

if __name__ == "__main__":
    main()

