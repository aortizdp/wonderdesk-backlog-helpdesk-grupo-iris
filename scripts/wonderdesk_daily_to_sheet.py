#!/usr/bin/env python3
"""
WonderDesk — Daily → Google Sheets (DATOS-Daily)
-----------------------------------------------
- Multi-agència, login WonderDesk i extracció mètriques diàries.
- **DS** ara suma **DS + IS** (amb número al darrere).
- Finestra diària: **ahir** (00:00–24:00). Si és **dilluns**, compta **divendres + cap de setmana**.
- Escriu a la fulla **DATOS-Daily** del Google Sheets amb columnes:
  A Fecha (DD/MM/YYYY)
  B AGENCIA
  C Tickets Abiertos
  D Tickets Cerrados
  E Abiertos Última Semana   (ús diari: oberts ahir / o divendres+cap setmana)
  F Cerrados Última Semana   (ús diari: tancats ahir / o divendres+cap setmana)
  G DS   (compta DS+IS)
  H P3
  I Total = C + D
  J LW total = E + F
  K =IFNA(D[f] - D[f-22];0)   ← diferencia vs fa 22 files (paràmetre `;`)
  L Semana = YYYY-SS
  M Mes = YYYY-MM
  N Año = YYYY

- **Fallback Cerrados=0**: si el valor de D (tancats) calculat és 0, es copia el valor de **D[f-22]** (si existeix).
- Expansió automàtica de files de la pestanya si cal.
- Correcció `gspread.update(values=..., range_name=...)` per evitar DeprecationWarning.

Dependències:
  pip install -U python-dotenv playwright python-dateutil gspread google-auth nest_asyncio
  python -m playwright install chromium

Config .env (exemple):
  HELPDESK_BASE_URL=https://helpdesk.grupoiris.net
  HEADFUL=false
  AGENCIES=ACVIAJES,ADRIANO
  ACVIAJES_NOMBRE=AC Viajes
  ACVIAJES_USUARIO=...
  ACVIAJES_PASSWORD=...
  ADRIANO_NOMBRE=Adriano
  ADRIANO_USUARIO=...
  ADRIANO_PASSWORD=...
  GOOGLE_SHEETS_SPREADSHEET_ID=...
  GOOGLE_APPLICATION_CREDENTIALS=/Users/cio/Helpdesk/.login-gsheets.json
"""
from __future__ import annotations

import os
import re
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import List, Dict, Tuple
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from dateutil import parser as dtparse
from playwright.async_api import async_playwright, Page

# ========================= Config / Constants =========================
TZ = ZoneInfo("Europe/Madrid")
DAILY_ROW_STRIDE = 22  # diferència de 22 files per a la columna K
SHEET_NAME = "DATOS-Daily"

RX_ISSUE = re.compile(r"(?i)\b(?:DS|IS)\s*-?\s*\d+")  # DS123, IS-1234, etc.
RX_P3    = re.compile(r"(?i)\bP\s*3(\b|\.)")

# ========================= ENV helpers =========================

def env_bool(name: str, default: bool=False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1","true","yes","y","on"}


def get_agencies_from_env():
    load_dotenv()
    base_url = os.getenv("HELPDESK_BASE_URL", "https://helpdesk.grupoiris.net").rstrip("/")
    headful  = env_bool("HEADFUL", False)
    raw = os.getenv("AGENCIES", "").strip()
    ag_list = []
    if raw:
        codes = [c.strip().upper() for c in raw.split(',') if c.strip()]
        for code in codes:
            nombre   = os.getenv(f"{code}_NOMBRE") or os.getenv(f"{code}_NOM") or code
            usuario  = os.getenv(f"{code}_USUARIO") or os.getenv(f"{code}_USERNAME")
            password = os.getenv(f"{code}_PASSWORD")
            ag_list.append({"code": code, "nombre": nombre, "usuario": usuario, "password": password})
    else:
        company = os.getenv("COMPANY", "").strip().upper()
        if not company:
            raise SystemExit("Falta AGENCIES=... o COMPANY=... al .env")
        usuario  = os.getenv(f"{company}_USUARIO") or os.getenv(f"{company}_USERNAME")
        password = os.getenv(f"{company}_PASSWORD")
        nombre = os.getenv(f"{company}_NOMBRE") or company
        ag_list = [{"code": company, "nombre": nombre, "usuario": usuario, "password": password}]
    # Google Sheets
    sheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    creds    = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    return base_url, headful, ag_list, sheet_id, creds

# ========================= Dates / finestres =========================

def today_tz() -> datetime:
    return datetime.now(TZ)


def window_yesterday_or_weekend() -> Tuple[datetime, datetime, date]:
    """Retorna (start_dt, end_dt, label_date) per la finestra diària.
    - Dies laborables (no dilluns): ahir 00:00 → avui 00:00. label_date = ahir (data de la fila)
    - Dilluns: divendres 00:00 → avui (dilluns) 00:00. label_date = dilluns-1 (ahir), però agregant cap de setmana
    Nota: el label (col A) mostra la **data d'avui en format DD/MM/YYYY** per coherència diària.
    """
    now = today_tz().replace(hour=0, minute=0, second=0, microsecond=0)
    weekday = now.weekday()  # dilluns=0 ... diumenge=6
    if weekday == 0:
        # dilluns: de divendres 00:00 a dilluns 00:00
        start = (now - timedelta(days=3))
        end   = now
        label = now  # posem la data d'avui (dilluns)
    else:
        start = (now - timedelta(days=1))
        end   = now
        label = now
    return start, end, label.date()


def fmt_ddmmyyyy(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def week_year_weeknum(d: date) -> str:
    # setmana ISO (dl=1)
    iso = d.isocalendar()  # (year, week, weekday)
    return f"{iso[0]}-{iso[1]:02d}"

# ========================= Navegació WonderDesk =========================

async def login(page: Page, base_url: str, usuario: str, password: str) -> None:
    await page.goto(f"{base_url}/wonderdesk.cgi", wait_until="load")

    async def attempt(ctx) -> bool:
        try:
            pw_loc = ctx.locator("input[type='password']")
            if await pw_loc.count() > 0:
                pw = pw_loc.first
                for sel in ["input[name*='user' i]","input[name*='login' i]","input[type='email']","input[type='text']","input:not([type])"]:
                    loc = ctx.locator(sel)
                    if await loc.count() > 0:
                        await loc.first.fill(usuario)
                        await pw.fill(password)
                        clicked = False
                        for b in ["input[type='submit']","button[type='submit']","input[type='image']","button:has-text('Login')","button:has-text('Entrar')","button:has-text('Acceder')"]:
                            bloc = ctx.locator(b)
                            if await bloc.count() > 0:
                                await bloc.first.click(); clicked=True; break
                        if not clicked:
                            await pw.press("Enter")
                        await ctx.wait_for_load_state("networkidle", timeout=10000)
                        return True
        except Exception:
            pass
        try:
            await ctx.get_by_label(re.compile(r"^(User|Usuario)", re.I)).fill(usuario)
            await ctx.get_by_label(re.compile(r"^(Password|Clave|Contraseña)", re.I)).fill(password)
            await ctx.keyboard.press("Enter")
            await ctx.wait_for_load_state("networkidle", timeout=8000)
            return True
        except Exception:
            return False

    if not await attempt(page):
        for fr in page.frames:
            try:
                if await attempt(fr):
                    break
            except Exception:
                pass

    if await page.locator("input[type='password']").count() > 0:
        raise RuntimeError("No s'ha pogut iniciar sessió")


async def click_menu(page: Page, names: List[str]) -> None:
    for n in names:
        try:
            await page.get_by_role("link", name=re.compile(rf"^{re.escape(n)}$", re.I)).click(timeout=1500)
            await page.wait_for_load_state("networkidle", timeout=8000)
            return
        except Exception:
            try:
                await page.get_by_text(re.compile(n, re.I), exact=False).click(timeout=1500)
                await page.wait_for_load_state("networkidle", timeout=8000)
                return
            except Exception:
                continue
    raise RuntimeError(f"No s'ha trobat cap enllaç: {names}")


JS_FIND_ROWS = r"""
() => {
  const norm = (s) => (s||'').replace(/\s+/g,' ').trim();
  const out = [];
  const tables = Array.from(document.querySelectorAll('table'));
  for (const t of tables){
    const rows = Array.from(t.querySelectorAll(':scope > tbody > tr, :scope > tr'));
    if (!rows.length) continue;
    const heads = Array.from(rows[0].querySelectorAll('th,td')).map(c=>norm(c.innerText));
    const headsL = heads.map(h=>h.toLowerCase());
    const hasID = headsL.some(h => /^id$/.test(h));
    const hasDT = headsL.some(h => /(fecha|date)/.test(h));
    const hasCS = headsL.some(h => /(category.*subject|categoria.*(subject|asunto)|category|categoria)/.test(h));
    if (!(hasID && hasDT && hasCS)) continue;

    const idx={}; headsL.forEach((h,i)=>idx[h]=i);
    const find=(rx)=>{const k=Object.keys(idx).find(k=>rx.test(k)); return k!=null? idx[k]:-1};
    let iID=find(/^id$/i), iDT=find(/fecha|date/i), iCS=find(/category.*subject|categoria.*(subject|asunto)|category|categoria/i);
    if (iID<0 && heads.length>=2) iID=1;
    if (iDT<0 && heads.length>=3) iDT=2;
    if (iCS<0 && heads.length>=6) iCS=5;

    for (let r=1;r<rows.length;r++){
      const tds=Array.from(rows[r].querySelectorAll('td')); if(!tds.length) continue;
      const get=(i)=> (i>=0 && i<tds.length)? norm(tds[i].innerText):'';
      const idTxt=get(iID), dateTxt=get(iDT);
      let categoryTxt=get(iCS), subjectTxt='';
      const catCell = (iCS>=0 && iCS<tds.length)? tds[iCS]:null;
      if (catCell){ const a=catCell.querySelector('a'); subjectTxt=a? norm(a.textContent): categoryTxt; }
      out.push({id:idTxt, date:dateTxt, category:categoryTxt, subject:subjectTxt});
    }
  }
  return out;
}
"""

# ========================= Mètriques per agència =========================

@dataclass
class AgencyDaily:
    agency: str
    open_total: int
    closed_total: int
    open_window: int
    closed_window: int
    ds_total: int
    p3_total: int


def _parse_date(s: str):
    s = (s or '').strip()
    if not s:
        return None
    try:
        dt = dtparse.parse(s, dayfirst=False, fuzzy=True)
        return datetime(dt.year, dt.month, dt.day, tzinfo=TZ)
    except Exception:
        return None


async def read_home_metrics(page: Page) -> Tuple[int,int,int]:
    """Retorna (open_total, ds_total, p3_total) a HOME/Inicio i també `open_rows`.
       Nota: `open_total` és el nombre de files de la taula d'oberts.
    """
    try:
        await click_menu(page, ["Home","Inicio"])  
    except Exception:
        pass

    open_rows: List[Dict] = []
    for _ in range(500):
        try:
            rows = await page.evaluate(JS_FIND_ROWS)
        except Exception:
            rows = []
        if rows:
            for r in rows:
                rid   = (r.get('id') or '').strip()
                subj  = (r.get('subject') or '').strip()
                cat   = (r.get('category') or '').strip()
                if not (rid and (subj or cat)):
                    continue
                open_rows.append(r)
        # next
        moved = False
        for rx in [r"Next", r"Siguiente", r"Siguiente >", r"^>$", r"»"]:
            try:
                await page.get_by_role("link", name=re.compile(rx, re.I)).click(timeout=900)
                await page.wait_for_load_state("networkidle", timeout=6000)
                moved=True; break
            except Exception:
                continue
        if not moved:
            try:
                anchors = page.locator('a')
                texts = await anchors.all_text_contents()
                for i, t in enumerate(texts):
                    if t and t.strip() in {'>', '[>]', 'Siguiente >'}:
                        await anchors.nth(i).click(timeout=900)
                        await page.wait_for_load_state("networkidle", timeout=6000)
                        moved=True; break
            except Exception:
                pass
        if not moved:
            break

    # Comptes
    open_total = len(open_rows)
    ds_total = 0
    p3_total = 0
    for r in open_rows:
        target = (r.get('subject') or r.get('category') or '')
        if RX_ISSUE.search(target):
            ds_total += 1
        if RX_P3.search(target):
            p3_total += 1
    return open_total, ds_total, p3_total


async def read_closed_metrics(page: Page, start: datetime, end: datetime) -> Tuple[int,int]:
    """Retorna (closed_total, closed_window) a LIST CLOSED.
       Tira a "[>>]" i recorre cap enrere comptant dins la finestra.
    """
    try:
        await click_menu(page, ["List Closed","List Cerrado","List Cerrados","Cerrado","Closed"])  
    except Exception:
        pass

    # vés a la darrera pàgina amb [>>]
    jumped = False
    for rx in [r"\[>>\]", r"^>>$", r"Last", r"Final"]:
        try:
            await page.get_by_role("link", name=re.compile(rx, re.I)).click(timeout=900)
            await page.wait_for_load_state("networkidle", timeout=6000)
            jumped=True; break
        except Exception:
            continue
    if not jumped:
        try:
            anchors = page.locator('a')
            texts = await anchors.all_text_contents()
            for i, t in enumerate(texts):
                if t and t.strip() in {']]', '>>', '[>>]'}:
                    await anchors.nth(i).click(timeout=900)
                    await page.wait_for_load_state("networkidle", timeout=6000)
                    jumped=True; break
        except Exception:
            pass

    closed_total = 0
    closed_window = 0

    # recórrer cap enrere
    for _ in range(600):
        try:
            rows = await page.evaluate(JS_FIND_ROWS)
        except Exception:
            rows = []
        if rows:
            closed_total += len(rows)
            for r in rows:
                dt = _parse_date(r.get('date',''))
                if dt and (start <= dt < end):
                    closed_window += 1
        # pàgina anterior
        moved = False
        for rx in [r"\[<\]", r"^<$", r"Anterior", r"Previous"]:
            try:
                await page.get_by_role("link", name=re.compile(rx, re.I)).click(timeout=900)
                await page.wait_for_load_state("networkidle", timeout=6000)
                moved=True; break
            except Exception:
                continue
        if not moved:
            try:
                anchors = page.locator('a')
                texts = await anchors.all_text_contents()
                for i, t in enumerate(texts):
                    if t and t.strip() in {'<', '[<]'}:
                        await anchors.nth(i).click(timeout=900)
                        await page.wait_for_load_state("networkidle", timeout=6000)
                        moved=True; break
            except Exception:
                pass
        if not moved:
            break

    return closed_total, closed_window


async def collect_for_agency(page: Page, base_url: str, ag: Dict, start: datetime, end: datetime) -> AgencyDaily:
    nombre   = ag.get('nombre') or ag.get('code')
    usuario  = ag.get('usuario')
    password = ag.get('password')
    if not usuario or not password:
        raise RuntimeError(f"{nombre}: falten credencials")

    await login(page, base_url, usuario, password)

    open_total, ds_total, p3_total = await read_home_metrics(page)
    closed_total, closed_window   = await read_closed_metrics(page, start, end)

    # open_window = comptem oberts dins la finestra? (Home no porta estat data d'obertura fiable)
    # Mantindrem 0 o, si tens una columna d'obertura, aquí la pots calcular. Per coherència, deixem 0.
    open_window = 0

    return AgencyDaily(
        agency=nombre,
        open_total=open_total,
        closed_total=closed_total,
        open_window=open_window,
        closed_window=closed_window,
        ds_total=ds_total,
        p3_total=p3_total,
    )

# ========================= Google Sheets =========================

def append_daily_rows_to_sheet(rows: List[AgencyDaily], label_day: date, sheet_id: str, creds_path: str):
    try:
        import gspread
    except ImportError:
        raise SystemExit("[!] Falta gspread/google-auth. pip install gspread google-auth")

    gc = gspread.service_account(filename=os.path.expanduser(creds_path))
    sh = gc.open_by_key(sheet_id)

    # crea la pestanya si no existeix
    try:
        ws = sh.worksheet(SHEET_NAME)
    except Exception:
        ws = sh.add_worksheet(title=SHEET_NAME, rows="200", cols="30")
        headers = [
            "Fecha","AGENCIA","Tickets Abiertos","Tickets Cerrados",
            "Abiertos Última Semana","Cerrados Última Semana",
            "DS","P3","Total","LW total","ΔD(22)","Semana","Mes","Año"
        ]
        ws.update("A1", [headers])
        try:
            ws.freeze(rows=1)
        except Exception:
            pass

    # on començarem a escriure
    last_filled = len(ws.col_values(1))  # ultima fila amb dades a col A
    start_row = max(last_filled + 1, 2)

    batch: List[List] = []
    fecha_str = fmt_ddmmyyyy(label_day)
    semana    = week_year_weeknum(label_day)
    mes       = label_day.strftime("%Y-%m")
    anyo      = label_day.strftime("%Y")

    for r in rows:
        C = int(r.open_total)
        D = int(r.closed_total)
        E = int(r.open_window)
        F = int(r.closed_window)
        G = int(r.ds_total)
        H = int(r.p3_total)
        I = C + D
        J = E + F
        batch.append([
            fecha_str,      # A
            r.agency,       # B
            C,              # C
            D,              # D
            E,              # E
            F,              # F
            G,              # G (DS+IS)
            H,              # H
            I,              # I = C+D
            J,              # J = E+F
            None,           # K (formula per fila al final)
            semana,         # L
            mes,            # M
            anyo,           # N
        ])

    # Assegura capacitat grid
    end_row = start_row + len(batch) - 1
    if end_row > ws.row_count:
        ws.add_rows(end_row - ws.row_count)

    # Escriu A..N sense la columna K
    ws.update(
        range_name=f"A{start_row}:N{end_row}",
        values=batch,
        value_input_option='USER_ENTERED'
    )

    # Inserta la fórmula K per a cada fila i aplica fallback D si és 0
    # NOTA: cal saber la fila real
    updates = []
    for i in range(len(batch)):
        row_idx = start_row + i
        # Fallback Cerrados (D): si 0 → copia D[row-DAILY_ROW_STRIDE] (si existeix)
        try:
            d_val = ws.acell(f"D{row_idx}").value
            if d_val in (None, "", "0") and row_idx - DAILY_ROW_STRIDE >= 2:
                prev = ws.acell(f"D{row_idx-DAILY_ROW_STRIDE}").value
                if prev not in (None,""):
                    ws.update(range_name=f"D{row_idx}", values=[[prev]], value_input_option='USER_ENTERED')
        except Exception:
            pass
        # K: =IFNA(D[row]-D[row-22];0)
        formula = f"=IFNA(D{row_idx}-D{row_idx-DAILY_ROW_STRIDE};0)"
        updates.append([formula])
    ws.update(
        range_name=f"K{start_row}:K{end_row}",
        values=updates,
        value_input_option='USER_ENTERED'
    )

    print(f"[+] Afegides {len(batch)} files a '{SHEET_NAME}' (files {start_row}-{end_row})")

# ========================= MAIN =========================

async def amain():
    base_url, headful, agencies, sheet_id, creds = get_agencies_from_env()

    start, end, label = window_yesterday_or_weekend()

    results: List[AgencyDaily] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not headful)
        for ag in agencies:
            ctx = await browser.new_context()
            page = await ctx.new_page()
            try:
                print(f"[*] {ag.get('nombre') or ag.get('code')}: iniciant…")
                res = await collect_for_agency(page, base_url, ag, start, end)
                results.append(res)
                print(f"    [+] open={res.open_total} closed={res.closed_total} ds/is={res.ds_total} p3={res.p3_total}")
            except Exception as e:
                print(f"    [!] {ag.get('nombre') or ag.get('code')}: error — {e}")
            finally:
                await ctx.close()
        await browser.close()

    if not sheet_id or not creds:
        raise SystemExit("[!] Falta GOOGLE_SHEETS_SPREADSHEET_ID o GOOGLE_APPLICATION_CREDENTIALS al .env")

    append_daily_rows_to_sheet(results, label_day=label, sheet_id=sheet_id, creds_path=creds)


if __name__ == '__main__':
    try:
        asyncio.run(amain())
    except RuntimeError as e:
        if "asyncio.run() cannot be called from a running event loop" in str(e):
            import nest_asyncio
            nest_asyncio.apply()
            import asyncio as _asyncio
            _asyncio.get_event_loop().run_until_complete(amain())
        else:
            raise

