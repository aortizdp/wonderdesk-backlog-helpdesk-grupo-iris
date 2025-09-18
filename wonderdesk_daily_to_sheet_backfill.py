#!/usr/bin/env python3
"""
WonderDesk — Append diari al Google Sheets (pestanya "DATOS-Daily")

Modes de funcionament:
  1) **Diari per defecte**: finestra "intel·ligent" (ahir; o dilluns = divendres→dilluns).
  2) **Backfill per dates**: un sol dia (`--date YYYY-MM-DD`) o un rang (`--start YYYY-MM-DD --end YYYY-MM-DD`).
     En backfill, cada dia es tracta **individualment** (00:00→24:00) *sense* la regla especial de dilluns.

Les capçaleres de la pestanya es mantenen per compatibilitat:
  E = "Abiertos Última Semana"  (ara: finestra diària)
  F = "Cerrados Última Semana"  (ara: finestra diària)

Columnes A–N escrites:
 A. Fecha (DD/MM/YYYY)
 B. AGENCIA
 C. Tickets Abiertos
 D. Tickets Cerrados
 E. Abiertos Última Semana (finestra diària)
 F. Cerrados Última Semana (finestra diària)
 G. DS
 H. P3
 I. Total (= C + D)
 J. LW total (= E + F)
 K. =IFNA(D[actual]-D[actual-22];0)
 L. Semana (YYYY-SS)
 M. Mes (YYYY-MM)
 N. Año (YYYY)

Ús:
  python wonderdesk_daily_to_sheet_backfill.py                     # mode diari
  python wonderdesk_daily_to_sheet_backfill.py --date 2025-01-06   # un dia
  python wonderdesk_daily_to_sheet_backfill.py --start 2025-01-06 --end 2025-01-08  # rang

CONFIG (.env):
  HELPDESK_BASE_URL=https://helpdesk.grupoiris.net
  HEADFUL=false

  # Multi-agències
  AGENCIES=AG1,AG2,AG3
  AG1_NOMBRE=Agència 1
  AG1_USUARIO=usuari1
  AG1_PASSWORD=contrasenya1
  ...

  # Alternativa 1 agència
  COMPANY=TAG
  TAG_USERNAME=usuari
  TAG_PASSWORD=contrasenya
  TAG_NOMBRE=Nom Visible (opcional)

  # Google Sheets
  GOOGLE_SHEETS_SPREADSHEET_ID=...
  GOOGLE_APPLICATION_CREDENTIALS=/ruta/absoluta/.login-gsheets.json
  SHEET_DAILY_TAB=DATOS-Daily   # opcional

Dependències:
  pip install -U python-dotenv playwright python-dateutil gspread google-auth
  python -m playwright install chromium
"""
from __future__ import annotations

import os, re, argparse, asyncio
from datetime import datetime, timedelta, date as date_cls
from typing import List, Dict, Optional, Iterable
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from dateutil import parser as dtparse
from playwright.async_api import async_playwright, Page

# ---------- ENV helpers ----------

def env_bool(name: str, default: bool=False) -> bool:
    v = os.getenv(name)
    if v is None: return default
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
    return base_url, headful, ag_list

# ---------- Finestra temporal ----------

def window_for_monday_logic(today: date_cls):
    """Mode automàtic: dilluns = divendres→dilluns; resta = ahir."""
    if today.weekday() == 0:  # Monday
        start = datetime.combine(today - timedelta(days=3), datetime.min.time())
    else:
        start = datetime.combine(today - timedelta(days=1), datetime.min.time())
    end = datetime.combine(today, datetime.min.time())
    return start, end


def window_for_exact_day(d: date_cls):
    """Un dia 00:00→24:00 (sense lògica de dilluns)."""
    start = datetime.combine(d, datetime.min.time())
    end = start + timedelta(days=1)
    return start, end

# ---------- Parsers i navegació WonderDesk ----------

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
                        if not clicked: await pw.press("Enter")
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
                if await attempt(fr): break
            except Exception: pass
    if await page.locator("input[type='password']").count() > 0:
        raise RuntimeError("No s'ha pogut iniciar sessió")

async def click_menu(page: Page, names: List[str]) -> None:
    for n in names:
        try:
            await page.get_by_role("link", name=re.compile(rf"^{re.escape(n)}$", re.I)).click(timeout=1500)
            await page.wait_for_load_state("networkidle", timeout=8000); return
        except Exception:
            try:
                await page.get_by_text(re.compile(n, re.I), exact=False).click(timeout=1500)
                await page.wait_for_load_state("networkidle", timeout=8000); return
            except Exception:
                continue
    raise RuntimeError(f"No s'ha trobat cap enllaç: {names}")

async def read_calls_banner(ctx) -> int:
    txt = await ctx.evaluate("() => document.body.innerText")
    m = re.search(r"(\d+)\s+Calls", txt, flags=re.I)
    return int(m.group(1)) if m else 0

JS_FIND_ROWS = r"""
() => {
  const norm = (s) => (s||'').replace(/\s+/g,' ').trim();
  const result = { ok:false, rows:[], meta:{} };
  const tables = Array.from(document.querySelectorAll('table'));
  const good=[];
  for (const t of tables){
    const rows = Array.from(t.querySelectorAll(':scope > tbody > tr, :scope > tr'));
    if (!rows.length) continue;
    let heads = Array.from(rows[0].querySelectorAll('th,td')).map(c=>norm(c.innerText).toLowerCase());
    if (!heads.length) continue;
    const hasID=heads.some(x=>/^id$/.test(x));
    const hasDT=heads.some(x=>/fecha|date/.test(x));
    const hasCS=heads.some(x=>/category.*subject|categoria.*(subject|asunto)|category|categoria/.test(x));
    if (!(hasID && hasDT && hasCS)) continue;
    const idx={}; heads.forEach((h,i)=>idx[h]=i);
    const find=(rx)=>{const k=Object.keys(idx).find(k=>rx.test(k)); return k!=null? idx[k]:-1};
    let iID=find(/^id$/i), iDT=find(/fecha|date/i), iCS=find(/category.*subject|categoria.*(subject|asunto)|category|categoria/i);
    if (iID<0 && heads.length>=2) iID=1; if (iDT<0 && heads.length>=3) iDT=2; if (iCS<0 && heads.length>=6) iCS=5;
    const data=[];
    for (let r=1;r<rows.length;r++){
      const tds=Array.from(rows[r].querySelectorAll('td')); if(!tds.length) continue;
      const get=(i)=> (i>=0 && i<tds.length)? norm(tds[i].innerText):'';
      const idTxt=get(iID), dateTxt=get(iDT);
      let categoryTxt=get(iCS), subjectTxt='';
      const catCell = (iCS>=0 && iCS<tds.length)? tds[iCS]:null;
      if (catCell){ const a=catCell.querySelector('a'); subjectTxt=a? norm(a.textContent): categoryTxt; }
      data.push({id:idTxt, date:dateTxt, category:categoryTxt, subject:subjectTxt});
    }
    if (data.length) good.push({data});
  }
  if (good.length){ good.sort((a,b)=> b.data.length - a.data.length); return {ok:true, rows:good[0].data}; }
  return result;
}
"""

async def extract_table_any(ctx) -> List[Dict[str,str]]:
    try:
        res = await ctx.evaluate(JS_FIND_ROWS)
        if res and res.get('ok') and res.get('rows'): return res['rows']
    except Exception:
        pass
    if isinstance(ctx, Page):
        for fr in ctx.frames:
            try:
                res = await fr.evaluate(JS_FIND_ROWS)
                if res and res.get('ok') and res.get('rows'): return res['rows']
            except Exception:
                continue
    return []

def parse_date_any(s: str) -> Optional[datetime]:
    s=(s or '').strip()
    if not s:
        return None
    try:
        return dtparse.parse(s, dayfirst=False, fuzzy=True)
    except Exception:
        m=re.search(r"([A-Za-zÀ-ÿ]{3,}\s+\d{1,2}\s+\d{4})", s)
        if m:
            try:
                return dtparse.parse(m.group(1), dayfirst=False, fuzzy=True)
            except Exception:
                return None
        return None

async def click_last_page(page: Page) -> bool:
    for sel in ["a:has-text('>>')", "a:has-text('[>>]')", "a:has-text('»»')"]:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.first.click(timeout=1500)
                await page.wait_for_load_state("networkidle", timeout=8000)
                return True
        except Exception:
            pass
    try:
        await page.get_by_role('link', name=re.compile(r"»»|>>", re.I)).first.click(timeout=1500)
        await page.wait_for_load_state("networkidle", timeout=8000)
        return True
    except Exception:
        return False

async def click_prev_page(page: Page) -> bool:
    for sel in ["a:has-text('<<')", "a:has-text('[<]')", "a:has-text('<')", "a:has-text('««')"]:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.last.click(timeout=1500)
                await page.wait_for_load_state("networkidle", timeout=8000)
                return True
        except Exception:
            pass
    try:
        await page.get_by_role('link', name=re.compile(r"««|<<|\[<]|^\s*<\s*$", re.I)).first.click(timeout=1500)
        await page.wait_for_load_state("networkidle", timeout=8000)
        return True
    except Exception:
        return False

async def click_next_page(page: Page) -> bool:
    for rx in [r"Next", r"Siguiente", r"Avanzar", r"^>$", r"Siguiente >", r"»"]:
        try:
            await page.get_by_role("link", name=re.compile(rx, re.I)).click(timeout=1200)
            await page.wait_for_load_state("networkidle", timeout=8000)
            return True
        except Exception:
            continue
    try:
        anchors = page.locator('a')
        texts = await anchors.all_text_contents()
        for i, t in enumerate(texts):
            if t and t.strip() in {'>', '[>]', 'Siguiente >'}:
                await anchors.nth(i).click(timeout=1200)
                await page.wait_for_load_state("networkidle", timeout=8000)
                return True
    except Exception:
        pass
    return False

# ---------- Comptes (Home i Closed) sobre una finestra donada ----------

async def read_home_metrics(page: Page, start: datetime, end: datetime):
    try:
        await click_menu(page, ["Home","Inicio"]) 
    except Exception:
        await page.goto(page.url.split('?')[0], wait_until="load")
    open_calls = await read_calls_banner(page)
    rows = await extract_table_any(page)

    bugs=p3=0
    win_open = 0
    for r in rows:
        target=(r.get('subject') or r.get('category') or '').strip()
        if re.search(r'(?i)\bDS\s*-?\s*\d+', target):
            bugs += 1
        if re.search(r'(?i)\bP\s*3(\b|\.)', target):
            p3 += 1
        d=parse_date_any(r.get('date',''))
        if d and start <= d < end:
            win_open += 1
    return {"open_calls": open_calls, "bugs": bugs, "p3": p3, "win_open": win_open}

async def read_closed_window(page: Page, base_url: str, start: datetime, end: datetime):
    try:
        await click_menu(page, ["List Closed","List Cerrado","List Closed ","List Cerrados"]) 
    except Exception:
        await page.goto(f"{base_url}/wonderdesk.cgi?do=hd_list&help_status=Closed", wait_until="load")
    closed_calls = await read_calls_banner(page)

    total = 0

    if await click_last_page(page):
        for _ in range(200):
            rows = await extract_table_any(page)
            if not rows:
                break
            dmax=None
            for r in rows:
                d=parse_date_any(r.get('date',''))
                if d and (dmax is None or d>dmax):
                    dmax=d
            if dmax is not None and dmax < start:
                break  # pàgina massa antiga
            for r in rows:
                d=parse_date_any(r.get('date',''))
                if d and start <= d < end:
                    total += 1
            if not await click_prev_page(page):
                break
    else:
        for _ in range(200):
            rows = await extract_table_any(page)
            if not rows:
                break
            any_in=False; oldest=None
            for r in rows:
                d=parse_date_any(r.get('date',''))
                if not d:
                    continue
                if oldest is None or d<oldest:
                    oldest=d
                if start <= d < end:
                    total += 1; any_in=True
            if oldest is not None and ((oldest < start and not any_in) or (oldest >= end and not any_in)):
                break
            if not await click_next_page(page):
                break

    return {"closed_calls": closed_calls, "win_closed": total}

# ---------- Google Sheets append ----------

def append_daily_rows_to_sheet(rows: List[Dict[str,object]]):
    try:
        import gspread
    except ImportError:
        raise SystemExit("[!] Falta gspread/google-auth. pip install gspread google-auth")
    sheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    tab = os.getenv("SHEET_DAILY_TAB", "DATOS-Daily")
    if not sheet_id or not creds:
        raise SystemExit("[!] Falta GOOGLE_SHEETS_SPREADSHEET_ID o GOOGLE_APPLICATION_CREDENTIALS al .env")
    creds = os.path.expanduser(creds)
    gc = gspread.service_account(filename=creds)
    sh = gc.open_by_key(sheet_id)

    try:
        ws = sh.worksheet(tab)
    except Exception:
        ws = sh.add_worksheet(title=tab, rows="200", cols="30")
        ws.update('A1', [[
            'Fecha','AGENCIA','Tickets Abiertos','Tickets Cerrados','Abiertos Última Semana','Cerrados Última Semana','DS','P3','Total','LW total','Δ Cerrados(-22d)','Semana','Mes','Año'
        ]])
        try:
            ws.freeze(rows=1)
        except Exception:
            pass

    existing_rows = len(ws.col_values(1))
    start_row = existing_rows + 1

    batch = []
    for i, r in enumerate(rows):
        rownum = start_row + i
        fecha_str = r.get('FechaStr')
        semana    = r.get('Semana')
        mes       = r.get('Mes')
        anio      = r.get('Año')
        C = int(r.get('Tickets Abiertos', 0) or 0)
        D = int(r.get('Tickets Cerrados', 0) or 0)
        E = int(r.get('Abiertos Última Semana', 0) or 0)
        F = int(r.get('Cerrados Última Semana', 0) or 0)
        G = int(r.get('DS', 0) or 0)
        H = int(r.get('P3', 0) or 0)
        total_formula = f"=C{rownum}+D{rownum}"
        lw_formula = f"=E{rownum}+F{rownum}"
        delta22 = f"=IFNA(D{rownum}-D{rownum-22};0)" if rownum>22 else "0"
        batch.append([
            fecha_str,
            r.get('Nombre Agencia',''),
            C, D, E, F, G, H,
            total_formula,
            lw_formula,
            delta22,
            semana,
            mes,
            anio,
        ])

    end_row = start_row + len(batch) - 1
    if batch:
        ws.update(f'A{start_row}:N{end_row}', batch, value_input_option='USER_ENTERED')
        print(f"[+] Afegides {len(batch)} files a '{tab}' (files {start_row}-{end_row})")
    else:
        print("[i] Cap fila a afegir")

# ---------- Utilitats ----------

def daterange_inclusive(start: date_cls, end: date_cls) -> Iterable[date_cls]:
    d = start
    delta = timedelta(days=1)
    while d <= end:
        yield d
        d += delta

# ---------- MAIN ----------

async def amain():
    ap = argparse.ArgumentParser(description='WonderDesk → Google Sheets (diari / backfill)')
    ap.add_argument('--date', help='Processar un sol dia (YYYY-MM-DD)')
    ap.add_argument('--start', help='Inici del rang (YYYY-MM-DD)')
    ap.add_argument('--end', help='Final del rang (YYYY-MM-DD) — inclusiu')
    args = ap.parse_args()

    base_url, headful, ags = get_agencies_from_env()

    # Dies a processar
    days: List[date_cls] = []
    if args.date:
        days = [datetime.strptime(args.date, '%Y-%m-%d').date()]
    elif args.start and args.end:
        d0 = datetime.strptime(args.start, '%Y-%m-%d').date()
        d1 = datetime.strptime(args.end,   '%Y-%m-%d').date()
        if d1 < d0:
            d0, d1 = d1, d0
        days = list(daterange_inclusive(d0, d1))
    else:
        days = [datetime.now().date()]  # mode diari

    results_rows: List[Dict[str,object]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not headful)
        for a in ags:
            nombre=a.get('nombre') or a.get('code'); usuario=a.get('usuario'); password=a.get('password')
            if not usuario or not password:
                print(f"[!] {nombre}: falten credencials")
                continue
            ctx = await browser.new_context(); page = await ctx.new_page()
            try:
                await login(page, base_url, usuario, password)
                for today in days:
                    # finestra per aquest dia
                    if args.date or (args.start and args.end):
                        start, end = window_for_exact_day(today)
                    else:
                        start, end = window_for_monday_logic(today)
                    home   = await read_home_metrics(page, start, end)
                    closed = await read_closed_window(page, base_url, start, end)

                    tz_dt = datetime(today.year, today.month, today.day, tzinfo=ZoneInfo('Europe/Madrid'))
                    fecha_str = tz_dt.strftime('%d/%m/%Y')
                    year, week, _ = tz_dt.isocalendar()
                    semana = f"{year}-{week:02d}"
                    mes = tz_dt.strftime('%Y-%m')
                    anio = tz_dt.strftime('%Y')

                    results_rows.append({
                        'Nombre Agencia': nombre,
                        'FechaStr': fecha_str,
                        'Semana': semana,
                        'Mes': mes,
                        'Año': anio,
                        'Tickets Abiertos': home['open_calls'],
                        'Tickets Cerrados': closed['closed_calls'],
                        'Abiertos Última Semana': home['win_open'],
                        'Cerrados Última Semana': closed['win_closed'],
                        'DS': home['bugs'],
                        'P3': home['p3'],
                    })
            except Exception as e:
                print(f"[!] {nombre}: {e}")
            finally:
                await ctx.close()
        await browser.close()

    append_daily_rows_to_sheet(results_rows)

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

