#!/usr/bin/env python3
"""
WonderDesk — Exportar **tots els tickets oberts** de cada agència a Google Sheets (v4)

Correcció: s'eviten **línies en blanc** al full — només es pujen files amb **ID, Fecha i Subject** no buits.

Canvis principals (respecte v3):
- Filtre de validesa de fila (requereix `id` + `date` + `subject`).
- Doble filtre (abans d'acumular i abans d'escriure al Sheet) per garantir que no s'escriuen files incompletes.

Columnes al Sheet: `AGENCIA, ID, Fecha (DD/MM/YYYY), Mes (YYYY-MM), Año (YYYY), Subject`.

Ús:
  python wonderdesk_open_tickets_to_sheet.py                  # crea OPEN-TICKETS-YYYYMMDD
  python wonderdesk_open_tickets_to_sheet.py --tab-name OPEN  # crea/reescriu OPEN

Dependències:
  pip install -U python-dotenv playwright python-dateutil gspread google-auth
  python -m playwright install chromium
"""
from __future__ import annotations

import os, re, sys, asyncio, argparse
from datetime import datetime
from typing import List, Dict
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from dateutil import parser as dtparse
from playwright.async_api import async_playwright, Page

# =============== Helpers ENV ===============

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

# =============== Navegació i parsing WonderDesk ===============

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
    // heurístics per WonderDesk
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
      out.push({id:idTxt, date:dateTxt, subject:subjectTxt});
    }
  }
  return out;
}
"""

async def extract_open_rows_full(page: Page) -> List[Dict[str,str]]:
    """Recorre la paginació a Home i retorna totes les files d'oberts."""
    try:
        await click_menu(page, ["Home","Inicio"]) 
    except Exception:
        pass

    all_rows: List[Dict[str,str]] = []
    for _ in range(500):  # límit de seguretat
        try:
            rows = await page.evaluate(JS_FIND_ROWS)
            if rows: all_rows.extend(rows)
        except Exception:
            pass
        # intenta passar a la següent pàgina
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
    return all_rows

# =============== Utilitats ===============

def to_ddmmyyyy(date_text: str) -> str:
    """Converteix "Fri, Jul 5 2024" / variants → "05/07/2024". Si falla, retorna l'original."""
    s = (date_text or '').strip()
    if not s:
        return s
    try:
        dt = dtparse.parse(s, dayfirst=False, fuzzy=True)
        return dt.strftime('%d/%m/%Y')
    except Exception:
        return s

# =============== Google Sheets ===============

def write_to_sheet(rows: List[Dict[str,str]], tab_name: str):
    try:
        import gspread
    except ImportError:
        raise SystemExit("[!] Falta gspread/google-auth. pip install gspread google-auth")
    sheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not sheet_id or not creds:
        raise SystemExit("[!] Falta GOOGLE_SHEETS_SPREADSHEET_ID o GOOGLE_APPLICATION_CREDENTIALS al .env")
    creds = os.path.expanduser(creds)

    gc = gspread.service_account(filename=creds)
    sh = gc.open_by_key(sheet_id)

    # si existeix, la reescrivim (borrem i creem per simplicitat)
    try:
        try:
            ws = sh.worksheet(tab_name)
            sh.del_worksheet(ws)
        except Exception:
            pass
        ws = sh.add_worksheet(title=tab_name, rows="200", cols="10")
    except Exception as e:
        raise SystemExit(f"[!] No puc crear la pestanya '{tab_name}': {e}")

    # capçaleres
    headers = ["AGENCIA","ID","Fecha","Mes","Año","Subject"]
    ws.update('A1', [headers])
    try: ws.freeze(rows=1)
    except Exception: pass

    # dades
    batch = []
    valid_rows = 0
    for r in rows:
        agency = (r.get('agency','') or '').strip()
        _id = (r.get('id','') or '').strip()
        raw_date = (r.get('date','') or '').strip()
        subj = (r.get('subject','') or '').strip()
        # Filtre: cal ID + data + subject
        if not (_id and raw_date and subj):
            continue
        fecha_fmt = to_ddmmyyyy(raw_date)
        mes = anio = ''
        try:
            dt = dtparse.parse(raw_date, dayfirst=False, fuzzy=True)
            mes = dt.strftime('%Y-%m')
            anio = dt.strftime('%Y')
        except Exception:
            pass
        batch.append([agency, _id, fecha_fmt, mes, anio, subj])
        valid_rows += 1

    if batch:
        ws.update(f'A2:F{len(batch)+1}', batch)
    print(f"[+] Escrites {valid_rows} files vàlides a '{tab_name}' (de {len(rows)} candidates)")

# =============== MAIN ===============

async def amain():
    ap = argparse.ArgumentParser(description='WonderDesk → Google Sheets (open tickets per agència)')
    ap.add_argument('--tab-name', help='Nom de la pestanya de sortida; per defecte OPEN-TICKETS-YYYYMMDD', default=None)
    args = ap.parse_args()

    base_url, headful, agencies = get_agencies_from_env()

    tz = ZoneInfo('Europe/Madrid')
    today = datetime.now(tz)
    tab_name = args.tab_name or f"OPEN-TICKETS-{today.strftime('%Y%m%d')}"

    out_rows: List[Dict[str,str]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not headful)
        for ag in agencies:
            nombre = ag.get('nombre') or ag.get('code')
            usuario= ag.get('usuario')
            password=ag.get('password')
            if not usuario or not password:
                print(f"[!] {nombre}: falten credencials — salto")
                continue
            ctx = await browser.new_context()
            page = await ctx.new_page()
            try:
                print(f"[*] {nombre}: iniciant…")
                await login(page, base_url, usuario, password)
                rows = await extract_open_rows_full(page)
                # Filtra aquí també per evitar acumular línies buides
                valid = 0
                for r in rows:
                    _id = (r.get('id') or '').strip()
                    _date = (r.get('date') or '').strip()
                    _subj = (r.get('subject') or '').strip()
                    if not (_id and _date and _subj):
                        continue
                    out_rows.append({'agency': nombre, 'id': _id, 'date': _date, 'subject': _subj})
                    valid += 1
                print(f"    [+] {nombre}: {valid} tickets oberts vàlids (de {len(rows)})")
            except Exception as e:
                print(f"    [!] {nombre}: error — {e}")
            finally:
                await ctx.close()
        await browser.close()

    # Escriu a Google Sheets
    write_to_sheet(out_rows, tab_name)

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

