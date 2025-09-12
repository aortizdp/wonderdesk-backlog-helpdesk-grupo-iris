# === FILE: wonderdesk_all_reports_async.py ===
#!/usr/bin/env python3
"""
WonderDesk — Multi‑agències (ASYNC): Resum + 3 CSVs + integració opcional amb Google Sheets

Millora aplicada (petició #2):
  • El recompte de "Cerrados Última Semana" ara salta a la darrera pàgina ([>>]) i
    retrocedeix pàgina a pàgina comptant **tots** els tancats dins dels últims 7 dies,
    amb condicions de tall segures per evitar bucles infinits.
  • El còmput de DS/P3 a Home és per **ocurrència** (cada fila que matxa suma 1).

Sortides locals:
  1) agencias_wonderdesk_stats.csv — resum per agència (+ fila TOTAL)
  2) tickets_wonderdesk.csv       — llistat de tickets (Open + Closed últims 7 dies)
  3) ds_cross_agencies.csv        — llistat de DS agregat (DS, Subject, Agencias, Num Agencias)

Opcional: pujar aquests 3 CSV a Google Sheets (una pestanya per cada fitxer, amb data).

CONFIG (.env):
  HELPDESK_BASE_URL=https://helpdesk.grupoiris.net
  HEADFUL=false
  DEBUG=false

  # Multi-agències
  AGENCIES=AG1,AG2,AG3
  AG1_NOMBRE=Agencia Uno
  AG1_USUARIO=usuario1
  AG1_PASSWORD=clave1
  ...

  # Alternativa 1 agència
  COMPANY=TAG
  TAG_USERNAME=usuario
  TAG_PASSWORD=contraseña
  TAG_NOMBRE=Nombre Visible (opcional)

  # Push a Google Sheets (opcional)
  GOOGLE_SHEETS_PUSH=true
  GOOGLE_SHEETS_SPREADSHEET_ID=1AbCDeFgHIJ...
  GOOGLE_APPLICATION_CREDENTIALS=/ruta/absoluta/al/service_account.json

Dependències:
  pip install -U python-dotenv playwright python-dateutil gspread google-auth
  python -m playwright install chromium
"""
from __future__ import annotations

import os, re, csv, sys, asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from dateutil import parser as dtparse
from playwright.async_api import async_playwright, Page

# -------------------- ENV --------------------

def env_bool(name: str, default: bool=False) -> bool:
    v = os.getenv(name)
    if v is None: return default
    return str(v).strip().lower() in {"1","true","yes","y","on"}


def get_agencies_from_env():
    load_dotenv()
    base_url = os.getenv("HELPDESK_BASE_URL", "https://helpdesk.grupoiris.net").rstrip("/")
    headful  = env_bool("HEADFUL", False)
    debug    = env_bool("DEBUG", False)

    ag_list = []
    raw = os.getenv("AGENCIES", "").strip()
    if raw:
        codes = [c.strip().upper() for c in raw.split(',') if c.strip()]
        for code in codes:
            nombre   = os.getenv(f"{code}_NOMBRE") or os.getenv(f"{code}_NOM") or code
            usuario  = os.getenv(f"{code}_USUARIO") or os.getenv(f"{code}_USERNAME")
            password = os.getenv(f"{code}_PASSWORD")
            if not usuario or not password:
                ag_list.append({
                    "code": code, "nombre": nombre, "usuario": usuario, "password": password,
                    "error": f"Falten credencials per {code} (_USUARIO/_USERNAME i/o _PASSWORD)"
                })
            else:
                ag_list.append({"code": code, "nombre": nombre, "usuario": usuario, "password": password})
    else:
        company = os.getenv("COMPANY", "").strip().upper()
        if not company:
            raise SystemExit("Falta AGENCIES=... o COMPANY=... al .env")
        usuario  = os.getenv(f"{company}_USUARIO") or os.getenv(f"{company}_USERNAME")
        password = os.getenv(f"{company}_PASSWORD")
        if not usuario or not password:
            raise SystemExit(f"Falten credencials {company}_USUARIO/_USERNAME i/o {company}_PASSWORD al .env")
        nombre = os.getenv(f"{company}_NOMBRE") or company
        ag_list = [{"code": company, "nombre": nombre, "usuario": usuario, "password": password}]

    return base_url, headful, debug, ag_list

# -------------------- LOGIN (async) --------------------

async def login(page: Page, base_url: str, usuario: str, password: str) -> None:
    await page.goto(f"{base_url}/wonderdesk.cgi", wait_until="load")

    async def attempt(ctx) -> bool:
        try:
            pw_loc = ctx.locator("input[type='password']")
            if await pw_loc.count() > 0:
                pw = pw_loc.first
                for sel in [
                    "input[name*='user' i]","input[name*='login' i]",
                    "input[type='email']","input[type='text']","input:not([type])",
                ]:
                    loc = ctx.locator(sel)
                    if await loc.count() > 0:
                        u = loc.first
                        await u.fill(usuario)
                        await pw.fill(password)
                        clicked = False
                        for b in [
                            "input[type='submit']","button[type='submit']","input[type='image']",
                            "button:has-text('Login')","button:has-text('Entrar')","button:has-text('Acceder')",
                        ]:
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
                if await attempt(fr):
                    break
            except Exception:
                pass

    if await page.locator("input[type='password']").count() > 0:
        raise RuntimeError("No s'ha pogut iniciar sessió (revisa credencials o canvis al formulari)")

# -------------------- UI helpers (async) --------------------

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
    raise RuntimeError(f"No s'ha trobat cap enllaç de menú: {names}")


async def read_calls_banner(ctx) -> int:
    txt = await ctx.evaluate("() => document.body.innerText")
    m = re.search(r"(\d+)\s+Calls", txt, flags=re.I)
    return int(m.group(1)) if m else 0

# -------------------- PARSING TAULA (async) --------------------

JS_FIND_ROWS = r"""
() => {
  const norm = (s) => (s||'').replace(/\s+/g,' ').trim();
  const result = { ok:false, rows:[], meta:{} };
  const tables = Array.from(document.querySelectorAll('table'));
  const goodTables = [];
  for (const t of tables){
    const rows = Array.from(t.querySelectorAll(':scope > tbody > tr, :scope > tr'));
    if (!rows.length) continue;
    let headerIdx = -1, heads = [];
    for (let r=0; r<Math.min(3, rows.length); r++){
      const cells = Array.from(rows[r].querySelectorAll('th,td'));
      const h = cells.map(c => norm(c.innerText));
      const hasID   = h.some(x => /^id$/i.test(x));
      const hasDate = h.some(x => /fecha|date/i.test(x));
      const hasCatS = h.some(x => /category\s*subject|categoria\s*subject|categoria\s*asunto|category|categoria/i.test(x));
      if (cells.length >= 6 && hasID && hasDate && hasCatS) { headerIdx = r; heads = h; break; }
    }
    if (headerIdx < 0) continue;
    const idx = {}; heads.forEach((h,i)=>{ idx[h.toLowerCase()] = i; });
    const findIdx = (rx) => { const k = Object.keys(idx).find(k => rx.test(k)); return (k!=null)? idx[k] : -1; };
    let idIx   = findIdx(/^\s*id\s*$/i);
    let dateIx = findIdx(/fecha|date/i);
    let catIx  = findIdx(/category\s*subject|categoria\s*subject|categoria\s*asunto|category|categoria/i);
    if (idIx < 0 && heads.length >= 2) idIx = 1;
    if (dateIx < 0 && heads.length >= 3) dateIx = 2;
    if (catIx < 0 && heads.length >= 6) catIx = 5;
    const data = [];
    for (let r = headerIdx + 1; r < rows.length; r++){
      const cells = Array.from(rows[r].querySelectorAll('td'));
      if (!cells.length) continue;
      const get = (i) => (i>=0 && i<cells.length) ? cells[i] : null;
      const idTxt   = get(idIx)   ? norm(get(idIx).innerText)   : '';
      const dateTxt = get(dateIx) ? norm(get(dateIx).innerText) : '';
      let categoryTxt = '', subjectTxt = '';
      const catCell = get(catIx);
      if (catCell){
        categoryTxt = norm(catCell.innerText);
        const a = catCell.querySelector('a');
        subjectTxt = a ? norm(a.textContent) : categoryTxt;
      }
      data.push({ id:idTxt, date:dateTxt, category:categoryTxt, subject:subjectTxt, rowText: norm(rows[r].innerText||'') });
    }
    const ok = data.filter(r => (r.subject||r.category) && r.date).length >= 3;
    if (ok) { goodTables.push({ data, heads, headerIdx }); }
  }
  if (goodTables.length){
    goodTables.sort((a,b)=> b.data.length - a.data.length);
    return { ok:true, rows:goodTables[0].data, meta:{ heads:goodTables[0].heads, headerIdx:goodTables[0].headerIdx } };
  }
  return result;
}
"""

async def extract_table_any(ctx, debug=False) -> List[Dict[str,str]]:
    try:
        res = await ctx.evaluate(JS_FIND_ROWS)
        if res and res.get("ok") and res.get("rows"):
            return res["rows"]
    except Exception:
        pass
    if isinstance(ctx, Page):
        for fr in ctx.frames:
            try:
                res = await fr.evaluate(JS_FIND_ROWS)
                if res and res.get("ok") and res.get("rows"):
                    return res["rows"]
            except Exception:
                continue
    return []

# -------------------- Navegació i recompte tancats (async) --------------------

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

async def read_home(page: Page, debug=False):
    try:
        await click_menu(page, ["Home","Inicio"])  
    except Exception:
        await page.goto(page.url.split('?')[0], wait_until="load")
    open_calls = await read_calls_banner(page)
    rows = await extract_table_any(page, debug=debug)

    open_ticket_rows = []
    bugs = p3 = 0
    for r in rows:
        cat  = (r.get('category','') or '').strip()
        subj = (r.get('subject','') or '').strip()
        target = subj or cat
        is_ds = bool(re.search(r'(?i)\bDS\s*-?\s*\d+', target))
        is_p3 = bool(re.search(r'(?i)\bP\s*3(\b|\.)', target))
        if is_ds: bugs += 1
        if is_p3: p3 += 1
        open_ticket_rows.append({
            'id': r.get('id',''),
            'date': r.get('date',''),
            'subject': subj or cat,
            'category': cat,
            'is_ds': is_ds,
            'is_p3': is_p3,
        })

    # darrers 7 dies (oberts)
    now = datetime.now(); cutoff = now - timedelta(days=7)
    last7_open = 0
    for r in rows:
        d = parse_date_any(r.get('date',''))
        if d and cutoff <= d <= now:
            last7_open += 1

    return {
        "open_calls": open_calls,
        "bugs": bugs,
        "p3": p3,
        "last7_open": last7_open,
        "open_rows": open_ticket_rows,
    }


def parse_date_any(s: str) -> Optional[datetime]:
    s = (s or '').strip()
    if not s: return None
    try:
        return dtparse.parse(s, dayfirst=False, fuzzy=True)
    except Exception:
        m = re.search(r"([A-Za-zÀ-ÿ]{3,}\s+\d{1,2}\s+\d{4})", s)
        if m:
            try:   return dtparse.parse(m.group(1), dayfirst=False, fuzzy=True)
            except Exception: return None
        return None

async def read_closed_last7(page: Page):
    # Navega a List Closed
    try:
        await click_menu(page, ["List Closed","List Cerrado","List Closed ","List Cerrados"]) 
    except Exception:
        base = page.url.split('/wonderdesk.cgi')[0]
        await page.goto(f"{base}/wonderdesk.cgi?do=hd_list&help_status=Closed", wait_until="load")
    closed_calls = await read_calls_banner(page)

    now = datetime.now(); cutoff = now - timedelta(days=7)
    collected: List[Dict[str,str]] = []

    # Ruta principal: [>>] i retrocedir [<]
    if await click_last_page(page):
        for _ in range(200):  # límit de seguretat
            rows = await extract_table_any(page, debug=False)
            if not rows:
                break
            dmax = None
            for r in rows:
                d = parse_date_any(r.get('date',''))
                if d and (dmax is None or d > dmax): dmax = d
            if dmax is not None and dmax < cutoff:
                # tota la pàgina és massa antiga → ja podem tallar
                break
            for r in rows:
                d = parse_date_any(r.get('date',''))
                if d and cutoff <= d <= now:
                    rr = {**r}
                    rr['subject']  = (rr.get('subject','') or rr.get('category','')).strip()
                    rr['category'] = (rr.get('category','') or '').strip()
                    rr['is_ds'] = bool(re.search(r'(?i)\bDS\s*-?\s*\d+', rr['subject'] or rr['category']))
                    rr['is_p3'] = bool(re.search(r'(?i)\bP\s*3(\b|\.)', rr['subject'] or rr['category']))
                    collected.append(rr)
            if not await click_prev_page(page):
                break

    # Fallback: si no s'ha pogut saltar a l'última, recorrem endavant i parem quan ja no calgui
    if not collected:
        for _ in range(200):
            rows = await extract_table_any(page, debug=False)
            if not rows:
                break
            any_in = False; oldest = None
            for r in rows:
                d = parse_date_any(r.get('date',''))
                if not d: continue
                if oldest is None or d < oldest: oldest = d
                if cutoff <= d <= now:
                    rr = {**r}
                    rr['subject']  = (rr.get('subject','') or rr.get('category','')).strip()
                    rr['category'] = (rr.get('category','') or '').strip()
                    rr['is_ds'] = bool(re.search(r'(?i)\bDS\s*-?\s*\d+', rr['subject'] or rr['category']))
                    rr['is_p3'] = bool(re.search(r'(?i)\bP\s*3(\b|\.)', rr['subject'] or rr['category']))
                    collected.append(rr); any_in = True
            if oldest is not None and oldest < cutoff and not any_in:
                break
            if not await click_next_page(page):
                break

    last7 = len(collected)
    return {"closed_calls": closed_calls, "last7_closed": last7, "closed_rows": collected}

# -------------------- CSV helpers --------------------

def _to_int(x) -> int:
    try: return int(x)
    except Exception: return 0


def compute_totals(rows: List[Dict[str,object]]):
    keys = ["Tickets Abiertos","Tickets Cerrados","Abiertos Última Semana","Cerrados Última Semana","DS","P3"]
    tot = {k:0 for k in keys}
    for r in rows:
        for k in keys:
            tot[k] += _to_int(r.get(k, 0))
    return tot


def print_summary_table(rows: List[Dict[str,object]]):
    headers = ["Nombre Agencia","Tickets Abiertos","Tickets Cerrados","Abiertos Última Semana","Cerrados Última Semana","DS","P3","Error"]
    widths = {h: max(len(h), *(len(str(r.get(h, ''))) for r in rows)) for h in headers}
    tot = compute_totals(rows)
    widths["Nombre Agencia"] = max(widths["Nombre Agencia"], len("TOTAL"))
    for k,v in tot.items(): widths[k] = max(widths[k], len(str(v)))
    def line(ch='-'):
        print('+'.join(ch*(widths[h]+2) for h in headers))
    def row_print(d):
        print('|'.join(' '+str(d.get(h, '')).ljust(widths[h])+' ' for h in headers))
    line('='); row_print({h:h for h in headers}); line('=')
    for r in rows: row_print(r)
    line('-')
    total_row = {h: '' for h in headers}
    total_row["Nombre Agencia"] = "TOTAL"
    for k,v in tot.items(): total_row[k] = v
    row_print(total_row)
    line('=')


def write_summary_csv(path: str, rows: List[Dict[str,object]]):
    headers = ["Nombre Agencia","Tickets Abiertos","Tickets Cerrados","Abiertos Última Semana","Cerrados Última Semana","DS","P3","Error"]
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
        w.writeheader(); w.writerows(rows)
        tot = compute_totals(rows)
        total_row = {h: '' for h in headers}
        total_row["Nombre Agencia"] = "TOTAL"
        for k,v in tot.items(): total_row[k] = v
        w.writerow(total_row)


def write_tickets_csv(path: str, tickets: List[Dict[str,object]]):
    headers = ["Agency","Status","ID","Date","Subject","Category","DS","P3"]
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
        w.writeheader()
        for t in tickets:
            w.writerow({
                "Agency": t.get('agency',''),
                "Status": t.get('status',''),
                "ID": t.get('id',''),
                "Date": t.get('date',''),
                "Subject": t.get('subject',''),
                "Category": t.get('category',''),
                "DS": 'YES' if t.get('is_ds') else '',
                "P3": 'YES' if t.get('is_p3') else '',
            })


def build_ds_cross_agencies(tickets: List[Dict[str,object]]):
    index: Dict[str, Dict[str, object]] = {}
    for r in tickets:
        ag = r.get('agency')
        subject = r.get('subject') or r.get('category')
        target = subject or ''
        m = re.search(r'(?i)\bDS\s*-?\s*(\d{3,})', target)
        if not m: continue
        dsnum = f"DS{m.group(1)}"
        if dsnum not in index:
            index[dsnum] = {"subject": subject, "agencies": set([ag])}
        else:
            index[dsnum]["agencies"].add(ag)
            if subject and len(subject) > len(index[dsnum]["subject"] or ""):
                index[dsnum]["subject"] = subject
    def ds_key(k: str):
        m = re.search(r"(\d+)", k); return int(m.group(1)) if m else 0
    out = []
    for k, v in sorted(index.items(), key=lambda kv: ds_key(kv[0])):
        ags = sorted(v["agencies"])  # alfabètic
        out.append({
            "DS": k,
            "Subject": v["subject"],
            "Agencias": ", ".join(ags),
            "Num Agencias": len(ags),
        })
    return out


def write_ds_csv(path: str, ds_rows: List[Dict[str,object]]):
    headers = ["DS","Subject","Agencias","Num Agencias"]
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
        w.writeheader(); w.writerows(ds_rows)

# -------------------- PUSH a Google Sheets (opc.) --------------------

def push_csvs_to_google_sheets(csv_paths: List[str]):
    if not env_bool("GOOGLE_SHEETS_PUSH", False):
        return
    try:
        import gspread
    except ImportError:
        print("[!] Falta gspread/google-auth. pip install gspread google-auth")
        return

    sheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not sheet_id or not creds_path:
        print("[!] Falta GOOGLE_SHEETS_SPREADSHEET_ID o GOOGLE_APPLICATION_CREDENTIALS al .env")
        return
    creds_path = os.path.expanduser(creds_path)
    today_str = datetime.now(ZoneInfo("Europe/Madrid")).strftime("%Y-%m-%d")

    try:
        import csv as _csv
        import re as _re
        gc = gspread.service_account(filename=creds_path)
        sh = gc.open_by_key(sheet_id)
    except Exception as e:
        print(f"[!] No puc obrir Google Sheets: {e}")
        return

    def _slug(title: str) -> str:
        s = _re.sub(r"[\[\]\:\*\?\/\\]", " ", title)
        s = _re.sub(r"\s+", " ", s).strip()
        return s[:100]

    for path in csv_paths:
        if not os.path.exists(path):
            print(f"[!] CSV no trobat, s'ignora: {path}")
            continue
        with open(path, newline='', encoding='utf-8') as f:
            data = list(_csv.reader(f))
        base = os.path.splitext(os.path.basename(path))[0]
        title = _slug(f"{base}_{today_str}")
        # esborra si ja existeix
        try:
            ws = sh.worksheet(title); sh.del_worksheet(ws)
        except Exception:
            pass
        rows = len(data) if data else 1
        cols = max((len(r) for r in data), default=1)
        ws = sh.add_worksheet(title=title, rows=str(max(rows,1)), cols=str(max(cols,1)))
        if data:
            ws.update('A1', data)
            try: ws.freeze(rows=1)
            except Exception: pass
        print(f"[+] Sheets: creada pestanya '{title}' ({rows}x{cols})")

# -------------------- PER‑AGÈNCIA (async) --------------------

async def run_for_agency(page: Page, base_url: str, nombre: str, usuario: str, password: str):
    await login(page, base_url, usuario, password)
    home = await read_home(page, debug=False)
    closed = await read_closed_last7(page)

    # composa tickets
    tickets = []
    for r in home["open_rows"]:
        tickets.append({**r, 'status': 'Open', 'agency': nombre})
    for r in closed["closed_rows"]:
        tickets.append({**r, 'status': 'Closed', 'agency': nombre})

    summary = {
        "Nombre Agencia": nombre,
        "Tickets Abiertos": home["open_calls"],
        "Tickets Cerrados": closed["closed_calls"],
        "Abiertos Última Semana": home["last7_open"],
        "Cerrados Última Semana": closed["last7_closed"],
        "DS": home["bugs"],
        "P3": home["p3"],
        "Error": "",
    }
    return summary, tickets

# -------------------- MAIN (async) --------------------

async def amain():
    base_url, headful, debug, ags = get_agencies_from_env()
    if not ags:
        print("No s'han definit agències a .env (AGENCIES=...) ni COMPANY=...")
        sys.exit(1)

    results_summary: List[Dict[str,object]] = []
    all_tickets: List[Dict[str,object]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not headful)
        for a in ags:
            nombre = a.get('nombre') or a.get('code')
            usuario = a.get('usuario')
            password= a.get('password')
            print(f"[*] {nombre}: iniciant…")
            if a.get('error'):
                print(f"    ! Config error: {a['error']}")
                results_summary.append({
                    "Nombre Agencia": nombre,
                    "Tickets Abiertos": "",
                    "Tickets Cerrados": "",
                    "Abiertos Última Semana": "",
                    "Cerrados Última Semana": "",
                    "DS": "",
                    "P3": "",
                    "Error": a['error'],
                })
                continue
            ctx = await browser.new_context()
            page = await ctx.new_page()
            try:
                summary, tickets = await run_for_agency(page, base_url, nombre, usuario, password)
                results_summary.append(summary)
                all_tickets.extend(tickets)
                if summary.get('Error'):
                    print(f"    ! Error: {summary['Error']}")
                else:
                    print(f"    ✓ Resum: Open={summary['Tickets Abiertos']} | Closed_total={summary['Tickets Cerrados']} | Last7={summary['Cerrados Última Semana']} | DS={summary['DS']} | P3={summary['P3']}")
            except Exception as e:
                print(f"    ! Error a {nombre}: {e}")
            finally:
                await ctx.close()
        await browser.close()

    # 1) Resum a consola
    print("\nRESUM PER AGÈNCIA:")
    print_summary_table(results_summary)

    # 2) CSV: resum per agència (amb TOTAL)
    out_summary = "agencias_wonderdesk_stats.csv"
    write_summary_csv(out_summary, results_summary)
    print(f"CSV guardat: {out_summary}")

    # 3) CSV: tickets (oberts + tancats_7d)
    tickets_csv = 'tickets_wonderdesk.csv'
    write_tickets_csv(tickets_csv, all_tickets)
    print(f"CSV guardat: {tickets_csv}")

    # 4) CSV: DS agregat transversal
    ds_rows = build_ds_cross_agencies(all_tickets)
    ds_csv = 'ds_cross_agencies.csv'
    write_ds_csv(ds_csv, ds_rows)
    print(f"CSV guardat: {ds_csv}")

    # 5) Push a Google Sheets (opcional)
    push_csvs_to_google_sheets([out_summary, tickets_csv, ds_csv])

if __name__ == '__main__':
    try:
        asyncio.run(amain())
    except RuntimeError as e:
        if "asyncio.run() cannot be called from a running event loop" in str(e):
            import nest_asyncio
            nest_asyncio.apply()
            loop = asyncio.get_event_loop()
            loop.run_until_complete(amain())
        else:
            raise


