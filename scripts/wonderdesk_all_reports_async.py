#!/usr/bin/env python3
"""
WonderDesk — Multi-agencias (ASYNC): Resumen + CSVs + Google Sheets (append)

Cambios clave:
  - La métrica "DS" ahora suma DS+IS (con número).
  - Parser de tabla soporta "Categoría" y "Título" separados (UI en ES).
  - ds_cross_agencies incluye columna Subject (el primero que aparece).
  - Pestañas de Google Sheets con fecha YYYYMMDD y modo APPEND.

Salidas locales:
  1) agencias_wonderdesk_stats.csv — resumen por agencia (+ fila TOTAL)
  2) tickets_wonderdesk.csv       — tickets (Open + Closed últimos 7 días)
  3) ds_cross_agencies.csv        — agregación DS/IS (Issue, Subject, Agencias, Num Agencias)

CONFIG (.env):
  HELPDESK_BASE_URL=https://helpdesk.grupoiris.net
  HEADFUL=false
  DEBUG=false

  AGENCIES=AG1,AG2,AG3
  AG1_NOMBRE=Agencia Uno
  AG1_USUARIO=usuario1
  AG1_PASSWORD=clave1
  ...

  (Alternativa 1 agencia)
  COMPANY=TAG
  TAG_USUARIO=...
  TAG_PASSWORD=...

  Google Sheets (opcional)
  GOOGLE_SHEETS_PUSH=true
  GOOGLE_SHEETS_SPREADSHEET_ID=...
  GOOGLE_APPLICATION_CREDENTIALS=/Users/.../.login-gsheets.json

Dependencias:
  pip install -U python-dotenv playwright python-dateutil gspread google-auth nest_asyncio
  python -m playwright install chromium
"""
from __future__ import annotations

import os
import re
import csv
import sys
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from zoneinfo import ZoneInfo
from collections import defaultdict

from dotenv import load_dotenv
from dateutil import parser as dtparse
from playwright.async_api import async_playwright, Page

# -------------------- Regex DS/IS --------------------
RX_ISSUE = re.compile(r"(?i)\b(?:DS|IS)\s*-?\s*\d+")

def extract_issue_codes(text: str) -> List[str]:
    """Devuelve códigos normalizados DS12345 / IS12345 encontrados en el texto."""
    s = (text or "").strip()
    out: List[str] = []
    for m in re.finditer(r"(?i)\b(DS|IS)\s*-?\s*(\d+)", s):
        out.append(f"{m.group(1).upper()}{m.group(2)}")
    return out


# -------------------- ENV --------------------
def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def get_agencies_from_env() -> Tuple[str, bool, bool, List[Dict[str, str]]]:
    load_dotenv()
    base_url = os.getenv("HELPDESK_BASE_URL", "https://helpdesk.grupoiris.net").rstrip("/")
    headful = env_bool("HEADFUL", False)
    debug = env_bool("DEBUG", False)

    ag_list: List[Dict[str, str]] = []
    raw = os.getenv("AGENCIES", "").strip()
    if raw:
        codes = [c.strip().upper() for c in raw.split(",") if c.strip()]
        for code in codes:
            nombre = os.getenv(f"{code}_NOMBRE") or os.getenv(f"{code}_NOM") or code
            usuario = os.getenv(f"{code}_USUARIO") or os.getenv(f"{code}_USERNAME")
            password = os.getenv(f"{code}_PASSWORD")
            if not usuario or not password:
                ag_list.append(
                    {
                        "code": code,
                        "nombre": nombre,
                        "usuario": usuario or "",
                        "password": password or "",
                        "error": f"Faltan credenciales para {code} (_USUARIO/_USERNAME y/o _PASSWORD)",
                    }
                )
            else:
                ag_list.append({"code": code, "nombre": nombre, "usuario": usuario, "password": password})
    else:
        company = os.getenv("COMPANY", "").strip().upper()
        if not company:
            raise SystemExit("Falta AGENCIES=... o COMPANY=... en el .env")
        usuario = os.getenv(f"{company}_USUARIO") or os.getenv(f"{company}_USERNAME")
        password = os.getenv(f"{company}_PASSWORD")
        if not usuario or not password:
            raise SystemExit(f"Faltan credenciales {company}_USUARIO/_USERNAME y/o {company}_PASSWORD en el .env")
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
                    "input[name*='user' i]",
                    "input[name*='login' i]",
                    "input[type='email']",
                    "input[type='text']",
                    "input:not([type])",
                ]:
                    loc = ctx.locator(sel)
                    if await loc.count() > 0:
                        u = loc.first
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
                            bloc = ctx.locator(b)
                            if await bloc.count() > 0:
                                await bloc.first.click()
                                clicked = True
                                break
                        if not clicked:
                            await pw.press("Enter")
                        await ctx.wait_for_load_state("networkidle", timeout=10000)
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
                pass

    if await page.locator("input[type='password']").count() > 0:
        raise RuntimeError("No se pudo iniciar sesión (revisa credenciales o cambios en el formulario)")


# -------------------- UI helpers (async) --------------------
async def click_menu(page: Page, names: List[str]) -> None:
    for n in names:
        try:
            await page.get_by_role("link", name=re.compile(rf"^{re.escape(n)}$", re.I)).click(timeout=2000)
            await page.wait_for_load_state("networkidle", timeout=10000)
            return
        except Exception:
            try:
                await page.get_by_text(re.compile(n, re.I), exact=False).click(timeout=2000)
                await page.wait_for_load_state("networkidle", timeout=10000)
                return
            except Exception:
                continue
    raise RuntimeError(f"No se encontró ningún enlace de menú: {names}")


async def read_calls_banner(ctx) -> int:
    txt = await ctx.evaluate("() => document.body.innerText")
    m = re.search(r"(\d+)\s+Calls", txt, flags=re.I)
    return int(m.group(1)) if m else 0


# -------------------- PARSING TABLA (async) --------------------
# Soporta: ID/Fecha y columnas separadas "Categoría" + "Título"
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
    for (let r=0; r<Math.min(4, rows.length); r++){
      const cells = Array.from(rows[r].querySelectorAll('th,td'));
      const h = cells.map(c => norm(c.innerText));
      const hasID   = h.some(x => /^id$/i.test(x));
      const hasDate = h.some(x => /(fecha|date)/i.test(x));
      const hasCat  = h.some(x => /(categor[ií]a|category)/i.test(x));
      const hasTit  = h.some(x => /(t[ií]tulo|title|asunto|subject)/i.test(x));

      if (cells.length >= 6 && hasID && hasDate && (hasCat || hasTit)) {
        headerIdx = r; heads = h; break;
      }
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

      data.push({
        id: idTxt,
        date: dateTxt,
        category: categoryTxt,
        subject: subjectTxt,
        rowText: norm(rows[r].innerText||'')
      });
    }

    const ok = data.filter(r => (r.subject || r.category) && r.date).length >= 3;
    if (ok) goodTables.push({ data, heads, headerIdx });
  }

  if (goodTables.length){
    goodTables.sort((a,b)=> b.data.length - a.data.length);
    return { ok:true, rows:goodTables[0].data, meta:{ heads:goodTables[0].heads, headerIdx:goodTables[0].headerIdx } };
  }
  return result;
}
"""

async def extract_table_any(ctx, debug: bool = False) -> List[Dict[str, str]]:
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


# -------------------- Navegación paginación (async) --------------------
async def click_last_page(page: Page) -> bool:
    for sel in ["a:has-text('>>')", "a:has-text('[>>]')", "a:has-text('»»')"]:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.first.click(timeout=2000)
                await page.wait_for_load_state("networkidle", timeout=10000)
                return True
        except Exception:
            pass
    try:
        await page.get_by_role("link", name=re.compile(r"»»|>>", re.I)).first.click(timeout=2000)
        await page.wait_for_load_state("networkidle", timeout=10000)
        return True
    except Exception:
        return False


async def click_prev_page(page: Page) -> bool:
    for sel in ["a:has-text('<<')", "a:has-text('[<]')", "a:has-text('<')", "a:has-text('««')"]:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.last.click(timeout=2000)
                await page.wait_for_load_state("networkidle", timeout=10000)
                return True
        except Exception:
            pass
    try:
        await page.get_by_role("link", name=re.compile(r"««|<<|\[<]|^\s*<\s*$", re.I)).first.click(timeout=2000)
        await page.wait_for_load_state("networkidle", timeout=10000)
        return True
    except Exception:
        return False


async def click_next_page(page: Page) -> bool:
    for rx in [r"Next", r"Siguiente", r"Avanzar", r"^\s*>\s*$", r"Siguiente >", r"»"]:
        try:
            await page.get_by_role("link", name=re.compile(rx, re.I)).click(timeout=2000)
            await page.wait_for_load_state("networkidle", timeout=10000)
            return True
        except Exception:
            continue
    return False


# -------------------- Date parse --------------------
def parse_date_any(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return dtparse.parse(s, dayfirst=False, fuzzy=True)
    except Exception:
        return None


# -------------------- Lectura Home / Closed --------------------
async def read_home(page: Page, debug: bool = False) -> Dict[str, object]:
    try:
        await click_menu(page, ["Home", "Inicio"])
    except Exception:
        await page.goto(page.url.split("?")[0], wait_until="load")

    open_calls = await read_calls_banner(page)
    rows = await extract_table_any(page, debug=debug)

    open_ticket_rows: List[Dict[str, object]] = []
    ds_count = 0
    p3_count = 0

    for r in rows:
        cat = (r.get("category", "") or "").strip()
        subj = (r.get("subject", "") or "").strip()
        target = subj or cat

        is_ds = bool(RX_ISSUE.search(target))  # DS+IS
        is_p3 = bool(re.search(r"(?i)\bP\s*3(\b|\.)", target))

        if is_ds:
            ds_count += 1
        if is_p3:
            p3_count += 1

        open_ticket_rows.append(
            {
                "id": r.get("id", ""),
                "date": r.get("date", ""),
                "subject": subj or cat,
                "category": cat,
                "is_ds": is_ds,
                "is_p3": is_p3,
            }
        )

    # abiertos últimos 7 días
    now = datetime.now()
    cutoff = now - timedelta(days=7)
    last7_open = 0
    for r in rows:
        d = parse_date_any(r.get("date", ""))
        if d and cutoff <= d <= now:
            last7_open += 1

    return {
        "open_calls": open_calls,
        "bugs": ds_count,          # mantenemos nombre interno
        "p3": p3_count,
        "last7_open": last7_open,
        "open_rows": open_ticket_rows,
    }


async def read_closed_last7(page: Page) -> Dict[str, object]:
    try:
        await click_menu(page, ["List Closed", "List Cerrado", "List Cerrados"])
    except Exception:
        base = page.url.split("/wonderdesk.cgi")[0]
        await page.goto(f"{base}/wonderdesk.cgi?do=hd_list&help_status=Closed", wait_until="load")

    closed_calls = await read_calls_banner(page)

    now = datetime.now()
    cutoff = now - timedelta(days=7)
    collected: List[Dict[str, object]] = []

    # Ruta rápida: ir a última página y retroceder
    if await click_last_page(page):
        for _ in range(250):
            rows = await extract_table_any(page, debug=False)
            if not rows:
                break

            # Si la fecha máxima de la página ya es < cutoff, podemos cortar
            dmax = None
            for r in rows:
                d = parse_date_any(r.get("date", ""))
                if d and (dmax is None or d > dmax):
                    dmax = d
            if dmax is not None and dmax < cutoff:
                break

            for r in rows:
                d = parse_date_any(r.get("date", ""))
                if d and cutoff <= d <= now:
                    cat = (r.get("category", "") or "").strip()
                    subj = (r.get("subject", "") or "").strip()
                    target = (subj or cat).strip()

                    rr = dict(r)
                    rr["subject"] = subj or cat
                    rr["category"] = cat
                    rr["is_ds"] = bool(RX_ISSUE.search(target))
                    rr["is_p3"] = bool(re.search(r"(?i)\bP\s*3(\b|\.)", target))
                    collected.append(rr)

            if not await click_prev_page(page):
                break

    # Fallback: recorrer hacia delante
    if not collected:
        for _ in range(250):
            rows = await extract_table_any(page, debug=False)
            if not rows:
                break

            any_in = False
            oldest = None
            for r in rows:
                d = parse_date_any(r.get("date", ""))
                if not d:
                    continue
                if oldest is None or d < oldest:
                    oldest = d
                if cutoff <= d <= now:
                    cat = (r.get("category", "") or "").strip()
                    subj = (r.get("subject", "") or "").strip()
                    target = (subj or cat).strip()

                    rr = dict(r)
                    rr["subject"] = subj or cat
                    rr["category"] = cat
                    rr["is_ds"] = bool(RX_ISSUE.search(target))
                    rr["is_p3"] = bool(re.search(r"(?i)\bP\s*3(\b|\.)", target))
                    collected.append(rr)
                    any_in = True

            if oldest is not None and oldest < cutoff and not any_in:
                break
            if not await click_next_page(page):
                break

    return {"closed_calls": closed_calls, "last7_closed": len(collected), "closed_rows": collected}


# -------------------- CSV helpers --------------------
def _to_int(x) -> int:
    try:
        if x is None or x == "":
            return 0
        if isinstance(x, (int, float)):
            return int(x)
        s = str(x).strip()
        s = s.replace(".", "").replace(",", "")
        return int(s)
    except Exception:
        return 0


def compute_totals(rows: List[Dict[str, object]]):
    keys = ["Tickets Abiertos", "Tickets Cerrados", "Abiertos Última Semana", "Cerrados Última Semana", "DS", "P3"]
    tot = {k: 0 for k in keys}
    for r in rows:
        for k in keys:
            tot[k] += _to_int(r.get(k, 0))
    return tot


def print_summary_table(rows: List[Dict[str, object]]):
    headers = [
        "Nombre Agencia",
        "Tickets Abiertos",
        "Tickets Cerrados",
        "Abiertos Última Semana",
        "Cerrados Última Semana",
        "DS",
        "P3",
        "Error",
    ]
    widths = {h: max(len(h), *(len(str(r.get(h, ""))) for r in rows)) for h in headers}
    tot = compute_totals(rows)

    widths["Nombre Agencia"] = max(widths["Nombre Agencia"], len("TOTAL"))
    for k, v in tot.items():
        widths[k] = max(widths[k], len(str(v)))

    def line(ch="-"):
        print("+".join(ch * (widths[h] + 2) for h in headers))

    def row_print(d):
        print("|".join(" " + str(d.get(h, "")).ljust(widths[h]) + " " for h in headers))

    line("=")
    row_print({h: h for h in headers})
    line("=")
    for r in rows:
        row_print(r)
    line("-")
    total_row = {h: "" for h in headers}
    total_row["Nombre Agencia"] = "TOTAL"
    for k, v in tot.items():
        total_row[k] = v
    row_print(total_row)
    line("=")


def write_summary_csv(path: str, rows: List[Dict[str, object]]):
    headers = [
        "Nombre Agencia",
        "Tickets Abiertos",
        "Tickets Cerrados",
        "Abiertos Última Semana",
        "Cerrados Última Semana",
        "DS",
        "P3",
        "Error",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

        tot = compute_totals(rows)
        total_row = {h: "" for h in headers}
        total_row["Nombre Agencia"] = "TOTAL"
        for k, v in tot.items():
            total_row[k] = v
        w.writerow(total_row)


def write_tickets_csv(path: str, tickets: List[Dict[str, object]]):
    headers = ["Agency", "Status", "ID", "Date", "Subject", "Category", "DS", "P3"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        for t in tickets:
            w.writerow(
                {
                    "Agency": t.get("agency", ""),
                    "Status": t.get("status", ""),
                    "ID": t.get("id", ""),
                    "Date": t.get("date", ""),
                    "Subject": t.get("subject", ""),
                    "Category": t.get("category", ""),
                    "DS": "YES" if t.get("is_ds") else "",
                    "P3": "YES" if t.get("is_p3") else "",
                }
            )


def build_ds_cross_agencies_from_open(open_tickets: List[Dict[str, object]]) -> List[Dict[str, object]]:
    """
    Agrega DS/IS SOLO a partir de tickets OPEN (como pediste), guardando el primer subject que aparece.
    """
    issue_to_agencies: Dict[str, set] = defaultdict(set)
    issue_first_subject: Dict[str, str] = {}

    for r in open_tickets:
        ag = (r.get("agency") or "").strip()
        subj = (r.get("subject") or r.get("category") or "").strip()
        if not ag or not subj:
            continue

        codes = extract_issue_codes(subj)
        for code in codes:
            issue_to_agencies[code].add(ag)
            if code not in issue_first_subject:
                issue_first_subject[code] = subj

    out: List[Dict[str, object]] = []
    for code, ags in issue_to_agencies.items():
        out.append(
            {
                "Issue": code,
                "Subject": issue_first_subject.get(code, ""),
                "Agencias": ", ".join(sorted(ags)),
                "Num Agencias": len(ags),
            }
        )
    # Orden: Num Agencias desc, luego Issue asc
    out.sort(key=lambda x: (-int(x["Num Agencias"]), str(x["Issue"])))
    return out


def write_ds_csv(path: str, rows: List[Dict[str, object]]):
    headers = ["Issue", "Subject", "Agencias", "Num Agencias"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


# -------------------- Google Sheets (append, tabs YYYYMMDD) --------------------
def push_to_google_sheets_append(
    summary_rows: List[Dict[str, object]],
    open_tickets_rows: List[Dict[str, object]],
    ds_rows: List[Dict[str, object]],
) -> None:
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
        print("[!] Falta GOOGLE_SHEETS_SPREADSHEET_ID o GOOGLE_APPLICATION_CREDENTIALS en el .env")
        return

    creds_path = os.path.expanduser(creds_path)
    tz = ZoneInfo("Europe/Madrid")
    date_tag = datetime.now(tz).strftime("%Y%m%d")

    try:
        gc = gspread.service_account(filename=creds_path)
        sh = gc.open_by_key(sheet_id)
    except Exception as e:
        print(f"[!] No puedo abrir Google Sheets: {e}")
        return

    def get_or_create_ws(title: str, cols: int = 12):
        try:
            return sh.worksheet(title)
        except Exception:
            return sh.add_worksheet(title=title, rows="200", cols=str(cols))

    # 1) AGENCIAS-RESUMEN-YYYYMMDD
    ws_sum = get_or_create_ws(f"AGENCIAS-RESUMEN-{date_tag}", cols=12)
    sum_headers = [
        "Nombre Agencia",
        "Tickets Abiertos",
        "Tickets Cerrados",
        "Abiertos Última Semana",
        "Cerrados Última Semana",
        "DS",
        "P3",
        "Error",
    ]
    if not ws_sum.acell("A1").value:
        ws_sum.update("A1", [sum_headers])
        try:
            ws_sum.freeze(rows=1)
        except Exception:
            pass

    totals = compute_totals(summary_rows)
    block = []
    for r in summary_rows:
        block.append([r.get(h, "") for h in sum_headers])
    total_row = ["TOTAL", totals["Tickets Abiertos"], totals["Tickets Cerrados"], totals["Abiertos Última Semana"],
                 totals["Cerrados Última Semana"], totals["DS"], totals["P3"], ""]
    block.append(total_row)

    if block:
        ws_sum.append_rows(block, value_input_option="USER_ENTERED")
        print(f"[+] Sheets append: AGENCIAS-RESUMEN-{date_tag} (+{len(block)} filas)")

    # 2) OPEN-TICKETS-YYYYMMDD
    ws_open = get_or_create_ws(f"OPEN-TICKETS-{date_tag}", cols=10)
    open_headers = ["AGENCIA", "ID", "Fecha", "Subject"]
    if not ws_open.acell("A1").value:
        ws_open.update("A1", [open_headers])
        try:
            ws_open.freeze(rows=1)
        except Exception:
            pass

    open_block = []
    for t in open_tickets_rows:
        open_block.append([t.get("agency", ""), t.get("id", ""), t.get("date", ""), t.get("subject", "")])
    if open_block:
        ws_open.append_rows(open_block, value_input_option="USER_ENTERED")
        print(f"[+] Sheets append: OPEN-TICKETS-{date_tag} (+{len(open_block)} filas)")

    # 3) ds_cross_agencies-YYYYMMDD
    ws_ds = get_or_create_ws(f"ds_cross_agencies-{date_tag}", cols=12)
    ds_headers = ["Issue", "Subject", "Agencias", "Num Agencias"]
    if not ws_ds.acell("A1").value:
        ws_ds.update("A1", [ds_headers])
        try:
            ws_ds.freeze(rows=1)
        except Exception:
            pass

    ds_block = []
    for r in ds_rows:
        ds_block.append([r.get("Issue", ""), r.get("Subject", ""), r.get("Agencias", ""), r.get("Num Agencias", "")])
    if ds_block:
        ws_ds.append_rows(ds_block, value_input_option="USER_ENTERED")
        print(f"[+] Sheets append: ds_cross_agencies-{date_tag} (+{len(ds_block)} filas)")


# -------------------- PER-AGENCIA (async) --------------------
async def run_for_agency(page: Page, base_url: str, nombre: str, usuario: str, password: str):
    await login(page, base_url, usuario, password)
    home = await read_home(page, debug=False)
    closed = await read_closed_last7(page)

    tickets: List[Dict[str, object]] = []
    for r in home["open_rows"]:
        tickets.append({**r, "status": "Open", "agency": nombre})
    for r in closed["closed_rows"]:
        tickets.append({**r, "status": "Closed", "agency": nombre})

    summary = {
        "Nombre Agencia": nombre,
        "Tickets Abiertos": home["open_calls"],
        "Tickets Cerrados": closed["closed_calls"],
        "Abiertos Última Semana": home["last7_open"],
        "Cerrados Última Semana": closed["last7_closed"],
        "DS": home["bugs"],  # DS+IS
        "P3": home["p3"],
        "Error": "",
    }
    return summary, tickets


# -------------------- MAIN (async) --------------------
async def amain():
    base_url, headful, debug, ags = get_agencies_from_env()
    if not ags:
        print("No se han definido agencias en .env (AGENCIES=...) ni COMPANY=...")
        sys.exit(1)

    results_summary: List[Dict[str, object]] = []
    all_tickets: List[Dict[str, object]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not headful)
        for a in ags:
            nombre = a.get("nombre") or a.get("code")
            usuario = a.get("usuario")
            password = a.get("password")

            print(f"[*] {nombre}: iniciando…")
            if a.get("error"):
                print(f"    ! Config error: {a['error']}")
                results_summary.append(
                    {
                        "Nombre Agencia": nombre,
                        "Tickets Abiertos": "",
                        "Tickets Cerrados": "",
                        "Abiertos Última Semana": "",
                        "Cerrados Última Semana": "",
                        "DS": "",
                        "P3": "",
                        "Error": a["error"],
                    }
                )
                continue

            ctx = await browser.new_context()
            page = await ctx.new_page()
            try:
                summary, tickets = await run_for_agency(page, base_url, nombre, usuario, password)
                results_summary.append(summary)
                all_tickets.extend(tickets)

                print(
                    f"    ✓ Resumen: Open={summary['Tickets Abiertos']} | "
                    f"Closed_total={summary['Tickets Cerrados']} | "
                    f"Closed_7d={summary['Cerrados Última Semana']} | "
                    f"DS(DS+IS)={summary['DS']} | P3={summary['P3']}"
                )
            except Exception as e:
                print(f"    ! Error en {nombre}: {e}")
                results_summary.append(
                    {
                        "Nombre Agencia": nombre,
                        "Tickets Abiertos": "",
                        "Tickets Cerrados": "",
                        "Abiertos Última Semana": "",
                        "Cerrados Última Semana": "",
                        "DS": "",
                        "P3": "",
                        "Error": str(e),
                    }
                )
            finally:
                await ctx.close()

        await browser.close()

    # 1) Resumen consola
    print("\nRESUMEN POR AGENCIA:")
    print_summary_table(results_summary)

    # 2) CSV resumen por agencia
    out_summary = "agencias_wonderdesk_stats.csv"
    write_summary_csv(out_summary, results_summary)
    print(f"CSV guardado: {out_summary}")

    # 3) CSV tickets (Open + Closed últimos 7 días)
    tickets_csv = "tickets_wonderdesk.csv"
    write_tickets_csv(tickets_csv, all_tickets)
    print(f"CSV guardado: {tickets_csv}")

    # 4) CSV ds_cross_agencies (DS/IS) SOLO sobre OPEN (como pediste)
    open_only = [t for t in all_tickets if t.get("status") == "Open"]
    ds_rows = build_ds_cross_agencies_from_open(open_only)
    ds_csv = "ds_cross_agencies.csv"
    write_ds_csv(ds_csv, ds_rows)
    print(f"CSV guardado: {ds_csv}")

    # 5) Google Sheets (append, tabs YYYYMMDD) — opcional
    push_to_google_sheets_append(
        summary_rows=results_summary,
        open_tickets_rows=open_only,
        ds_rows=ds_rows,
    )


if __name__ == "__main__":
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

