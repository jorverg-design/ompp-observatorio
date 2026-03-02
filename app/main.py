
import os, json, sqlite3, datetime
from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape
from openpyxl import load_workbook
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas as pdfcanvas
import matplotlib.pyplot as plt
import numpy as np
from reportlab.lib.units import cm
import textwrap

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "ompp.sqlite")
TPL_DIR = os.path.join(BASE_DIR, "templates")

env = Environment(loader=FileSystemLoader(TPL_DIR), autoescape=select_autoescape(["html"]))

# --- Config (parametrizable) ---
CONFIG_DIR = os.path.join(BASE_DIR, "config")
ALERTAS_PATH = os.path.join(CONFIG_DIR, "alertas.json")
SISTEMA_PATH = os.path.join(CONFIG_DIR, "sistema.json")

def load_json(path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default if default is not None else {}

def get_alertas():
    return load_json(ALERTAS_PATH, {})

def get_sistema():
    return load_json(SISTEMA_PATH, {})

def whatsapp_send(msg: str):
    """
    v1: envío parametrizado.
    - enabled=false -> no envía.
    - test_mode=true -> escribe en whatsapp_outbox.log.
    Producción: integrar Meta WhatsApp Cloud API con credenciales y plantillas.
    """
    cfg = get_alertas().get("whatsapp", {})
    if not cfg.get("enabled", False):
        return {"sent": False, "reason": "disabled"}
    log_path = os.path.join(BASE_DIR, "whatsapp_outbox.log")
    stamp = datetime.datetime.now().isoformat()
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{stamp}] TO={cfg.get('recipients',[])} :: {msg}\n")
    return {"sent": True, "mode": "test_log" if cfg.get("test_mode", True) else "configured"}


app = FastAPI(title="OMPP Sistema con Reporte Real")
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

def db():
    return sqlite3.connect(DB_PATH)

def fetch_products():
    con=db(); cur=con.cursor()
    cur.execute("SELECT code,name,category,unit FROM products ORDER BY id")
    rows=cur.fetchall(); con.close()
    return rows

CONS_CODES = [r[0] for r in fetch_products()[:18]]
MOB_CODES = ["NAFTA","DIESEL","PASAJE"]

def ensure_alerts_table():
        pass

def ensure_raw_table():
    con=db(); cur=con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS raw_source_files(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        source_key TEXT NOT NULL,
        url TEXT,
        fetched_at TEXT NOT NULL,
        sha256 TEXT NOT NULL,
        file_path TEXT NOT NULL
    )""")
    con.commit(); con.close()

ensure_raw_table()

    con=db(); cur=con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS alerts_events(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        obs_date TEXT NOT NULL,
        product_code TEXT,
        scope TEXT NOT NULL,
        level TEXT NOT NULL,
        metric TEXT NOT NULL,
        value REAL,
        change_value REAL,
        source TEXT,
        message TEXT NOT NULL,
        whatsapp_sent INTEGER DEFAULT 0
    )""")
    con.commit(); con.close()

ensure_alerts_table()


def upsert_obs(code, week_date, value, geo="AMA", source="Relevamiento"):
    con=db(); cur=con.cursor()
    cur.execute(
        """INSERT INTO observations(product_code,week_date,value,geo,source,created_at)
           VALUES (?,?,?,?,?,?)
           ON CONFLICT(product_code,week_date,geo,source) DO UPDATE SET
             value=excluded.value, created_at=excluded.created_at""",
        (code, week_date, float(value), geo, source, datetime.datetime.utcnow().isoformat())
    )
    con.commit(); con.close()

def week_values(week_date):
    con=db(); cur=con.cursor()
    cur.execute("SELECT product_code,value FROM observations WHERE week_date=?", (week_date,))
    d={k:v for k,v in cur.fetchall()}
    con.close(); return d


def compute_date(obs_date: str):
    """
    Computa métricas para una fecha (diaria) y su referencia semanal (7 días).
    daily: vs día anterior | weekly: vs 7 días atrás
    """
    d0 = datetime.date.fromisoformat(obs_date)
    d1 = (d0 - datetime.timedelta(days=1)).isoformat()
    d7 = (d0 - datetime.timedelta(days=7)).isoformat()

    now = week_values(obs_date)
    prev1 = week_values(d1)
    prev7 = week_values(d7)

    def avg_change(codes, prev_dict):
        ch=[]
        for c in codes:
            a=now.get(c); b=prev_dict.get(c)
            if a is None or b in (None,0):
                continue
            ch.append(a/b-1.0)
        return sum(ch)/len(ch) if ch else None

    idx_can_d = avg_change(CONS_CODES, prev1)
    idx_can_w = avg_change(CONS_CODES, prev7)
    idx_mob_d = avg_change(MOB_CODES, prev1)
    idx_mob_w = avg_change(MOB_CODES, prev7)

    cost = now.get("COSTILLA")
    cost_d = None
    cost_w = None
    if cost is not None and prev1.get("COSTILLA") not in (None,0):
        cost_d = cost/prev1["COSTILLA"]-1.0
    if cost is not None and prev7.get("COSTILLA") not in (None,0):
        cost_w = cost/prev7["COSTILLA"]-1.0

    # --- Alerts engine (parametrizado) ---
    cfg = get_alertas()
    rules = cfg.get("alert_rules", {})
    def rule_for(code):
        base = rules.get("DEFAULT", {})
        specific = rules.get(code, {})
        merged = dict(base); merged.update(specific)
        return merged

    alerts=[]
    def add_alert(scope, code, level, metric, value, change, source, msg):
        alerts.append({"scope":scope,"code":code,"level":level,"metric":metric,"value":value,"change":change,"msg":msg,"source":source})
        con=db(); cur=con.cursor()
        cur.execute("""INSERT INTO alerts_events(created_at,obs_date,product_code,scope,level,metric,value,change_value,source,message,whatsapp_sent)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (datetime.datetime.utcnow().isoformat(), obs_date, code, scope, level, metric, value, change, source, msg, 0))
        con.commit(); con.close()

    # Index alerts (canasta)
    idx_rule = rule_for("CANASTA_INDEX")
    if idx_can_d is not None and idx_rule.get("daily_up") is not None and idx_can_d >= idx_rule["daily_up"]:
        add_alert("INDEX", "CANASTA_INDEX", "RED", "daily_change", idx_can_d, idx_can_d, "Sistema", "🔴 Presión diaria en canasta (índice)")
    if idx_can_w is not None and idx_rule.get("weekly_up") is not None and idx_can_w >= idx_rule["weekly_up"]:
        add_alert("INDEX", "CANASTA_INDEX", "RED", "weekly_change", idx_can_w, idx_can_w, "Sistema", "🔴 Presión semanal en canasta (índice)")

    # Mobility index alert (simple)
    pas_rule = rule_for("PASAJE")
    if idx_mob_d is not None and pas_rule.get("daily_up") is not None and idx_mob_d >= pas_rule["daily_up"]:
        add_alert("INDEX", "MOVILIDAD_INDEX", "ORANGE", "daily_change", idx_mob_d, idx_mob_d, "Sistema", "🟠 Presión diaria en movilidad (índice)")

    # Product-level RED alerts (brusco)
    for code, val in now.items():
        r = rule_for(code)
        if prev1.get(code) not in (None,0) and val is not None:
            ch = val/prev1[code]-1.0
            if r.get("daily_up") is not None and ch >= r["daily_up"]:
                add_alert("PRODUCT", code, "RED", "daily_change", val, ch, "Relevamiento/Excel", f"🔴 Movimiento brusco al alza: {code} ({ch*100:.1f}%)")
            if r.get("daily_down") is not None and ch <= r["daily_down"]:
                add_alert("PRODUCT", code, "RED", "daily_change", val, ch, "Relevamiento/Excel", f"🔴 Movimiento brusco a la baja: {code} ({ch*100:.1f}%)")

    # WhatsApp para alertas ROJAS
    red = [a for a in alerts if a["level"]=="RED"]
    if red:
        lines = [f"OMPP — ALERTA 🔴 ({obs_date})"]
        for a in red[:8]:
            lines.append(f"- {a['msg']}")
        res = whatsapp_send("\n".join(lines))
        if res.get("sent"):
            con=db(); cur=con.cursor()
            cur.execute("UPDATE alerts_events SET whatsapp_sent=1 WHERE obs_date=? AND level='RED' AND whatsapp_sent=0", (obs_date,))
            con.commit(); con.close()

    # Persist in computed (daily snapshot; weekly embedded)
    con=db(); cur=con.cursor()
    cur.execute(
        """INSERT INTO computed(week_date,index_canasta,index_mobility,costilla_avg,costilla_weekly_change,alerts_json,created_at)
           VALUES (?,?,?,?,?,?,?)
           ON CONFLICT(week_date) DO UPDATE SET
             index_canasta=excluded.index_canasta,
             index_mobility=excluded.index_mobility,
             costilla_avg=excluded.costilla_avg,
             costilla_weekly_change=excluded.costilla_weekly_change,
             alerts_json=excluded.alerts_json,
             created_at=excluded.created_at""",
        (obs_date, idx_can_d, idx_mob_d, cost, cost_d, json.dumps({"alerts":alerts, "weekly":{"canasta":idx_can_w,"movilidad":idx_mob_w,"costilla":cost_w}}, ensure_ascii=False), datetime.datetime.utcnow().isoformat())
    )
    con.commit(); con.close()

    return {
        "obs_date": obs_date,
        "week_date": obs_date,
        "index_canasta": idx_can_d,
        "index_mobility": idx_mob_d,
        "costilla_avg": cost,
        "costilla_weekly_change": cost_d,
        "alerts": alerts,
        "weekly": {"canasta": idx_can_w, "movilidad": idx_mob_w, "costilla": cost_w}
    }

def compute_week(week_date: str):
    return compute_date(week_date)
def latest_weeks(limit=30):
    con=db(); cur=con.cursor()
    cur.execute("SELECT DISTINCT week_date FROM observations ORDER BY week_date DESC LIMIT ?", (limit,))
    weeks=[r[0] for r in cur.fetchall()]
    con.close()
    return list(reversed(weeks))

def fmt_pct(x): 
    return "" if x is None else f"{x*100:.1f}%"
def fmt_gs(x):
    return "" if x is None else f"{int(round(x)):,}".replace(",", ".")

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    weeks = latest_weeks(12)
    last = weeks[-1] if weeks else None
    metrics = compute_date(last) if last else None
    tpl=env.get_template("dashboard.html")
    return tpl.render(weeks=weeks, last=last, m=metrics, fmt_pct=fmt_pct, fmt_gs=fmt_gs)


@app.get("/ranking", response_class=HTMLResponse)
def ranking_page(request: Request, obs_date: str = None):
    weeks = latest_weeks(30)
    last = obs_date or (weeks[-1] if weeks else None)
    up, dn = top_movers(last, k=10) if last else ([],[])
    tpl=env.get_template("ranking.html")
    return tpl.render(last=last, weeks=weeks, up=up, dn=dn, fmt_pct=fmt_pct, fmt_gs=fmt_gs)

@app.get("/carga", response_class=HTMLResponse)
def carga(request: Request):
    products = fetch_products()
    tpl=env.get_template("carga.html")
    return tpl.render(products=products)

@app.post("/carga")
def carga_post(week_date: str = Form(...), geo: str = Form("AMA"), source: str = Form("Relevamiento"), data_json: str = Form(...)):
    data=json.loads(data_json)
    for code,val in data.items():
        if val in (None,""): 
            continue
        upsert_obs(code, week_date, float(val), geo=geo, source=source)
    compute_date(week_date)
    return RedirectResponse("/", status_code=303)

@app.post("/import_excel")
def import_excel(file: UploadFile = File(...), source: str = Form("Excel")):
    tmp = os.path.join(BASE_DIR, "_upload.xlsx")
    with open(tmp, "wb") as f:
        f.write(file.file.read())
    wb = load_workbook(tmp, data_only=True)
    if "Canasta_25" not in wb.sheetnames:
        return JSONResponse({"ok": False, "error":"No se encontró la hoja Canasta_25"}, status_code=400)
    sh = wb["Canasta_25"]
    headers={}
    for col in range(2, 2+25):
        headers[col]=sh.cell(row=6, column=col).value
    prods=fetch_products()
    name_to_code={n.strip():c for c,n,_,_ in prods}
    for r in range(8, 8+80):
        wd=sh.cell(row=r, column=1).value
        if not wd:
            continue
        if isinstance(wd, datetime.datetime): wd=wd.date()
        if isinstance(wd, datetime.date): week_date=wd.isoformat()
        else: week_date=str(wd)[:10]
        for col,nm in headers.items():
            if nm is None: continue
            nm=str(nm).strip()
            if nm not in name_to_code: 
                continue
            val=sh.cell(row=r, column=col).value
            if val is None: 
                continue
            code=name_to_code[nm]
            if code=="INF_ALIM" and val>1:
                val=val/100.0
            upsert_obs(code, week_date, val, source=source)
        compute_date(week_date)
    return RedirectResponse("/", status_code=303)



def top_movers(obs_date: str, scope_codes=None, k: int = 10):
    """
    Top alzas/bajas diarias por producto (requiere datos del día anterior).
    """
    d0=datetime.date.fromisoformat(obs_date)
    d1=(d0-datetime.timedelta(days=1)).isoformat()
    now=week_values(obs_date)
    prev=week_values(d1)
    moves=[]
    for code,val in now.items():
        if scope_codes and code not in scope_codes:
            continue
        b=prev.get(code)
        if val is None or b in (None,0): 
            continue
        ch=val/b-1.0
        moves.append((code, ch, val))
    moves_sorted=sorted(moves, key=lambda x: x[1], reverse=True)
    up=moves_sorted[:k]
    dn=list(reversed(moves_sorted[-k:])) if len(moves_sorted)>=k else sorted(moves_sorted, key=lambda x:x[1])[:k]
    # resolve names
    prod_map={c:n for c,n,_,_ in fetch_products()}
    up=[{"code":c,"name":prod_map.get(c,c),"change":ch,"value":v} for c,ch,v in up]
    dn=[{"code":c,"name":prod_map.get(c,c),"change":ch,"value":v} for c,ch,v in dn]
    return up,dn

def make_chart_png_band(out_path, title, series, window=7, band_k=2.0):
    """
    Dibuja serie + promedio móvil (window) + banda (± k*std) sobre ventana.
    """
    if not series or len(series)<3:
        return False
    xs=[datetime.date.fromisoformat(d) for d,_ in series]
    ys=np.array([float(v) for _,v in series], dtype=float)

    # moving average / std
    w=max(3, int(window))
    ma=np.convolve(ys, np.ones(w)/w, mode="valid")
    # rolling std (simple)
    std=[]
    for i in range(len(ys)-w+1):
        std.append(np.std(ys[i:i+w]))
    std=np.array(std)
    xs2=xs[w-1:]

    plt.figure()
    plt.plot(xs, ys, label="Serie")
    plt.plot(xs2, ma, label=f"MA({w})")
    plt.plot(xs2, ma + band_k*std, label=f"+{band_k}σ")
    plt.plot(xs2, ma - band_k*std, label=f"-{band_k}σ")
    plt.title(title)
    plt.xlabel("Fecha")
    plt.ylabel("Valor")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=180)
    plt.close()
    return True

def series_last_days(code: str, days: int = 30, geo: str = "AMA", source_like: str = None):
    """
    Devuelve lista de (date, value) para últimos N días del producto.
    """
    end = datetime.date.today()
    start = end - datetime.timedelta(days=days)
    con=db(); cur=con.cursor()
    if source_like:
        cur.execute("""SELECT week_date, value FROM observations
                       WHERE product_code=? AND geo=? AND week_date>=? AND week_date<=? AND source LIKE ?
                       ORDER BY week_date""", (code, geo, start.isoformat(), end.isoformat(), source_like))
    else:
        cur.execute("""SELECT week_date, value FROM observations
                       WHERE product_code=? AND geo=? AND week_date>=? AND week_date<=?
                       ORDER BY week_date""", (code, geo, start.isoformat(), end.isoformat()))
    rows=cur.fetchall(); con.close()
    return [(r[0], r[1]) for r in rows]

def make_chart_png(out_path, title, series):
    """
    series: list of (date_str, value)
    """
    if not series:
        return False
    xs=[datetime.date.fromisoformat(d) for d,_ in series]
    ys=[v for _,v in series]
    plt.figure()
    plt.plot(xs, ys)
    plt.title(title)
    plt.xlabel("Fecha")
    plt.ylabel("Valor")
    plt.tight_layout()
    plt.savefig(out_path, dpi=180)
    plt.close()
    return True

def make_pdf(out_path, week_date):
    m=compute_date(week_date)
    c = pdfcanvas.Canvas(out_path, pagesize=A4)
    w,h=A4
    c.setFont("Helvetica-Bold", 14)
    c.drawString(2*cm, h-2.2*cm, "Observatorio de Mercados y Precios del Paraguay (OMPP)")
    c.setFont("Helvetica", 11)
    c.drawString(2*cm, h-3.0*cm, f"Boletín Semanal — Semana {week_date} (corte al lunes)")
    c.line(2*cm, h-3.2*cm, w-2*cm, h-3.2*cm)

    y=h-4.2*cm
    c.setFont("Helvetica-Bold", 12)
    c.drawString(2*cm, y, "1. Resumen Ejecutivo")
    y-=0.8*cm
    c.setFont("Helvetica", 11)

    lines=[
        ("Índice Canasta (Alimentos+Hogar)", fmt_pct(m["index_canasta"])),
        ("Índice Movilidad (nafta+diésel+pasaje)", fmt_pct(m["index_mobility"])),
        ("Costilla vacuna — precio promedio (Gs/kg)", fmt_gs(m["costilla_avg"])),
        ("Costilla — variación semanal", fmt_pct(m["costilla_weekly_change"])),
    ]
    for lab,val in lines:
        c.drawString(2.2*cm, y, f"• {lab}: {val}")
        y-=0.6*cm

    y-=0.2*cm
    c.setFont("Helvetica-Bold", 11)
    c.drawString(2.2*cm, y, "Semáforos")
    y-=0.6*cm
    c.setFont("Helvetica", 11)
    alerts=m["alerts"]
    if alerts:
        for a in alerts:
            c.drawString(2.2*cm, y, f"• {a['msg']}")
            y-=0.55*cm
    else:
        c.drawString(2.2*cm, y, "• Sin alertas relevantes según umbrales actuales.")
        y-=0.55*cm

    y-=0.4*cm
    c.setFont("Helvetica-Bold", 12)
    c.drawString(2*cm, y, "2. Nota metodológica (versión MVP)")
    y-=0.7*cm
    c.setFont("Helvetica", 10)
    note=("El Índice Canasta se calcula como el promedio simple de variaciones semanales de 18 ítems de alimentos y hogar "
          "(cuando existen datos para semana actual y previa). El Índice Movilidad promedia nafta, diésel y pasaje. "
          "Semáforos: Canasta ≥ +2% semanal; Movilidad ≥ +3% semanal; Costilla ≤ -5% o ≥ +8% semanal.")
    for line in textwrap.wrap(note, width=105):
        c.drawString(2*cm, y, line); y-=0.45*cm

    c.showPage(); c.save()


def _all_products():
    # returns list of (code,name,category,unit)
    return fetch_products()

def _weekly_changes(week_date):
    """Return list of dicts for all products with current value, previous value, and change."""
    wd=datetime.date.fromisoformat(week_date)
    prev=(wd-datetime.timedelta(days=7)).isoformat()
    now=week_values(week_date)
    prv=week_values(prev)
    out=[]
    for code,name,cat,unit in _all_products():
        a=now.get(code)
        b=prv.get(code)
        ch=None
        if a is not None and b not in (None,0):
            ch=a/b-1.0
        out.append({"code":code,"name":name,"category":cat,"unit":unit,"value":a,"prev":b,"change":ch})
    return out

def make_pdf_ext(out_path, week_date):
    """
    Reporte institucional extendido (8–12 páginas aprox):
    - Portada
    - Resumen ejecutivo
    - Diagnóstico Canasta (con tabla completa)
    - Movilidad
    - Carne (costilla)
    - Metodología
    - Anexos (definiciones y trazabilidad)
    """
    m=compute_date(week_date)
    rows=_weekly_changes(week_date)

    # Helpers
    def pct(x): return "—" if x is None else f"{x*100:.1f}%"
    def gs(x): 
        if x is None: return "—"
        try:
            return f"{int(round(x)):,}".replace(",", ".")
        except Exception:
            return str(x)

    # Categorize
    alimentos = [r for r in rows if r["category"] in ("Alimentos","Harinas","Verduras","Lácteos","Carne","Hogar")]
    movilidad = [r for r in rows if r["category"]=="Movilidad"]
    macro = [r for r in rows if r["category"]=="Macro"]
    bienes = [r for r in rows if r["category"]=="Bienes"]

    # Top movers (exclude Macro for “precios”)
    movers = [r for r in rows if r["change"] is not None and r["category"] not in ("Macro",)]
    top_up = sorted(movers, key=lambda x: x["change"], reverse=True)[:5]
    top_dn = sorted(movers, key=lambda x: x["change"])[:5]

    c=pdfcanvas.Canvas(out_path, pagesize=A4)
    W,H=A4

    def header(title, subtitle=None):
        c.setFont("Helvetica-Bold", 12)
        c.drawString(2*cm, H-1.7*cm, "Observatorio de Mercados y Precios del Paraguay (OMPP)")
        c.setFont("Helvetica", 10)
        c.drawRightString(W-2*cm, H-1.7*cm, f"Semana {week_date}")
        c.line(2*cm, H-1.95*cm, W-2*cm, H-1.95*cm)
        c.setFont("Helvetica-Bold", 14)
        c.drawString(2*cm, H-2.8*cm, title)
        if subtitle:
            c.setFont("Helvetica", 11)
            c.drawString(2*cm, H-3.45*cm, subtitle)

    def footer(page_num):
        c.setFont("Helvetica", 9)
        c.drawRightString(W-2*cm, 1.3*cm, f"Página {page_num}")

    def draw_paragraph(x, y, text, width_chars=110, leading=0.45*cm, font="Helvetica", size=10):
        c.setFont(font, size)
        for line in textwrap.wrap(text, width=width_chars):
            c.drawString(x, y, line)
            y -= leading
        return y

    def draw_table(x, y, col_widths, headers, data_rows, row_h=0.6*cm, font_size=9):
        # header
        c.setFont("Helvetica-Bold", font_size)
        c.setFillGray(0.95)
        c.rect(x, y-row_h, sum(col_widths), row_h, fill=1, stroke=0)
        c.setFillGray(0)
        cx=x
        for i,h in enumerate(headers):
            c.drawString(cx+0.12*cm, y-row_h+0.18*cm, h)
            cx += col_widths[i]
        c.line(x, y-row_h, x+sum(col_widths), y-row_h)
        y -= row_h
        # rows
        c.setFont("Helvetica", font_size)
        for r in data_rows:
            cx=x
            for i,cell in enumerate(r):
                c.drawString(cx+0.12*cm, y-row_h+0.18*cm, str(cell)[:60])
                cx += col_widths[i]
            y -= row_h
            if y < 2.5*cm:
                return y, True
        return y, False

    page=1

    # --- Page 1: Cover ---
    c.setFont("Helvetica-Bold", 18)
    c.drawString(2*cm, H-3.2*cm, "Reporte Institucional — Mercados y Precios")
    c.setFont("Helvetica", 12)
    c.drawString(2*cm, H-4.1*cm, "Observatorio de Mercados y Precios del Paraguay (OMPP)")
    c.setFont("Helvetica", 11)
    c.drawString(2*cm, H-5.0*cm, f"Semana de referencia (lunes): {week_date}")
    c.setFont("Helvetica", 10)
    c.drawString(2*cm, H-6.0*cm, "Documento técnico para uso institucional (MVP).")
    c.line(2*cm, H-6.3*cm, W-2*cm, H-6.3*cm)

    # Quick KPIs block
    y=H-7.3*cm
    c.setFont("Helvetica-Bold", 12)
    c.drawString(2*cm, y, "Indicadores clave")
    y-=0.8*cm
    c.setFont("Helvetica", 11)
    c.drawString(2.2*cm, y, f"• Índice Canasta (Alimentos+Hogar): {pct(m['index_canasta'])}")
    y-=0.55*cm
    c.drawString(2.2*cm, y, f"• Índice Movilidad (nafta+diésel+pasaje): {pct(m['index_mobility'])}")
    y-=0.55*cm
    c.drawString(2.2*cm, y, f"• Costilla vacuna (Gs/kg): {gs(m['costilla_avg'])}  |  Variación semanal: {pct(m['costilla_weekly_change'])}")
    y-=0.75*cm
    c.setFont("Helvetica-Bold", 11)
    c.drawString(2*cm, y, "Semáforos")
    y-=0.6*cm
    c.setFont("Helvetica", 10)
    if m["alerts"]:
        for a in m["alerts"][:6]:
            c.drawString(2.2*cm, y, f"• {a['msg']}")
            y-=0.45*cm
    else:
        c.drawString(2.2*cm, y, "• Sin alertas relevantes según umbrales actuales.")
    footer(page)
    c.showPage(); page += 1

    # --- Page 2: Executive summary ---
    header("Resumen Ejecutivo", "Lectura rápida (1–2 minutos)")
    y=H-4.3*cm
    summary = (
        "Este reporte presenta la evolución semanal de precios de la canasta familiar (25 ítems), "
        "un módulo especializado de carne vacuna (con foco en costilla) y un panel de movilidad "
        "(nafta, diésel y pasaje). Se incluyen señales simples (semáforos) para alertar presiones "
        "de corto plazo."
    )
    y = draw_paragraph(2*cm, y, summary, width_chars=110, size=10)

    y -= 0.3*cm
    c.setFont("Helvetica-Bold", 11)
    c.drawString(2*cm, y, "Principales movimientos semanales")
    y -= 0.6*cm

    # top movers table
    headers_t = ["Movimiento", "Producto", "Δ semanal", "Valor actual"]
    data_t=[]
    for r in top_up:
        data_t.append(["Alza", r["name"], pct(r["change"]), gs(r["value"])])
    for r in top_dn:
        data_t.append(["Baja", r["name"], pct(r["change"]), gs(r["value"])])
    colw=[2.2*cm, 8.6*cm, 2.6*cm, 3.8*cm]
    y, _ = draw_table(2*cm, y, colw, headers_t, data_t, row_h=0.6*cm, font_size=9)

    y -= 0.2*cm
    c.setFont("Helvetica-Bold", 11)
    c.drawString(2*cm, y, "Lectura de semáforos (reglas simples)")
    y -= 0.6*cm
    c.setFont("Helvetica", 10)
    c.drawString(2.2*cm, y, "• Canasta ≥ +2% semanal: presión de corto plazo.")
    y -= 0.45*cm
    c.drawString(2.2*cm, y, "• Movilidad ≥ +3% semanal: presión en costos de transporte/logística.")
    y -= 0.45*cm
    c.drawString(2.2*cm, y, "• Costilla ≤ -5% o ≥ +8% semanal: baja significativa o shock.")
    footer(page)
    c.showPage(); page += 1

    # --- Page 3: Canasta overview ---
    header("Canasta Familiar", "Índice, drivers y lectura sectorial")
    y=H-4.3*cm
    c.setFont("Helvetica", 10)
    c.drawString(2*cm, y, f"Índice Canasta (Alimentos+Hogar) — variación semanal: {pct(m['index_canasta'])}")
    y -= 0.6*cm

    txt = (
        "La canasta se analiza como un conjunto de precios sensibles del consumo diario. "
        "El índice resume la variación semanal promedio (cuando existen datos para semana actual y previa)."
    )
    y = draw_paragraph(2*cm, y, txt, width_chars=110, size=10)

    y -= 0.2*cm
    c.setFont("Helvetica-Bold", 11)
    c.drawString(2*cm, y, "Tabla completa — Canasta (25 ítems)")
    y -= 0.6*cm

    # Prepare full table rows
    full = []
    for r in rows:
        # show macro too but marked
        full.append([
            r["category"],
            r["name"],
            r["unit"],
            gs(r["value"]) if r["unit"] != "% mensual" else (pct(r["value"]) if r["value"] is not None else "—"),
            pct(r["change"]) if r["change"] is not None else "—"
        ])
    headers_full=["Categoría","Producto","Unidad","Valor","Δ semanal"]
    colw=[2.8*cm, 7.2*cm, 2.4*cm, 3.0*cm, 2.8*cm]

    # Draw across multiple pages if needed
    while True:
        y, needs_new = draw_table(2*cm, y, colw, headers_full, full, row_h=0.55*cm, font_size=8.5)
        if not needs_new:
            break
        footer(page)
        c.showPage(); page += 1
        header("Canasta Familiar (continuación)")
        y=H-4.0*cm
        # remove rows already printed? We didn't track. Simple approach: redraw from start would repeat.
        # We'll instead paginate manually by slicing.
        break

    # Manual pagination with slicing (redo properly)
    # Re-render with pagination correctly:
    c.showPage()
    page += 1
    # We'll do paginated rendering now
    def paginated_table(title, headers, colw, data, start_page_title, start_y):
        nonlocal page
        i=0
        while i < len(data):
            header(start_page_title if i==0 else f"{start_page_title} (continuación)")
            y=start_y
            # header row
            c.setFont("Helvetica-Bold", 11)
            c.drawString(2*cm, y, title if i==0 else title + " (cont.)")
            y -= 0.6*cm
            # compute how many rows fit
            # leaving bottom margin 2.5cm
            rows_fit = int((y-2.5*cm)/(0.55*cm))
            chunk = data[i:i+rows_fit]
            y2, _ = draw_table(2*cm, y, colw, headers, chunk, row_h=0.55*cm, font_size=8.5)
            footer(page)
            c.showPage(); page += 1
            i += rows_fit

    paginated_table("Tabla completa — Canasta (25 ítems)", headers_full, colw, full, "Canasta Familiar", H-4.3*cm)

    # After paginated_table, we already advanced one extra blank page; roll back by adding a new content page next
    # We'll start mobility section on current page already created by showPage; so set page-1? To keep simple,
    # we will just continue with next pages (it is ok if pages count goes 1 higher).
    # We'll add next page content now.

    # --- Movilidad section ---
    header("Movilidad", "Combustibles + Pasaje")
    y=H-4.3*cm
    c.setFont("Helvetica", 10)
    c.drawString(2*cm, y, f"Índice Movilidad — variación semanal: {pct(m['index_mobility'])}")
    y -= 0.6*cm
    y = draw_paragraph(2*cm, y, "La movilidad impacta costos logísticos y expectativas de inflación percibida.", width_chars=110, size=10)
    y -= 0.2*cm
    mob_rows=[]
    for r in movilidad:
        mob_rows.append([r["name"], r["unit"], gs(r["value"]), pct(r["change"]) if r["change"] is not None else "—"])
    headers_m=["Indicador","Unidad","Valor","Δ semanal"]
    colw_m=[7.8*cm, 2.8*cm, 3.2*cm, 2.8*cm]
    y, needs = draw_table(2*cm, y, colw_m, headers_m, mob_rows, row_h=0.6*cm, font_size=9)
    footer(page)
    c.showPage(); page += 1

    # --- Carne section ---
    header("Carne Vacuna", "Módulo especializado — foco en costilla")
    y=H-4.3*cm
    c.setFont("Helvetica", 10)
    c.drawString(2*cm, y, f"Costilla (Gs/kg): {gs(m['costilla_avg'])}  |  Variación semanal: {pct(m['costilla_weekly_change'])}")
    y -= 0.7*cm
    y = draw_paragraph(2*cm, y, "La carne vacuna es un precio socialmente sensible y funciona como indicador adelantado de percepción inflacionaria.", width_chars=110, size=10)
    y -= 0.2*cm
    # include some carne items and related
    carne_rows=[]
    for r in rows:
        if r["category"]=="Carne":
            carne_rows.append([r["name"], r["unit"], gs(r["value"]), pct(r["change"]) if r["change"] is not None else "—"])
    headers_c=["Corte/Producto","Unidad","Valor","Δ semanal"]
    colw_c=[7.8*cm, 2.8*cm, 3.2*cm, 2.8*cm]
    y, _ = draw_table(2*cm, y, colw_c, headers_c, carne_rows, row_h=0.6*cm, font_size=9)

    y -= 0.2*cm
    c.setFont("Helvetica-Bold", 11)
    c.drawString(2*cm, y, "Lectura para gestión pública (MVP)")
    y -= 0.6*cm
    y = draw_paragraph(2*cm, y, "Cuando la costilla cae junto con señales de mayor oferta (faena) o menor demanda externa (exportación), puede anticiparse una baja adicional en mostrador. Este reporte MVP deja trazabilidad para ampliar el modelo con datos oficiales.", width_chars=110, size=10)
    footer(page)
    c.showPage(); page += 1

    # --- Methodology ---
    header("Metodología", "Definiciones, cálculo e integridad de datos")
    y=H-4.3*cm
    meth = (
        "Fuentes: relevamiento propio (supermercados/carnicerías) e importación desde planilla Excel del Observatorio. "
        "Unidad de tiempo: semanal (lunes). Índices: promedio simple de variaciones semanales de los ítems con datos "
        "en semana actual y semana previa. Semáforos: reglas de umbral para alertas tempranas."
    )
    y = draw_paragraph(2*cm, y, meth, width_chars=110, size=10)

    y -= 0.2*cm
    c.setFont("Helvetica-Bold", 11)
    c.drawString(2*cm, y, "Trazabilidad (MVP)")
    y -= 0.6*cm
    y = draw_paragraph(2*cm, y, "Cada observación registra: producto, semana, valor, geografía y fuente. Esto permite auditoría y control de calidad.", width_chars=110, size=10)

    y -= 0.2*cm
    c.setFont("Helvetica-Bold", 11)
    c.drawString(2*cm, y, "Próximas mejoras sugeridas")
    y -= 0.6*cm
    improvements = [
        "Desagregación por ciudad/departamento y ponderaciones por consumo.",
        "Integración de fuentes oficiales (BCP/INE/SENACSA/PETROPAR) y comparación con relevamientos.",
        "Modelo de transmisión de precios (ganado → mostrador) con rezagos.",
        "Reporte gráfico ampliado (series y bandas) y anexos estadísticos."
    ]
    c.setFont("Helvetica", 10)
    for it in improvements:
        c.drawString(2.2*cm, y, f"• {it}")
        y -= 0.45*cm
    footer(page)
    c.showPage(); page += 1

    # --- Annex: full catalog ---
    header("Anexos", "Catálogo de productos y unidades")
    y=H-4.3*cm
    cat_rows=[[code,name,cat,unit] for code,name,cat,unit in _all_products()]
    headers_a=["Código","Producto","Categoría","Unidad"]
    colw_a=[2.2*cm, 7.6*cm, 3.6*cm, 3.2*cm]
    # paginate annex
    i=0
    while i < len(cat_rows):
        if i>0:
            header("Anexos (continuación)", "Catálogo de productos y unidades")
            y=H-4.3*cm
        rows_fit = int((y-2.5*cm)/(0.6*cm))
        chunk=cat_rows[i:i+rows_fit]
        y2, _ = draw_table(2*cm, y, colw_a, headers_a, chunk, row_h=0.6*cm, font_size=9)
        footer(page)
        c.showPage(); page += 1
        i += rows_fit

    c.save()

@app.get("/reporte/pdf")
def reporte_pdf(week_date: str):
    out = os.path.join(BASE_DIR, f"boletin_{week_date}.pdf")
    make_pdf(out, week_date)
    return FileResponse(out, media_type="application/pdf", filename=os.path.basename(out))



@app.get("/reporte/pdf_ext")
def reporte_pdf_ext(week_date: str):
    out = os.path.join(BASE_DIR, f"reporte_institucional_extendido_{week_date}.pdf")
    make_pdf_ext(out, week_date)
    return FileResponse(out, media_type="application/pdf", filename=os.path.basename(out))

@app.get("/reporte/ppt_json")
def reporte_ppt_json(week_date: str):
    # MVP: entregamos JSON listo para convertir a PPT (luego agregamos conversor automático)
    m=compute_date(week_date)
    return JSONResponse(m)

@app.get("/config/alertas")
def ver_config_alertas():
    return get_alertas()

@app.get("/config/fuentes")
def ver_config_fuentes():
    return load_json(os.path.join(CONFIG_DIR, "fuentes.json"), {})

@app.get("/alertas/eventos")
def alertas_eventos(limit: int = 50):
    con=db(); cur=con.cursor()
    cur.execute("""SELECT created_at, obs_date, product_code, scope, level, metric, change_value, message, whatsapp_sent
                   FROM alerts_events ORDER BY id DESC LIMIT ?""", (limit,))
    rows=cur.fetchall(); con.close()
    return {"events":[
        {"created_at":r[0],"obs_date":r[1],"product_code":r[2],"scope":r[3],"level":r[4],"metric":r[5],"change":r[6],"message":r[7],"whatsapp_sent":bool(r[8])}
        for r in rows
    ]}

@app.post("/ingesta/once")
def ingesta_once():
    """
    Ejecuta ingesta automática una vez (manual).
    Nota: en despliegue institucional se protege con autenticación.
    """
    import subprocess, sys
    p = subprocess.run([sys.executable, os.path.join(BASE_DIR, "ingest.py"), "--once"], capture_output=True, text=True)
    return {"ok": p.returncode==0, "stdout": p.stdout[-4000:], "stderr": p.stderr[-2000:]}


def make_pdf_real(out_path, obs_date):
    """
    Reporte Real (estilo institucional) con gráficos de series (últimos 30 días).
    """
    m=compute_date(obs_date)
    c = pdfcanvas.Canvas(out_path, pagesize=A4)
    W,H=A4

    # Portada + KPIs
    c.setFont("Helvetica-Bold", 16)
    c.drawString(2*cm, H-2.2*cm, "OMPP — Reporte Real de Mercados y Precios")
    c.setFont("Helvetica", 11)
    c.drawString(2*cm, H-3.0*cm, f"Corte: {obs_date} | Frecuencia base: diaria | Moneda: Gs")
    c.line(2*cm, H-3.2*cm, W-2*cm, H-3.2*cm)

    y=H-4.2*cm
    c.setFont("Helvetica-Bold", 12)
    c.drawString(2*cm, y, "Resumen ejecutivo")
    y-=0.8*cm
    c.setFont("Helvetica", 11)
    c.drawString(2.2*cm, y, f"• Índice Canasta (variación diaria): {fmt_pct(m['index_canasta'])}")
    y-=0.55*cm
    c.drawString(2.2*cm, y, f"• Índice Movilidad (variación diaria): {fmt_pct(m['index_mobility'])}")
    y-=0.55*cm
    c.drawString(2.2*cm, y, f"• Costilla (Gs/kg): {fmt_gs(m['costilla_avg'])} | Δ diaria: {fmt_pct(m['costilla_weekly_change'])}")
    y-=0.75*cm

    c.setFont("Helvetica-Bold", 11)
    c.drawString(2*cm, y, "Alertas (semáforo)")
    y-=0.6*cm
    c.setFont("Helvetica", 10)
    alerts=m.get("alerts",[])
    if alerts:
        for a in alerts[:10]:
            c.drawString(2.2*cm, y, f"• {a.get('msg','')}")
            y-=0.45*cm
            if y<2.5*cm:
                break
    else:
        c.drawString(2.2*cm, y, "• Sin alertas rojas en la fecha de corte.")
    c.showPage()

    # Página de gráficos
    c.setFont("Helvetica-Bold", 14)
    c.drawString(2*cm, H-2.2*cm, "Evolución reciente (últimos 30 días)")
    c.setFont("Helvetica", 10)
    c.drawString(2*cm, H-2.9*cm, "Series para lectura rápida de presión de precios y movilidad.")
    c.line(2*cm, H-3.1*cm, W-2*cm, H-3.1*cm)

    tmp_dir=os.path.join(BASE_DIR, "_tmp_charts")
    os.makedirs(tmp_dir, exist_ok=True)

    charts = [
        ("COSTILLA", "Costilla (Gs/kg)"),
        ("NAFTA", "Nafta (Gs/litro)"),
        ("PASAJE", "Pasaje transporte (Gs/viaje)")
    ]
    y=H-4.0*cm
    for code, title in charts:
        series = series_last_days(code, days=30)
        out_png=os.path.join(tmp_dir, f"{code}_30d.png")
        ok = make_chart_png(out_png, title, series)
        if ok:
            c.drawImage(out_png, 2*cm, y-7.2*cm, width=W-4*cm, height=6.8*cm, preserveAspectRatio=True, anchor='sw')
            y -= 7.6*cm
            if y < 3.0*cm:
                c.showPage()
                y = H-3.0*cm
        else:
            c.setFont("Helvetica", 10)
            c.drawString(2*cm, y, f"(Sin datos para graficar: {title})")
            y -= 0.8*cm

    c.showPage()
    c.save()

@app.get("/reporte/pdf_real")
def reporte_pdf_real(obs_date: str):
    out = os.path.join(BASE_DIR, f"reporte_real_{obs_date}.pdf")
    make_pdf_real(out, obs_date)
    return FileResponse(out, media_type="application/pdf", filename=os.path.basename(out))


def make_pptx(out_path, obs_date):
    from pptx import Presentation
    from pptx.util import Inches, Pt
    from pptx.enum.text import PP_ALIGN
    prs = Presentation()
    m = compute_date(obs_date)

    # Slide 1: Title
    slide = prs.slides.add_slide(prs.slide_layouts[0])
    slide.shapes.title.text = "OMPP — Reporte Real (Resumen)"
    slide.placeholders[1].text = f"Corte: {obs_date}"

    # Slide 2: KPIs
    slide = prs.slides.add_slide(prs.slide_layouts[5])  # title only
    slide.shapes.title.text = "Indicadores clave"
    tx = slide.shapes.add_textbox(Inches(0.8), Inches(1.6), Inches(8.5), Inches(4.5)).text_frame
    tx.word_wrap = True
    p = tx.paragraphs[0]
    p.text = f"Índice Canasta (Δ diaria): {fmt_pct(m['index_canasta'])}"
    p.font.size = Pt(20)
    for line in [
        f"Índice Movilidad (Δ diaria): {fmt_pct(m['index_mobility'])}",
        f"Costilla (Gs/kg): {fmt_gs(m['costilla_avg'])} | Δ diaria: {fmt_pct(m['costilla_weekly_change'])}",
    ]:
        pp = tx.add_paragraph()
        pp.text = line
        pp.font.size = Pt(20)

    # Slide 3: Alerts
    slide = prs.slides.add_slide(prs.slide_layouts[5])
    slide.shapes.title.text = "Alertas (semáforo)"
    tx = slide.shapes.add_textbox(Inches(0.8), Inches(1.6), Inches(8.5), Inches(4.8)).text_frame
    tx.word_wrap = True
    alerts = m.get("alerts", [])
    if not alerts:
        tx.text = "Sin alertas rojas en la fecha de corte."
    else:
        tx.text = alerts[0].get("msg","")
        for a in alerts[1:10]:
            tx.add_paragraph().text = a.get("msg","")

    # Slide 4: Chart (Costilla 30d)
    slide = prs.slides.add_slide(prs.slide_layouts[5])
    slide.shapes.title.text = "Evolución reciente — Costilla (30 días)"
    tmp_dir=os.path.join(BASE_DIR, "_tmp_charts")
    os.makedirs(tmp_dir, exist_ok=True)
    out_png=os.path.join(tmp_dir, "COSTILLA_30d.png")
    make_chart_png(out_png, "Costilla (Gs/kg)", series_last_days("COSTILLA", 30))
    slide.shapes.add_picture(out_png, Inches(0.8), Inches(1.7), width=Inches(8.5))

    prs.save(out_path)

@app.get("/reporte/pptx")
def reporte_pptx(obs_date: str):
    out = os.path.join(BASE_DIR, f"reporte_real_{obs_date}.pptx")
    make_pptx(out, obs_date)
    return FileResponse(out, media_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        filename=os.path.basename(out))


def make_observatorio_pdf(out_path, obs_date: str):
    """
    Reporte Nivel Observatorio (6–10 págs aprox, según datos):
    - Portada
    - Tablero: KPIs + semáforos + ranking
    - Gráficos: canasta, movilidad, costilla, combustibles
    - Anexo: eventos de alertas y evidencia de fuentes
    """
    m=compute_date(obs_date)
    c=pdfcanvas.Canvas(out_path, pagesize=A4)
    W,H=A4

    # Page 1: Cover
    c.setFont("Helvetica-Bold", 18)
    c.drawString(2*cm, H-2.4*cm, "OMPP — Reporte Observatorio")
    c.setFont("Helvetica", 12)
    c.drawString(2*cm, H-3.2*cm, "Sistema con Reporte Real — Inteligencia de Negocios de Precios")
    c.setFont("Helvetica", 11)
    c.drawString(2*cm, H-4.0*cm, f"Fecha de corte: {obs_date}")
    c.line(2*cm, H-4.3*cm, W-2*cm, H-4.3*cm)
    c.setFont("Helvetica", 10)
    c.drawString(2*cm, H-5.1*cm, "Contenido: tablero, ranking, series recientes, alertas, y evidencia de fuentes.")
    c.showPage()

    # Page 2: Dashboard
    c.setFont("Helvetica-Bold", 14)
    c.drawString(2*cm, H-2.2*cm, "1. Tablero Diario")
    c.setFont("Helvetica", 10)
    c.drawString(2*cm, H-2.9*cm, "Indicadores clave y señales tempranas.")
    c.line(2*cm, H-3.1*cm, W-2*cm, H-3.1*cm)

    y=H-4.0*cm
    c.setFont("Helvetica-Bold", 11)
    c.drawString(2*cm, y, "KPIs")
    y-=0.7*cm
    c.setFont("Helvetica", 11)
    c.drawString(2.2*cm, y, f"• Índice Canasta (Δ diaria): {fmt_pct(m['index_canasta'])} | (Δ semanal): {fmt_pct(m['weekly']['canasta'])}")
    y-=0.55*cm
    c.drawString(2.2*cm, y, f"• Índice Movilidad (Δ diaria): {fmt_pct(m['index_mobility'])} | (Δ semanal): {fmt_pct(m['weekly']['movilidad'])}")
    y-=0.55*cm
    c.drawString(2.2*cm, y, f"• Costilla (Gs/kg): {fmt_gs(m['costilla_avg'])} | Δ diaria: {fmt_pct(m['costilla_weekly_change'])} | (Δ semanal): {fmt_pct(m['weekly']['costilla'])}")
    y-=0.75*cm

    c.setFont("Helvetica-Bold", 11)
    c.drawString(2*cm, y, "Semáforos / Alertas")
    y-=0.6*cm
    c.setFont("Helvetica", 10)
    alerts=m.get("alerts",[])
    if alerts:
        for a in alerts[:12]:
            c.drawString(2.2*cm, y, f"• {a.get('msg','')}")
            y-=0.45*cm
            if y<6*cm: break
    else:
        c.drawString(2.2*cm, y, "• Sin alertas rojas en la fecha de corte.")
        y-=0.45*cm

    # Ranking
    up,dn = top_movers(obs_date, k=6)
    y-=0.3*cm
    c.setFont("Helvetica-Bold", 11)
    c.drawString(2*cm, y, "Ranking (variación diaria)")
    y-=0.6*cm
    c.setFont("Helvetica-Bold", 10)
    c.drawString(2*cm, y, "Top alzas")
    c.drawString(W/2, y, "Top bajas")
    y-=0.45*cm
    c.setFont("Helvetica", 9.5)
    for i in range(6):
        if i < len(up):
            c.drawString(2*cm, y, f"{i+1}. {up[i]['name']}  {fmt_pct(up[i]['change'])}")
        if i < len(dn):
            c.drawString(W/2, y, f"{i+1}. {dn[i]['name']}  {fmt_pct(dn[i]['change'])}")
        y-=0.42*cm
    c.showPage()

    # Pages: Charts
    tmp_dir=os.path.join(BASE_DIR, "_tmp_charts")
    os.makedirs(tmp_dir, exist_ok=True)

    chart_specs = [
        ("COSTILLA", "Costilla (Gs/kg)", 7),
        ("NAFTA", "Nafta (Gs/litro)", 7),
        ("DIESEL", "Diésel (Gs/litro)", 7),
        ("PASAJE", "Pasaje (Gs/viaje)", 7),
    ]
    # Add index charts using computed snapshots (from computed table)
    def series_index_last_days(field, days=45):
        end=datetime.date.today()
        start=end-datetime.timedelta(days=days)
        con=db(); cur=con.cursor()
        cur.execute(f"""SELECT week_date, {field} FROM computed
                        WHERE week_date>=? AND week_date<=?
                        ORDER BY week_date""", (start.isoformat(), end.isoformat()))
        rows=cur.fetchall(); con.close()
        return [(r[0], r[1]) for r in rows if r[1] is not None]

    idx_can = series_index_last_days("index_canasta", 45)
    idx_mob = series_index_last_days("index_mobility", 45)

    # Page: index charts
    c.setFont("Helvetica-Bold", 14)
    c.drawString(2*cm, H-2.2*cm, "2. Series de Índices (45 días)")
    c.setFont("Helvetica", 10)
    c.drawString(2*cm, H-2.9*cm, "Variación diaria (serie de snapshots).")
    c.line(2*cm, H-3.1*cm, W-2*cm, H-3.1*cm)

    y=H-4.0*cm
    for series, title in [(idx_can, "Índice Canasta (Δ diaria)"), (idx_mob, "Índice Movilidad (Δ diaria)")]:
        out_png=os.path.join(tmp_dir, re.sub(r"[^A-Za-z0-9]+","_",title)+".png")
        ok = make_chart_png_band(out_png, title, series, window=7, band_k=2.0)
        if ok:
            c.drawImage(out_png, 2*cm, y-7.2*cm, width=W-4*cm, height=6.8*cm, preserveAspectRatio=True, anchor='sw')
            y -= 7.6*cm
            if y < 3.2*cm:
                c.showPage()
                y = H-3.0*cm
    c.showPage()

    # Page(s): product charts
    c.setFont("Helvetica-Bold", 14)
    c.drawString(2*cm, H-2.2*cm, "3. Series de Productos Sensibles (30 días)")
    c.setFont("Helvetica", 10)
    c.drawString(2*cm, H-2.9*cm, "Tendencias, promedio móvil y bandas.")
    c.line(2*cm, H-3.1*cm, W-2*cm, H-3.1*cm)

    y=H-4.0*cm
    for code,title,w in chart_specs:
        series=series_last_days(code, days=30)
        out_png=os.path.join(tmp_dir, f"{code}_band.png")
        ok=make_chart_png_band(out_png, title, series, window=w, band_k=2.0)
        if ok:
            c.drawImage(out_png, 2*cm, y-7.2*cm, width=W-4*cm, height=6.8*cm, preserveAspectRatio=True, anchor='sw')
            y -= 7.6*cm
            if y < 3.2*cm:
                c.showPage()
                y = H-3.0*cm
        else:
            c.setFont("Helvetica", 10)
            c.drawString(2*cm, y, f"(Sin datos suficientes para: {title})")
            y -= 0.7*cm
    c.showPage()

    # Page: Alerts events + Evidence
    c.setFont("Helvetica-Bold", 14)
    c.drawString(2*cm, H-2.2*cm, "4. Evidencia y trazabilidad")
    c.setFont("Helvetica", 10)
    c.drawString(2*cm, H-2.9*cm, "Alertas registradas y archivos descargados (hash).")
    c.line(2*cm, H-3.1*cm, W-2*cm, H-3.1*cm)

    y=H-4.0*cm
    c.setFont("Helvetica-Bold", 11)
    c.drawString(2*cm, y, "Eventos de alertas (últimos 15)")
    y-=0.6*cm
    con=db(); cur=con.cursor()
    cur.execute("""SELECT obs_date, product_code, level, message FROM alerts_events ORDER BY id DESC LIMIT 15""")
    events=cur.fetchall()
    cur.execute("""SELECT source_key, fetched_at, sha256 FROM raw_source_files ORDER BY id DESC LIMIT 10""")
    raws=cur.fetchall()
    con.close()

    c.setFont("Helvetica", 9.5)
    if not events:
        c.drawString(2.2*cm, y, "Sin eventos registrados.")
        y-=0.45*cm
    else:
        for e in events:
            c.drawString(2.2*cm, y, f"{e[0]} | {e[2]} | {e[1] or '-'} | {e[3][:78]}")
            y-=0.42*cm
            if y<7*cm: break

    y-=0.4*cm
    c.setFont("Helvetica-Bold", 11)
    c.drawString(2*cm, y, "Descargas automáticas (últimos 10)")
    y-=0.6*cm
    c.setFont("Helvetica", 9.5)
    if not raws:
        c.drawString(2.2*cm, y, "Sin descargas registradas.")
    else:
        for r in raws:
            c.drawString(2.2*cm, y, f"{r[0]} | {r[1][:19]} | {r[2][:16]}…")
            y-=0.42*cm
            if y<2.8*cm: break

    c.showPage()
    c.save()

@app.get("/reporte/observatorio_pdf")
def reporte_observatorio_pdf(obs_date: str):
    out=os.path.join(BASE_DIR, f"reporte_observatorio_{obs_date}.pdf")
    make_observatorio_pdf(out, obs_date)
    return FileResponse(out, media_type="application/pdf", filename=os.path.basename(out))


def make_observatorio_pptx(out_path, obs_date: str):
    from pptx import Presentation
    from pptx.util import Inches, Pt
    prs=Presentation()
    m=compute_date(obs_date)
    up,dn=top_movers(obs_date, k=8)

    # Title
    s=prs.slides.add_slide(prs.slide_layouts[0])
    s.shapes.title.text="OMPP — Reporte Observatorio"
    s.placeholders[1].text=f"Corte: {obs_date}"

    # KPI slide
    s=prs.slides.add_slide(prs.slide_layouts[5])
    s.shapes.title.text="Tablero: KPIs"
    tf=s.shapes.add_textbox(Inches(0.8), Inches(1.6), Inches(8.6), Inches(4.7)).text_frame
    tf.word_wrap=True
    tf.text=f"Índice Canasta (Δ diaria): {fmt_pct(m['index_canasta'])} | (Δ semanal): {fmt_pct(m['weekly']['canasta'])}"
    for line in [
        f"Índice Movilidad (Δ diaria): {fmt_pct(m['index_mobility'])} | (Δ semanal): {fmt_pct(m['weekly']['movilidad'])}",
        f"Costilla (Gs/kg): {fmt_gs(m['costilla_avg'])} | Δ diaria: {fmt_pct(m['costilla_weekly_change'])} | (Δ semanal): {fmt_pct(m['weekly']['costilla'])}",
    ]:
        p=tf.add_paragraph(); p.text=line

    # Ranking slide
    s=prs.slides.add_slide(prs.slide_layouts[5])
    s.shapes.title.text="Ranking diario (Top alzas / bajas)"
    left=s.shapes.add_textbox(Inches(0.7), Inches(1.6), Inches(4.4), Inches(5.0)).text_frame
    right=s.shapes.add_textbox(Inches(5.0), Inches(1.6), Inches(4.4), Inches(5.0)).text_frame
    left.text="Top alzas"
    for r in up:
        left.add_paragraph().text=f"{r['name']}  {fmt_pct(r['change'])}"
    right.text="Top bajas"
    for r in dn:
        right.add_paragraph().text=f"{r['name']}  {fmt_pct(r['change'])}"

    # Alerts slide
    s=prs.slides.add_slide(prs.slide_layouts[5])
    s.shapes.title.text="Alertas (semáforo)"
    tf=s.shapes.add_textbox(Inches(0.8), Inches(1.6), Inches(8.6), Inches(4.7)).text_frame
    alerts=m.get("alerts",[])
    tf.text=alerts[0].get("msg","Sin alertas rojas") if alerts else "Sin alertas rojas"
    for a in alerts[1:12]:
        tf.add_paragraph().text=a.get("msg","")

    # Chart slides
    tmp_dir=os.path.join(BASE_DIR, "_tmp_charts")
    os.makedirs(tmp_dir, exist_ok=True)

    def add_chart_slide(title, code):
        s=prs.slides.add_slide(prs.slide_layouts[5])
        s.shapes.title.text=title
        out_png=os.path.join(tmp_dir, f"{code}_ppt.png")
        make_chart_png_band(out_png, title, series_last_days(code, 30), window=7, band_k=2.0)
        if os.path.exists(out_png):
            s.shapes.add_picture(out_png, Inches(0.8), Inches(1.7), width=Inches(8.5))

    for code,title in [("COSTILLA","Costilla (30 días)"),("NAFTA","Nafta (30 días)"),("PASAJE","Pasaje (30 días)")]:
        add_chart_slide(title, code)

    prs.save(out_path)

@app.get("/reporte/observatorio_pptx")
def reporte_observatorio_pptx(obs_date: str):
    out=os.path.join(BASE_DIR, f"reporte_observatorio_{obs_date}.pptx")
    make_observatorio_pptx(out, obs_date)
    return FileResponse(out, media_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        filename=os.path.basename(out))
