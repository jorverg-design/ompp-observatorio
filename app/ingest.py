
import os, json, datetime, hashlib
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import re

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "ompp.sqlite")
CONFIG_DIR = os.path.join(BASE_DIR, "config")
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
os.makedirs(RAW_DIR, exist_ok=True)

FUENTES_PATH = os.path.join(CONFIG_DIR, "fuentes.json")
MAPPING_SEDECO = os.path.join(CONFIG_DIR, "mapping_sedeco.json")

def db():
    return sqlite3.connect(DB_PATH)

def load_json(path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default if default is not None else {}

def sha256_file(path):
    h=hashlib.sha256()
    with open(path,"rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def save_raw(source_key, url, content_bytes, ext):
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    fname = f"{source_key}_{ts}.{ext}"
    path = os.path.join(RAW_DIR, fname)
    with open(path,"wb") as f:
        f.write(content_bytes)
    s = sha256_file(path)
    con=db(); cur=con.cursor()
    cur.execute("""INSERT INTO raw_source_files(created_at,source_key,url,fetched_at,sha256,file_path)
                   VALUES (?,?,?,?,?,?)""",
                (datetime.datetime.utcnow().isoformat(), source_key, url, datetime.datetime.utcnow().isoformat(), s, path))
    con.commit(); con.close()
    return path, s

def upsert_obs(code, obs_date, value, geo="AMA", source="Automático"):
    con=db(); cur=con.cursor()
    cur.execute(
        """INSERT INTO observations(product_code,week_date,value,geo,source,created_at)
           VALUES (?,?,?,?,?,?)
           ON CONFLICT(product_code,week_date,geo,source) DO UPDATE SET
             value=excluded.value, created_at=excluded.created_at""",
        (code, obs_date, float(value), geo, source, datetime.datetime.utcnow().isoformat())
    )
    con.commit(); con.close()

def fetch_sedeco_dataset(fuente_cfg):
    url = fuente_cfg["url"]
    html = requests.get(url, timeout=40).text
    soup = BeautifulSoup(html, "html.parser")

    links=[]
    for a in soup.select("a"):
        href=a.get("href") or ""
        if "download" in href and any(href.lower().endswith(ext) for ext in (".csv",".xls",".xlsx")):
            links.append(href)
    def absu(h):
        return h if h.startswith("http") else ("https://www.datos.gov.py" + h)
    links=[absu(h) for h in links]
    if not links:
        raise RuntimeError("No se encontraron recursos descargables en datos.gov.py (verificar cambios en la página).")

    resource_url = links[0]
    resp = requests.get(resource_url, timeout=60)
    ext = resource_url.split("?")[0].split(".")[-1].lower()
    raw_path, _ = save_raw("sedeco", resource_url, resp.content, ext)

    mapping = load_json(MAPPING_SEDECO, {})
    name_to_code = mapping.get("name_to_code", {})

    if ext == "csv":
        df = pd.read_csv(raw_path)
    else:
        df = pd.read_excel(raw_path)

    cols={c.lower():c for c in df.columns}
    col_date = cols.get("fecha") or cols.get("date") or cols.get("dia") or df.columns[0]
    col_item = cols.get("producto") or cols.get("item") or cols.get("articulo") or None
    col_price = cols.get("precio") or cols.get("valor") or None

    if col_item is None or col_price is None:
        for c in df.columns:
            cl=c.lower()
            if col_item is None and any(k in cl for k in ["producto","product","item","art"]):
                col_item=c
            if col_price is None and any(k in cl for k in ["precio","valor","price"]):
                col_price=c
    if col_item is None or col_price is None:
        raise RuntimeError("No se pudieron identificar columnas producto/precio en dataset SEDECO.")

    imported=0
    for _, r in df.iterrows():
        item=str(r[col_item]).strip()
        if item not in name_to_code:
            continue
        code=name_to_code[item]
        val=r[col_price]
        if pd.isna(val):
            continue
        d=r[col_date]
        if isinstance(d, datetime.datetime):
            obs_date=d.date().isoformat()
        elif isinstance(d, datetime.date):
            obs_date=d.isoformat()
        else:
            obs_date=str(d)[:10]
        upsert_obs(code, obs_date, float(val), source="SEDECO")
        imported += 1

    return {"source":"sedeco","resource_url":resource_url,"raw_file":raw_path,"imported":imported}

def fetch_petropar_prices(fuente_cfg):
    url = fuente_cfg.get("url")
    html = requests.get(url, timeout=40).text
    raw_path,_ = save_raw("petropar", url, html.encode("utf-8"), "html")
    soup=BeautifulSoup(html,"html.parser")

    rows=[]
    for tr in soup.select("tr"):
        tds=[td.get_text(" ", strip=True) for td in tr.select("td")]
        if len(tds) >= 2:
            rows.append(tds)

    today = datetime.date.today().isoformat()
    imported=0
    for tds in rows:
        prod=" ".join(tds[:-1]).strip()
        last=tds[-1]
        m=re.search(r"([0-9]{1,3}(?:\.[0-9]{3})+)", last)
        if not m:
            m=re.search(r"([0-9]{4,})", last.replace(",",""))
        if not m:
            continue
        price=float(m.group(1).replace(".",""))
        pl=prod.lower()
        if "nafta" in pl:
            upsert_obs("NAFTA", today, price, source="PETROPAR"); imported += 1
        elif "di" in pl and "esel" in pl:
            upsert_obs("DIESEL", today, price, source="PETROPAR"); imported += 1
        elif "gas" in pl:
            upsert_obs("GAS", today, price, source="PETROPAR"); imported += 1
    return {"source":"petropar","raw_file":raw_path,"imported":imported,"date":today}

def fetch_bcp_ipc(fuente_cfg):
    url = fuente_cfg.get("url")
    html = requests.get(url, timeout=40).text
    raw_path,_ = save_raw("bcp_ipc", url, html.encode("utf-8"), "html")
    return {"source":"bcp_ipc","raw_file":raw_path,"imported":0}



def fetch_bcp_ipc_excel(fuente_cfg):
    """
    v3: intenta descubrir y descargar un Excel (xls/xlsx) desde la página de IPC o anexo.
    Si lo encuentra, lo guarda como evidencia y trata de extraer algunas series (si la estructura lo permite).
    """
    base_urls = [fuente_cfg.get("alt_url"), fuente_cfg.get("url")]
    base_urls = [u for u in base_urls if u]
    excel_links = []
    for url in base_urls:
        html = requests.get(url, timeout=40).text
        # guardar evidencia HTML
        save_raw("bcp_ipc_page", url, html.encode("utf-8"), "html")
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.select("a"):
            href = a.get("href") or ""
            if any(href.lower().endswith(ext) for ext in (".xls", ".xlsx")):
                excel_links.append(href if href.startswith("http") else ("https://www.bcp.gov.py" + href))
    if not excel_links:
        # si no hay excel, dejamos evidencia y salimos sin error duro
        return {"source":"bcp_ipc_excel","imported":0,"note":"No se encontró link xls/xlsx en páginas configuradas."}

    xurl = excel_links[0]
    resp = requests.get(xurl, timeout=80)
    ext = xurl.split("?")[0].split(".")[-1].lower()
    raw_path, _ = save_raw("bcp_ipc_excel", xurl, resp.content, ext)

    # parseo mínimo (best effort)
    imported = 0
    try:
        df = pd.read_excel(raw_path, sheet_name=0)
        # Heurística: si hay columnas con "Fecha" y alguna columna "IPC" o "Alimentos"
        cols = {c.lower(): c for c in df.columns}
        col_date = None
        for k in ["fecha","periodo","mes","date"]:
            if k in cols:
                col_date = cols[k]; break
        # buscar una columna ipc general
        target_cols = []
        for c in df.columns:
            cl = str(c).lower()
            if "ipc" in cl and ("general" in cl or "total" in cl):
                target_cols.append(c)
            if "alimento" in cl and ("ipc" in cl or "vari" in cl):
                target_cols.append(c)
        if col_date and target_cols:
            for _, r in df.iterrows():
                d = r[col_date]
                if pd.isna(d): 
                    continue
                if isinstance(d, datetime.datetime):
                    obs_date = d.date().isoformat()
                elif isinstance(d, datetime.date):
                    obs_date = d.isoformat()
                else:
                    s = str(d)
                    obs_date = s[:10]
                for tc in target_cols[:2]:
                    val = r[tc]
                    if pd.isna(val):
                        continue
                    # Guardamos como "INF_ALIM" si alimentos, sino como proxy macro (se puede expandir)
                    code = "INF_ALIM" if "alimento" in str(tc).lower() else "INF_ALIM"
                    # si viene en %, normalizamos
                    if val > 1:
                        val = float(val)/100.0
                    upsert_obs(code, obs_date, float(val), source="BCP")
                    imported += 1
    except Exception:
        pass

    return {"source":"bcp_ipc_excel","excel_url":xurl,"raw_file":raw_path,"imported":imported}


def run_once():
    fuentes = load_json(FUENTES_PATH, {})
    results=[]
    if fuentes.get("sedeco_datos_gov",{}).get("enabled"):
        try: results.append(fetch_sedeco_dataset(fuentes["sedeco_datos_gov"]))
        except Exception as e: results.append({"source":"sedeco","error":str(e)})
    if fuentes.get("petropar_combustibles",{}).get("enabled"):
        try: results.append(fetch_petropar_prices(fuentes["petropar_combustibles"]))
        except Exception as e: results.append({"source":"petropar","error":str(e)})

    if fuentes.get("bcp_ipc",{}).get("enabled"):
        try: results.append(fetch_bcp_ipc(fuentes["bcp_ipc"]))
        except Exception as e: results.append({"source":"bcp_ipc","error":str(e)})
        try: results.append(fetch_bcp_ipc_excel(fuentes["bcp_ipc"]))
        except Exception as e: results.append({"source":"bcp_ipc_excel","error":str(e)})

    return results


def run_scheduler():
    from apscheduler.schedulers.blocking import BlockingScheduler
    fuentes = load_json(FUENTES_PATH, {})
    sched = BlockingScheduler(timezone="America/Asuncion")

    def hhmm(t):
        hh,mm = t.split(":"); return int(hh), int(mm)

    if fuentes.get("petropar_combustibles",{}).get("enabled"):
        h,m = hhmm(fuentes["petropar_combustibles"].get("schedule","02:00"))
        sched.add_job(lambda: fetch_petropar_prices(fuentes["petropar_combustibles"]), "cron", hour=h, minute=m)
    if fuentes.get("sedeco_datos_gov",{}).get("enabled"):
        h,m = hhmm(fuentes["sedeco_datos_gov"].get("schedule","03:00"))
        sched.add_job(lambda: fetch_sedeco_dataset(fuentes["sedeco_datos_gov"]), "cron", hour=h, minute=m)
    if fuentes.get("bcp_ipc",{}).get("enabled"):
        h,m = hhmm(fuentes["bcp_ipc"].get("schedule","04:00"))
        sched.add_job(lambda: fetch_bcp_ipc(fuentes["bcp_ipc"]), "cron", hour=h, minute=m)

    print("Scheduler OMPP iniciado.")
    sched.start()

if __name__ == "__main__":
    import argparse
    ap=argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--daemon", action="store_true")
    args=ap.parse_args()
    if args.daemon:
        run_scheduler()
    else:
        print(json.dumps(run_once(), ensure_ascii=False, indent=2))
