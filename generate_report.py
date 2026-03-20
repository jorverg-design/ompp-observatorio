import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
DATA_FILE = BASE_DIR / "Canasta_25.xlsx"
OUTPUT_JSON = BASE_DIR / "reporte_diario.json"
OUTPUT_TXT = BASE_DIR / "reporte_diario.txt"


def norm_text(value: object) -> str:
    return str(value or "").strip().lower()


def find_column(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    normalized = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in normalized:
            return normalized[cand.lower()]
    return None


def pick_sheet(xls: pd.ExcelFile) -> str:
    preferred = ["Canasta_25", "Carga_Semanal", "Hoja1", "Sheet1"]
    for name in preferred:
        if name in xls.sheet_names:
            return name
    return xls.sheet_names[0]


def load_data() -> pd.DataFrame:
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"No se encontró {DATA_FILE.name}")

    xls = pd.ExcelFile(DATA_FILE)
    sheet = pick_sheet(xls)
    df = pd.read_excel(DATA_FILE, sheet_name=sheet)

    if df.empty:
        raise ValueError("La hoja está vacía")

    week_col = find_column(df, ["semana", "week"])
    city_col = find_column(df, ["ciudad", "city"])
    product_col = find_column(df, ["producto", "product"])
    price_col = find_column(df, ["precio", "price"])
    channel_col = find_column(df, ["canal", "channel"])
    datetime_col = find_column(df, ["fecha_hora", "datetime", "date_time", "fecha"])

    missing = [name for name, col in {
        "semana": week_col,
        "ciudad": city_col,
        "producto": product_col,
        "precio": price_col,
    }.items() if col is None]

    if missing:
        raise ValueError(f"Faltan columnas obligatorias: {', '.join(missing)}")

    work = pd.DataFrame({
        "week": df[week_col],
        "city": df[city_col],
        "product": df[product_col],
        "price": pd.to_numeric(df[price_col], errors="coerce"),
    })

    work["channel"] = df[channel_col] if channel_col else "General"
    work["datetime"] = pd.to_datetime(df[datetime_col], errors="coerce") if datetime_col else pd.NaT

    work = work.dropna(subset=["week", "city", "product", "price"]).copy()
    work["week"] = work["week"].astype(str).str.strip()
    work["city"] = work["city"].astype(str).str.strip()
    work["product"] = work["product"].astype(str).str.strip()
    work["channel"] = work["channel"].astype(str).str.strip()

    return work


def week_sort_key(week: str) -> int:
    try:
        year, w = week.split("-W")
        return int(year) * 100 + int(w)
    except Exception:
        return -1


def latest_weeks(df: pd.DataFrame) -> tuple[Optional[str], Optional[str]]:
    weeks = sorted(df["week"].dropna().unique().tolist(), key=week_sort_key)
    if not weeks:
        return None, None
    latest = weeks[-1]
    prev = weeks[-2] if len(weeks) > 1 else None
    return latest, prev


def avg_price(df: pd.DataFrame, week: str, city: Optional[str] = None, product: Optional[str] = None, channel: Optional[str] = None) -> Optional[float]:
    q = df[df["week"] == week]
    if city:
        q = q[q["city"] == city]
    if product:
        q = q[q["product"].str.lower() == product.lower()]
    if channel:
        q = q[q["channel"].str.lower() == channel.lower()]
    if q.empty:
        return None
    return float(q["price"].mean())


def variation(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    if current is None or previous is None or previous == 0:
        return None
    return ((current / previous) - 1.0) * 100.0


def tomato_metrics(df: pd.DataFrame, latest: str, prev: Optional[str]) -> dict:
    mayorista = avg_price(df, latest, product="Tomate", channel="Mayorista")
    minorista = avg_price(df, latest, product="Tomate", channel="Minorista")
    finca = avg_price(df, latest, product="Tomate", channel="Finca")

    mayorista_prev = avg_price(df, prev, product="Tomate", channel="Mayorista") if prev else None

    spread = None
    if minorista is not None and finca is not None and finca != 0:
        spread = ((minorista / finca) - 1.0) * 100.0

    mayorista_var = variation(mayorista, mayorista_prev)

    alerta = "SIN BASE"
    if mayorista_var is not None:
      if mayorista_var >= 10:
          alerta = "ALTA"
      elif mayorista_var >= 5:
          alerta = "MEDIA"
      else:
          alerta = "NORMAL"

    city_rows = []
    for city in sorted(df["city"].unique()):
        cur = avg_price(df, latest, city=city, product="Tomate", channel="Mayorista")
        if cur is None:
            continue
        prv = avg_price(df, prev, city=city, product="Tomate", channel="Mayorista") if prev else None
        city_rows.append({
            "city": city,
            "current": cur,
            "variation": variation(cur, prv)
        })

    city_rows.sort(key=lambda x: -999999 if x["variation"] is None else -x["variation"])
    ciudad_critica = city_rows[0]["city"] if city_rows else None

    return {
        "mayorista": round(mayorista, 2) if mayorista is not None else None,
        "minorista": round(minorista, 2) if minorista is not None else None,
        "finca": round(finca, 2) if finca is not None else None,
        "variacion_mayorista_semanal": round(mayorista_var, 2) if mayorista_var is not None else None,
        "brecha_pct": round(spread, 2) if spread is not None else None,
        "ciudad_critica": ciudad_critica,
        "alerta": alerta,
    }


def canasta_metrics(df: pd.DataFrame, latest: str, prev: Optional[str]) -> dict:
    cur = avg_price(df, latest)
    prv = avg_price(df, prev) if prev else None
    var = variation(cur, prv)

    products = []
    for product in sorted(df["product"].unique()):
        pcur = avg_price(df, latest, product=product)
        if pcur is None:
            continue
        pprv = avg_price(df, prev, product=product) if prev else None
        products.append({
            "product": product,
            "variation": variation(pcur, pprv)
        })

    products = [p for p in products if p["variation"] is not None]
    products.sort(key=lambda x: -x["variation"])
    producto_mas_presionado = products[0]["product"] if products else None

    city_rows = []
    for city in sorted(df["city"].unique()):
        cur_city = avg_price(df, latest, city=city)
        if cur_city is None:
            continue
        prev_city = avg_price(df, prev, city=city) if prev else None
        city_rows.append({
            "city": city,
            "variation": variation(cur_city, prev_city)
        })
    city_rows = [c for c in city_rows if c["variation"] is not None]
    city_rows.sort(key=lambda x: -x["variation"])
    ciudad_critica = city_rows[0]["city"] if city_rows else None

    return {
        "promedio_actual": round(cur, 2) if cur is not None else None,
        "variacion_semanal": round(var, 2) if var is not None else None,
        "producto_mas_presionado": producto_mas_presionado,
        "ciudad_critica": ciudad_critica,
    }


def build_summary(fecha: str, canasta: dict, tomate: dict) -> str:
    parts = [
        f"Informe diario de precios – {fecha}.",
        f"Canasta_25: promedio {canasta['promedio_actual'] if canasta['promedio_actual'] is not None else 's/d'}",
        f"variación semanal {str(canasta['variacion_semanal']) + '%' if canasta['variacion_semanal'] is not None else 's/d'}."
    ]

    if canasta["producto_mas_presionado"]:
        parts.append(f"Producto más presionado: {canasta['producto_mas_presionado']}.")

    if tomate["mayorista"] is not None:
        parts.append(f"Tomate mayorista: Gs. {int(round(tomate['mayorista'])):,}.".replace(",", "."))

    if tomate["minorista"] is not None:
        parts.append(f"Tomate minorista: Gs. {int(round(tomate['minorista'])):,}.".replace(",", "."))

    if tomate["finca"] is not None:
        parts.append(f"Tomate finca: Gs. {int(round(tomate['finca'])):,}.".replace(",", "."))

    if tomate["brecha_pct"] is not None:
        parts.append(f"Brecha finca-minorista: {round(tomate['brecha_pct'], 1)}%.")

    if tomate["ciudad_critica"]:
        parts.append(f"Ciudad crítica del tomate: {tomate['ciudad_critica']}.")

    parts.append(f"Alerta tomate: {tomate['alerta']}.")

    return " ".join(parts)


def main() -> None:
    df = load_data()
    latest, prev = latest_weeks(df)
    if latest is None:
        raise ValueError("No hay semanas válidas en la base")

    now = datetime.now()
    fecha = now.strftime("%Y-%m-%d")
    hora = now.strftime("%H:%M")

    canasta = canasta_metrics(df, latest, prev)
    tomate = tomato_metrics(df, latest, prev)
    resumen = build_summary(fecha, canasta, tomate)

    output = {
        "fecha": fecha,
        "hora": hora,
        "semana_actual": latest,
        "semana_previa": prev,
        "canasta25": canasta,
        "tomate": tomate,
        "resumen": resumen,
    }

    OUTPUT_JSON.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    OUTPUT_TXT.write_text(resumen, encoding="utf-8")
    print("Reporte generado:", OUTPUT_JSON.name, OUTPUT_TXT.name)


if __name__ == "__main__":
    main()
