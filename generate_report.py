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


def week_sort_key(week: str) -> int:
    try:
        year, w = week.split("-W")
        return int(year) * 100 + int(w)
    except Exception:
        return -1


def parse_week(value: object) -> Optional[str]:
    text = str(value or "").strip()

    if not text:
        return None

    if "-W" in text:
        return text

    # fechas tipo 20/03/2026 o 2026-03-20
    dt = pd.to_datetime(text, errors="coerce", dayfirst=True)
    if pd.isna(dt):
        return None

    iso = dt.isocalendar()
    return f"{int(iso.year)}-W{int(iso.week):02d}"


def load_canasta25_sheet(xls: pd.ExcelFile) -> Optional[pd.DataFrame]:
    if "Canasta_25" not in xls.sheet_names:
        return None

    df = pd.read_excel(DATA_FILE, sheet_name="Canasta_25")
    if df.empty:
        return None

    week_col = find_column(df, ["semana", "week"])
    city_col = find_column(df, ["ciudad", "city"])
    product_col = find_column(df, ["producto", "product"])
    price_col = find_column(df, ["precio", "price"])
    channel_col = find_column(df, ["canal", "channel"])
    datetime_col = find_column(df, ["fecha_hora", "datetime", "date_time", "fecha"])

    if not all([week_col, city_col, product_col, price_col]):
        return None

    work = pd.DataFrame({
        "week": df[week_col].astype(str).str.strip(),
        "city": df[city_col].astype(str).str.strip(),
        "product": df[product_col].astype(str).str.strip(),
        "price": pd.to_numeric(df[price_col], errors="coerce"),
    })

    work["channel"] = df[channel_col].astype(str).str.strip() if channel_col else "General"
    work["datetime"] = pd.to_datetime(df[datetime_col], errors="coerce") if datetime_col else pd.NaT

    work = work.dropna(subset=["week", "city", "product", "price"]).copy()
    work = work[work["week"] != ""]
    work = work[work["city"] != ""]
    work = work[work["product"] != ""]
    return work


def load_carga_semanal_sheet(xls: pd.ExcelFile) -> Optional[pd.DataFrame]:
    if "Carga_Semanal" not in xls.sheet_names:
        return None

    raw = pd.read_excel(DATA_FILE, sheet_name="Carga_Semanal", header=None)
    if raw.empty:
        return None

    header_row_idx = None
    date_col_idx = None

    for idx in range(min(15, len(raw))):
        row_vals = [norm_text(v) for v in raw.iloc[idx].tolist()]
        for j, val in enumerate(row_vals):
            if val in {"fecha_semana", "fecha", "semana", "week"}:
                header_row_idx = idx
                date_col_idx = j
                break
        if header_row_idx is not None:
            break

    if header_row_idx is None:
        return None

    headers = raw.iloc[header_row_idx].tolist()
    data = raw.iloc[header_row_idx + 1:].copy()
    data.columns = headers
    data = data.dropna(how="all")

    if data.empty:
        return None

    date_col = data.columns[date_col_idx]
    product_cols = [c for c in data.columns if c != date_col and str(c).strip() != ""]

    rows = []
    default_city = "Asunción"

    for _, r in data.iterrows():
        week = parse_week(r[date_col])
        if not week:
            continue

        for product in product_cols:
            price = pd.to_numeric(r[product], errors="coerce")
            if pd.isna(price):
                continue

            product_name = str(product).strip()
            channel = "General"

            if norm_text(product_name) == "tomate":
                channel = "Minorista"

            rows.append({
                "week": week,
                "city": default_city,
                "product": product_name,
                "price": float(price),
                "channel": channel,
                "datetime": pd.NaT,
            })

    if not rows:
        return None

    return pd.DataFrame(rows)


def load_data() -> pd.DataFrame:
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"No se encontró {DATA_FILE.name}")

    xls = pd.ExcelFile(DATA_FILE)

    df = load_canasta25_sheet(xls)
    if df is not None and not df.empty:
        return df

    df = load_carga_semanal_sheet(xls)
    if df is not None and not df.empty:
        return df

    raise ValueError(
        "No se encontró una hoja utilizable. Usa 'Canasta_25' en formato largo "
        "o 'Carga_Semanal' con fecha_semana y productos en columnas."
    )


def latest_weeks(df: pd.DataFrame) -> tuple[Optional[str], Optional[str]]:
    weeks = sorted(df["week"].dropna().unique().tolist(), key=week_sort_key)
    if not weeks:
        return None, None
    latest = weeks[-1]
    prev = weeks[-2] if len(weeks) > 1 else None
    return latest, prev


def avg_price(
    df: pd.DataFrame,
    week: str,
    city: Optional[str] = None,
    product: Optional[str] = None,
    channel: Optional[str] = None,
) -> Optional[float]:
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
    mayorista_var = variation(mayorista, mayorista_prev)

    spread = None
    if minorista is not None and finca is not None and finca != 0:
        spread = ((minorista / finca) - 1.0) * 100.0

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
        city_rows.append({"city": city, "variation": variation(cur, prv)})

    city_rows = [c for c in city_rows if c["variation"] is not None]
    city_rows.sort(key=lambda x: -x["variation"])
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
        v = variation(pcur, pprv)
        if v is not None:
            products.append({"product": product, "variation": v})

    products.sort(key=lambda x: -x["variation"])
    producto_mas_presionado = products[0]["product"] if products else None

    city_rows = []
    for city in sorted(df["city"].unique()):
        cur_city = avg_price(df, latest, city=city)
        if cur_city is None:
            continue
        prev_city = avg_price(df, prev, city=city) if prev else None
        v = variation(cur_city, prev_city)
        if v is not None:
            city_rows.append({"city": city, "variation": v})

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
