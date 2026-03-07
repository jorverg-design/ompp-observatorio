import pandas as pd
from openpyxl import Workbook
from datetime import datetime

# ID DE TU GOOGLE SHEET
SHEET_ID = "PEGA_AQUI_TU_ID"

# URL para descargar
URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv"

# leer datos
df = pd.read_csv(URL)

# crear tabla pivot
pivot = df.pivot_table(
    index="fecha_semana",
    columns="producto",
    values="precio",
    aggfunc="mean"
)

pivot = pivot.reset_index()

# crear Excel formato Canasta_25
wb = Workbook()
ws = wb.active
ws.title = "Canasta_25"

productos = list(pivot.columns[1:])

# encabezados en fila 6
for i, prod in enumerate(productos):
    ws.cell(row=6, column=i+2).value = prod

# datos desde fila 8
for r, row in pivot.iterrows():
    ws.cell(row=8+r, column=1).value = row["fecha_semana"]

    for c, prod in enumerate(productos):
        ws.cell(row=8+r, column=2+c).value = row[prod]

file_name = f"data/auto_import/canasta_{datetime.now().date()}.xlsx"

wb.save(file_name)

print("Excel generado:", file_name)
