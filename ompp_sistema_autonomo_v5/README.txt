OMPP Sistema con Reporte Real (v1) — Español + Guaraníes
============================================

Incluye
- App web local (FastAPI): carga semanal + dashboard.
- Importación desde tu Excel (hoja Canasta_25).
- Reporte PDF estilo institucional (1 página).
- Endpoint /reporte/ppt_json (JSON listo para convertir a PPT).

Cómo ejecutar
1) Instalar Python 3.10+
2) En esta carpeta:
   pip install -r requirements.txt
   cd app
   uvicorn main:app --reload
3) Abrir: http://127.0.0.1:8000

Siguiente upgrade (si querés que convierta a PPT automáticamente)
- Agregamos el generador PPTX (pptxgenjs o python-pptx) en el endpoint /reporte/pptx.


Reporte Institucional Extendido (PDF)
- Endpoint: /reporte/pdf_ext?week_date=YYYY-MM-DD
- Incluye portada + resumen + tabla completa + movilidad + carne + metodología + anexos.


CONFIG (parametrizado)
- app/config/alertas.json: WhatsApp + umbrales (alertas ROJAS)
- app/config/fuentes.json: fuentes y horarios (conectores v1)
- app/config/sistema.json: nombre/moneda/frecuencia

WHATSAPP
- Por defecto: whatsapp.enabled=false
- Si enabled=true y test_mode=true -> registra mensajes en app/whatsapp_outbox.log


INGESTA AUTOMÁTICA (v2)
- Correr una vez:  python ingest.py --once
- Modo diario (scheduler): python ingest.py --daemon
- Evidencia: app/data/raw/ + tabla raw_source_files (hash + URL + fecha).
- Para SEDECO: ajustar app/config/mapping_sedeco.json con nombres exactos del dataset.


REPORTE REAL (v3)
- PDF con gráficos: /reporte/pdf_real?obs_date=YYYY-MM-DD
- PPTX automático: /reporte/pptx?obs_date=YYYY-MM-DD

BCP (v3)
- ingest.py intenta descubrir y descargar Excel (xls/xlsx) desde páginas configuradas en fuentes.json.
- Si cambia la estructura, igual guarda evidencia (raw) y se ajusta el parser.


NIVEL OBSERVATORIO (v4)
- Reporte Observatorio PDF: /reporte/observatorio_pdf?obs_date=YYYY-MM-DD
- Reporte Observatorio PPTX: /reporte/observatorio_pptx?obs_date=YYYY-MM-DD
- Página Ranking: /ranking
- Reportes incluyen: KPIs + ranking + series con MA y bandas + evidencia (hash) y eventos.
