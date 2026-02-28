

import os
import time
import datetime
import subprocess
import sys
import requests

BASE=os.path.dirname(__file__)

REPORT_DIR=os.path.join(BASE,"reportes")

os.makedirs(REPORT_DIR,exist_ok=True)

def log(msg):
    print(datetime.datetime.now().isoformat(),msg)

def run_ingest():
    log("Running ingest")
    subprocess.run([sys.executable,"ingest.py","--once"])

def generate_reports():
    today=datetime.date.today().isoformat()

    try:

        pdf=requests.get(f"http://127.0.0.1:8000/reporte/observatorio_pdf?obs_date={today}")
        ppt=requests.get(f"http://127.0.0.1:8000/reporte/observatorio_pptx?obs_date={today}")

        open(os.path.join(REPORT_DIR,f"Reporte_{today}.pdf"),"wb").write(pdf.content)
        open(os.path.join(REPORT_DIR,f"Reporte_{today}.pptx"),"wb").write(ppt.content)

        log("Reports saved")

    except Exception as e:

        log("Report error "+str(e))

def run_cycle():

    run_ingest()

    generate_reports()

def scheduler():

    last_day=None

    while True:

        now=datetime.datetime.now()

        if last_day!=now.date() and now.hour>=5:

            log("Daily cycle")

            run_cycle()

            last_day=now.date()

        time.sleep(60)

if __name__=="__main__":

    log("OMPP Autonomous Service Started")

    scheduler()

