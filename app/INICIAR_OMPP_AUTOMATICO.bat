

echo Starting OMPP Autonomous System

start cmd /k uvicorn main:app --reload

timeout 10

start cmd /k python servicio_ompp.py

