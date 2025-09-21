@echo off
rem Run from the project root so relative paths work
cd /d "C:\Users\Mame Diarra\Downloads\Igrac_Project"
".venv\Scripts\python.exe" scripts\compute_asi.py >> "logs\compute_asi.log" 2>&1
