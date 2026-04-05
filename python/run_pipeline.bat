@echo off
REM This file runs the Consumer360 pipeline
REM Save this as: run_pipeline.bat in your project folder

REM Navigate to project directory
cd /d C:\Users\omkar\Downloads\Consumer360-RFM-Analytics

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Run the pipeline
python python\main_pipeline.py

REM Log completion
echo %date% %time% >> logs\schedule_runs.log
