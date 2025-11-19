@echo off
echo Starting University Data Engineering System Backend...
echo.

echo Step 1: Generating source data...
python setup_databases.py
echo.

echo Step 2: Running ETL pipeline...
python etl_pipeline.py
echo.

echo Step 3: Training ML model...
python ml_model.py
echo.

echo Step 4: Starting Flask server...
python app.py




