# Quick Start Guide

## Prerequisites Check
- Python 3.8+ installed
- Node.js 16+ installed
- npm or yarn installed

## Step 1: Install Backend Dependencies

```bash
pip install -r requirements.txt
```

## Step 2: Generate Source Data

```bash
python setup_databases.py
```

This will create:
- `data/source_db1.db` (Students, Courses, Enrollments)
- `data/source_db2.db` (Students, Courses, Attendance)
- `data/source_data1.csv` (Tuition Payments)
- `data/source_data2.csv` (Grades)

## Step 3: Run ETL Pipeline

```bash
python etl_pipeline.py
```

This will:
- Extract data to Bronze layer
- Transform data to Silver layer
- Load data to Gold layer (Data Warehouse)

## Step 4: Train ML Model

```bash
python ml_model.py
```

This creates the predictive model for student performance.

## Step 5: Start Backend Server

```bash
python app.py
```

Backend runs on `http://localhost:5000`

## Step 6: Start Frontend (New Terminal)

```bash
cd frontend
npm install
npm start
```

Frontend runs on `http://localhost:3000`

## Login Credentials

- **Admin**: username: `admin`, password: `admin123`
- **Analyst**: username: `analyst`, password: `analyst123`

## Using the Dashboard

1. Login with credentials above
2. View statistics cards at the top
3. Explore various charts:
   - Bar charts (Students by Department, Attendance, Top Students)
   - Line chart (Grades Over Time)
   - Pie chart (Payment Status)
   - Donut chart (Grade Distribution)
4. Use Performance Predictor:
   - Enter a student ID (e.g., STU000001)
   - Click "Predict Performance"
5. Download PDF Report:
   - Click "Download Report" button in header

## Troubleshooting

### Port 5000 already in use
- Change port in `app.py` or stop the process using port 5000

### Module not found errors
- Ensure all Python packages are installed: `pip install -r requirements.txt`
- Ensure all Node packages are installed: `cd frontend && npm install`

### Database errors
- Delete existing databases and re-run `setup_databases.py` and `etl_pipeline.py`

### ML Model errors
- Re-run `python ml_model.py` to train a new model

## Windows Users

You can use the batch file:
```bash
start_backend.bat
```

## Linux/Mac Users

You can use the shell script:
```bash
chmod +x start_backend.sh
./start_backend.sh
```




