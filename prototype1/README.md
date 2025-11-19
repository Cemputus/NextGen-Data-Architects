# University Data Engineering System

A comprehensive data engineering system for Uganda Christian University with ETL pipeline, data warehouse, BI dashboard, and predictive analytics.

## Features

- **Two Source Databases**: Each with three tables containing university data
- **CSV Data Sources**: Two CSV files with additional university data
- **ETL Pipeline**: Python-based ETL with Medallion Architecture (Bronze, Silver, Gold layers)
- **Star Schema Data Warehouse**: Integrated data warehouse with fact and dimension tables
- **BI Dashboard**: Interactive dashboard with multiple chart types (bar, line, pie, donut)
- **Predictive Model**: ML model to predict student performance based on attendance and tuition payments
- **PDF Report Generator**: Download comprehensive analytics reports
- **User Authentication**: Secure login system with JWT tokens
- **Modern GUI**: React-based frontend with beautiful UI

## System Architecture

### Data Sources
1. **Database 1** (`data/source_db1.db`):
   - Students table
   - Courses table
   - Enrollments table

2. **Database 2** (`data/source_db2.db`):
   - Students table
   - Courses table
   - Attendance table

3. **CSV Files**:
   - `data/source_data1.csv`: Tuition payment data
   - `data/source_data2.csv`: Grades data

### ETL Pipeline (Medallion Architecture)
- **Bronze Layer**: Raw extracted data from all sources
- **Silver Layer**: Cleaned and transformed data
- **Gold Layer**: Star schema data warehouse

### Data Warehouse Schema
- **Dimension Tables**: dim_student, dim_course, dim_time, dim_semester
- **Fact Tables**: fact_enrollment, fact_attendance, fact_payment, fact_grade

## Installation

### Prerequisites
- Python 3.8+
- Node.js 16+
- npm or yarn
- **MySQL** (Community Server 5.7+ or 8.0+)

See [MYSQL_SETUP.md](MYSQL_SETUP.md) for detailed MySQL setup instructions.

### Backend Setup

1. **Configure MySQL** (see MYSQL_SETUP.md):
   - Update `config.py` with your MySQL credentials
   - Or set environment variables for MySQL connection

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. Generate source data:
```bash
python setup_databases.py
```

3. Run ETL pipeline:
```bash
python etl_pipeline.py
```

4. Train ML model:
```bash
python ml_model.py
```

5. Start Flask server:
```bash
python app.py
```

The backend will run on `http://localhost:5000`

### Frontend Setup

1. Navigate to frontend directory:
```bash
cd frontend
```

2. Install dependencies:
```bash
npm install
```

3. Start React development server:
```bash
npm start
```

The frontend will run on `http://localhost:3000`

## Usage

### Login Credentials
- **Admin**: username: `admin`, password: `admin123`
- **Analyst**: username: `analyst`, password: `analyst123`

### Dashboard Features

1. **Statistics Cards**: Overview of key metrics
2. **Bar Charts**: 
   - Students by department
   - Attendance by course
   - Top performing students
3. **Line Chart**: Average grades over time
4. **Pie Chart**: Payment status distribution
5. **Donut Chart**: Grade distribution
6. **Performance Predictor**: Enter a student ID to predict their performance

### PDF Report
Click the "Download Report" button in the dashboard header to generate and download a comprehensive PDF report.

## Project Structure

```
.
├── app.py                 # Flask backend API
├── config.py             # Configuration settings
├── setup_databases.py    # Generate source databases and CSV files
├── etl_pipeline.py       # ETL pipeline with medallion architecture
├── ml_model.py           # ML predictive model
├── pdf_generator.py      # PDF report generator
├── requirements.txt      # Python dependencies
├── data/                 # Data directory
│   ├── source_db1.db
│   ├── source_db2.db
│   ├── source_data1.csv
│   ├── source_data2.csv
│   ├── data_warehouse.db
│   ├── bronze/          # Bronze layer data
│   ├── silver/          # Silver layer data
│   └── gold/            # Gold layer data
├── models/              # ML model storage
├── reports/             # Generated PDF reports
└── frontend/            # React frontend
    ├── src/
    │   ├── components/
    │   │   ├── Login.js
    │   │   ├── Dashboard.js
    │   │   ├── StatsCards.js
    │   │   ├── Charts.js
    │   │   └── PredictionPanel.js
    │   └── context/
    │       └── AuthContext.js
    └── package.json
```

## Technologies Used

### Backend
- Flask: Web framework
- SQLAlchemy: Database ORM
- Pandas: Data processing
- scikit-learn: Machine learning
- ReportLab: PDF generation
- JWT: Authentication

### Frontend
- React: UI framework
- Recharts: Chart library
- Axios: HTTP client
- React Router: Routing
- Bootstrap: Styling

## Data Flow

1. **Extract**: Data extracted from 2 databases and 2 CSV files
2. **Transform**: Data cleaned, deduplicated, and standardized
3. **Load**: Data loaded into star schema data warehouse
4. **Analyze**: Dashboard queries data warehouse for visualizations
5. **Predict**: ML model uses attendance and payment data to predict performance

## API Endpoints

- `POST /api/login` - User authentication
- `GET /api/dashboard/stats` - Dashboard statistics
- `GET /api/dashboard/students-by-department` - Department distribution
- `GET /api/dashboard/grades-over-time` - Grade trends
- `GET /api/dashboard/payment-status` - Payment status
- `GET /api/dashboard/attendance-by-course` - Course attendance
- `GET /api/dashboard/grade-distribution` - Grade distribution
- `GET /api/dashboard/top-students` - Top performers
- `POST /api/dashboard/predict-performance` - Predict student performance
- `POST /api/report/generate` - Generate PDF report

## License

This project is for educational purposes.

## Author

Data Engineering System for Uganda Christian University

