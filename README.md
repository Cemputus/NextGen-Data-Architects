### NextGen-Data-Architects - NextGen MIS

A comprehensive data engineering and analytics platform for Uganda Christian University (UCU) with multi-role access control, advanced analytics, and machine learning predictions.

## 🏗️ Project Structure

```
project/
├── backend/              # Backend (Python/Flask)
│   ├── api/             # API endpoints
│   │   ├── auth.py      # Authentication
│   │   ├── analytics.py # Analytics endpoints
│   │   └── predictions.py # ML predictions
│   ├── ml_models.py     # ML models (RF, GB, NN)
│   ├── app.py           # Flask application
│   ├── config.py        # Configuration
│   ├── etl_pipeline.py  # ETL pipeline
│   ├── setup_databases.py # Database setup
│   ├── rbac.py          # Role-based access control
│   ├── models/          # ML model files
│   ├── sql/             # SQL scripts
│   └── data/            # Data files
├── frontend/            # Frontend (React/Chakra UI)
│   └── src/
│       ├── pages/       # Page components
│       ├── components/  # Reusable components
│       └── context/     # React context
└── README.md
```

## 🚀 Features

### 1. Multi-Role System (RBAC)
- **9 User Roles**: Senate, SysAdmin, Analyst, Student, Staff, Dean, HOD, HR, Finance
- **Role-based dashboards** and data access
- **Access Number authentication** for students (A#####/B#####)

### 2. Advanced Analytics
- **FEX Analytics**: Failed exam analysis with drilldown
- **High School Analytics**: 
  - Enrollment, retention, graduation rates
  - Performance vs Tuition completion correlation
  - Program distribution analysis
- **Global Filter Panel**: Filter by faculty, department, program, course, semester, high school, etc.

### 3. Machine Learning Predictions
- **3 ML Models**:
  - Random Forest Regressor
  - Gradient Boosting Regressor
  - Neural Network (MLPRegressor)
  - Ensemble (averages all models)
- **Scenario Analysis**: 8 predefined scenarios for what-if analysis
- **Role-based predictions**: Access control based on user role

### 4. Data Engineering
- **ETL Pipeline**: Bronze → Silver → Gold (Medallion Architecture)
- **Star Schema Data Warehouse**
- **Source Databases**: Academics (DB1) and Administration (DB2)

## 📋 Prerequisites

- Python 3.8+
- MySQL 8.0+
- Node.js 16+ (for frontend)
- Virtual environment (`backend/.venv`)

## 🔧 Setup

### 1. Backend Setup

```bash
# Navigate to backend folder
cd backend

# Create virtual environment
python -m venv .venv

# Activate virtual environment
# Windows:
.venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure database in config.py
# Update MySQL credentials

# Setup databases
python setup_databases.py

# Run ETL pipeline
python etl_pipeline.py

# Train ML models
python ml_models.py

# Start Flask server
python app.py
```

### 2. Frontend Setup

```bash
cd frontend

# Install dependencies
npm install

# Start development server
npm start
```

## 🔐 User Roles & Permissions

| Role | Access Level |
|------|-------------|
| **Student** | Own data only, predictions for self |
| **Staff** | Own classes, student data in classes |
| **HOD** | Department-wide analytics |
| **Dean** | Faculty-wide analytics |
| **Senate** | Institution-wide analytics (read-only) |
| **Analyst** | Create/modify analytics, scenario analysis |
| **SysAdmin** | Full system control |
| **HR** | HR analytics and staff management |
| **Finance** | Finance analytics and payments |

## 📊 Key Endpoints

### Authentication
- `POST /api/auth/login` - Login (supports Access Number for students)
- `POST /api/auth/refresh` - Refresh token
- `GET /api/auth/profile` - Get profile
- `PUT /api/auth/profile` - Update profile

### Analytics
- `GET /api/analytics/fex` - FEX analysis
- `GET /api/analytics/high-school` - High school analytics
- `GET /api/analytics/filter-options` - Get filter options

### Predictions
- `POST /api/predictions/predict` - Single prediction
- `POST /api/predictions/scenario` - Scenario analysis
- `POST /api/predictions/batch-predict` - Batch predictions
- `GET /api/predictions/scenarios` - Get scenario templates

## 🎯 Usage Examples

### Student Login
```json
POST /api/auth/login
{
  "identifier": "A12345",  // Access Number
  "password": "password"
}
```

### Predict Performance
```json
POST /api/predictions/predict
{
  "student_id": "S23B12/005",
  "model_type": "ensemble"
}
```

### Scenario Analysis (Analyst/SysAdmin/Senate only)
```json
POST /api/predictions/scenario
{
  "scenario": {
    "base_student_id": "S23B12/005",
    "attendance_rate": 90,
    "payment_completion_rate": 100
  }
}
```

## 📈 ML Models

The system uses three machine learning models:

1. **Random Forest**: 100 estimators, max_depth=15
2. **Gradient Boosting**: 100 estimators, learning_rate=0.1
3. **Neural Network**: Hidden layers (100, 50), early stopping

**Ensemble Model**: Averages predictions from all three models for better accuracy.

## 🔍 Scenario Analysis

8 predefined scenarios for what-if analysis:
1. High Attendance (90%+)
2. Low Attendance (50%)
3. Full Tuition Payment
4. Tuition Arrears
5. Increased Course Load
6. Reduced Course Load
7. Top Performer
8. At-Risk Student

## 📝 Notes

- Access Numbers format: `A#####` or `B#####` (5 digits)
- Registration Numbers format: `S23B12/005` (Intake+Year+Degree+Program/Student)
- UCU Grading: A (80-100), B+ (75-79), B (70-74), C (60-69), D (50-59), F (0-49)
- Coursework/Exam: 60%/40% (Law: 30%/70%)
- FCW Threshold: 35% (Law: 17.5%)

## 🛠️ Development

### Running Tests
```bash
# Backend tests
cd backend
python -m pytest

# Frontend tests
cd frontend
npm test
```

### Building for Production
```bash
# Frontend build
cd frontend
npm run build

# Backend (no build needed, just ensure dependencies installed)
```

## 📄 License

This project is for academic purposes.

## 👥 Contributors

NextGen-Data-Architects Team
