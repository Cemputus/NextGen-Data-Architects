"""
ETL Pipeline with Medallion Architecture (Bronze, Silver, Gold)
Uses MySQL
"""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text
import pymysql
from config import (
    DB1_CONN_STRING, DB2_CONN_STRING, CSV1_PATH, CSV2_PATH,
    BRONZE_PATH, SILVER_PATH, GOLD_PATH,
    DATA_WAREHOUSE_NAME, DATA_WAREHOUSE_CONN_STRING,
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD
)

class ETLPipeline:
    def __init__(self):
        self.bronze_path = BRONZE_PATH
        self.silver_path = SILVER_PATH
        self.gold_path = GOLD_PATH
        self.dw_name = DATA_WAREHOUSE_NAME
        
    def create_data_warehouse(self):
        """Create data warehouse database if it doesn't exist"""
        try:
            # Connect to MySQL server (without specifying database)
            conn = pymysql.connect(
                host=MYSQL_HOST,
                port=int(MYSQL_PORT),
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                charset='utf8mb4'
            )
            cursor = conn.cursor()
            
            # Check if database exists
            cursor.execute(f"SHOW DATABASES LIKE '{self.dw_name}'")
            if cursor.fetchone() is None:
                cursor.execute(f"CREATE DATABASE `{self.dw_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
                print(f"Data warehouse database {self.dw_name} created successfully")
            else:
                print(f"Data warehouse database {self.dw_name} already exists")
            
            conn.close()
        except Exception as e:
            print(f"Error creating data warehouse: {e}")
            raise
        
    def extract(self):
        """Extract data from all sources (Bronze Layer)"""
        print("Extracting data to Bronze layer...")
        
        # Extract from Database 1
        engine1 = create_engine(DB1_CONN_STRING)
        students_db1 = pd.read_sql_query("SELECT * FROM students", engine1)
        courses_db1 = pd.read_sql_query("SELECT * FROM courses", engine1)
        enrollments_db1 = pd.read_sql_query("SELECT * FROM enrollments", engine1)
        engine1.dispose()
        
        # Extract from Database 2
        engine2 = create_engine(DB2_CONN_STRING)
        students_db2 = pd.read_sql_query("SELECT * FROM students", engine2)
        courses_db2 = pd.read_sql_query("SELECT * FROM courses", engine2)
        attendance_db2 = pd.read_sql_query("SELECT * FROM attendance", engine2)
        engine2.dispose()
        
        # Extract from CSV files
        payments_csv = pd.read_csv(CSV1_PATH)
        grades_csv = pd.read_csv(CSV2_PATH)
        
        # Save to Bronze layer (raw data)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        students_db1.to_parquet(self.bronze_path / f"bronze_students_db1_{timestamp}.parquet", index=False)
        courses_db1.to_parquet(self.bronze_path / f"bronze_courses_db1_{timestamp}.parquet", index=False)
        enrollments_db1.to_parquet(self.bronze_path / f"bronze_enrollments_db1_{timestamp}.parquet", index=False)
        
        students_db2.to_parquet(self.bronze_path / f"bronze_students_db2_{timestamp}.parquet", index=False)
        courses_db2.to_parquet(self.bronze_path / f"bronze_courses_db2_{timestamp}.parquet", index=False)
        attendance_db2.to_parquet(self.bronze_path / f"bronze_attendance_db2_{timestamp}.parquet", index=False)
        
        payments_csv.to_parquet(self.bronze_path / f"bronze_payments_{timestamp}.parquet", index=False)
        grades_csv.to_parquet(self.bronze_path / f"bronze_grades_{timestamp}.parquet", index=False)
        
        print("Bronze layer extraction complete!")
        return {
            'students_db1': students_db1,
            'courses_db1': courses_db1,
            'enrollments_db1': enrollments_db1,
            'students_db2': students_db2,
            'courses_db2': courses_db2,
            'attendance_db2': attendance_db2,
            'payments': payments_csv,
            'grades': grades_csv
        }
    
    def transform(self, bronze_data):
        """Transform and clean data (Silver Layer)"""
        print("Transforming data to Silver layer...")
        
        # Merge students from both databases (deduplicate)
        students_combined = pd.concat([bronze_data['students_db1'], bronze_data['students_db2']])
        students_silver = students_combined.drop_duplicates(subset=['student_id'], keep='first')
        students_silver = students_silver.fillna('')
        students_silver['email'] = students_silver['email'].str.lower().str.strip()
        
        # Merge courses from both databases
        courses_combined = pd.concat([bronze_data['courses_db1'], bronze_data['courses_db2']])
        courses_silver = courses_combined.drop_duplicates(subset=['course_code'], keep='first')
        courses_silver = courses_silver.fillna('')
        
        # Clean enrollments
        enrollments_silver = bronze_data['enrollments_db1'].copy()
        enrollments_silver = enrollments_silver.fillna('')
        enrollments_silver['enrollment_date'] = pd.to_datetime(enrollments_silver['enrollment_date'], errors='coerce')
        
        # Clean attendance
        attendance_silver = bronze_data['attendance_db2'].copy()
        attendance_silver = attendance_silver.fillna('')
        attendance_silver['attendance_date'] = pd.to_datetime(attendance_silver['attendance_date'], errors='coerce')
        attendance_silver['hours_attended'] = pd.to_numeric(attendance_silver['hours_attended'], errors='coerce').fillna(0)
        
        # Clean payments
        payments_silver = bronze_data['payments'].copy()
        payments_silver = payments_silver.fillna('')
        payments_silver['payment_date'] = pd.to_datetime(payments_silver['payment_date'], errors='coerce')
        payments_silver['amount'] = pd.to_numeric(payments_silver['amount'], errors='coerce').fillna(0)
        
        # Clean grades
        grades_silver = bronze_data['grades'].copy()
        grades_silver = grades_silver.fillna('')
        grades_silver['grade'] = pd.to_numeric(grades_silver['grade'], errors='coerce').fillna(0)
        grades_silver['exam_date'] = pd.to_datetime(grades_silver['exam_date'], errors='coerce')
        
        # Save to Silver layer
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        students_silver.to_parquet(self.silver_path / f"silver_students_{timestamp}.parquet", index=False)
        courses_silver.to_parquet(self.silver_path / f"silver_courses_{timestamp}.parquet", index=False)
        enrollments_silver.to_parquet(self.silver_path / f"silver_enrollments_{timestamp}.parquet", index=False)
        attendance_silver.to_parquet(self.silver_path / f"silver_attendance_{timestamp}.parquet", index=False)
        payments_silver.to_parquet(self.silver_path / f"silver_payments_{timestamp}.parquet", index=False)
        grades_silver.to_parquet(self.silver_path / f"silver_grades_{timestamp}.parquet", index=False)
        
        print("Silver layer transformation complete!")
        return {
            'students': students_silver,
            'courses': courses_silver,
            'enrollments': enrollments_silver,
            'attendance': attendance_silver,
            'payments': payments_silver,
            'grades': grades_silver
        }
    
    def load_to_warehouse(self, silver_data):
        """Load transformed data into star schema data warehouse (Gold Layer)"""
        print("Loading data to Gold layer (Data Warehouse)...")
        
        # Create data warehouse if it doesn't exist
        self.create_data_warehouse()
        
        engine = create_engine(DATA_WAREHOUSE_CONN_STRING)
        
        # Create dimension tables
        self._create_dimensions(engine, silver_data)
        
        # Populate time dimension before facts (facts reference dim_time)
        self._populate_time_dimension(engine)
        
        # Create fact tables
        self._create_facts(engine, silver_data)
        
        engine.dispose()
        print("Gold layer (Data Warehouse) loading complete!")
    
    def _create_dimensions(self, engine, silver_data):
        """Create dimension tables for star schema"""
        
        with engine.connect() as conn:
            # Temporarily disable FK checks so we can drop and recreate tables safely
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))

            # Drop fact tables first (they reference dimensions)
            conn.execute(text("DROP TABLE IF EXISTS fact_grade"))
            conn.execute(text("DROP TABLE IF EXISTS fact_payment"))
            conn.execute(text("DROP TABLE IF EXISTS fact_attendance"))
            conn.execute(text("DROP TABLE IF EXISTS fact_enrollment"))

            # Dim_Student
            conn.execute(text("DROP TABLE IF EXISTS dim_student"))
            conn.execute(text("""
                CREATE TABLE dim_student (
                    student_id VARCHAR(20) PRIMARY KEY,
                    first_name VARCHAR(50),
                    last_name VARCHAR(50),
                    email VARCHAR(100),
                    gender CHAR(1),
                    nationality VARCHAR(50),
                    admission_date DATE,
                    INDEX idx_name (last_name, first_name),
                    INDEX idx_email (email)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
            
            # Dim_Course
            conn.execute(text("DROP TABLE IF EXISTS dim_course"))
            conn.execute(text("""
                CREATE TABLE dim_course (
                    course_code VARCHAR(20) PRIMARY KEY,
                    course_name VARCHAR(100),
                    credits INT,
                    department VARCHAR(50),
                    INDEX idx_department (department)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
            
            # Dim_Time
            conn.execute(text("DROP TABLE IF EXISTS dim_time"))
            conn.execute(text("""
                CREATE TABLE dim_time (
                    date_key VARCHAR(8) PRIMARY KEY,
                    date DATE,
                    year INT,
                    quarter INT,
                    month INT,
                    month_name VARCHAR(20),
                    day INT,
                    day_of_week INT,
                    day_name VARCHAR(20),
                    is_weekend BOOLEAN,
                    INDEX idx_date (date),
                    INDEX idx_year_month (year, month)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
            
            # Dim_Semester
            conn.execute(text("DROP TABLE IF EXISTS dim_semester"))
            conn.execute(text("""
                CREATE TABLE dim_semester (
                    semester_id INT PRIMARY KEY,
                    semester_name VARCHAR(50),
                    academic_year VARCHAR(20),
                    INDEX idx_academic_year (academic_year)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))

            # Re‑enable foreign key checks
            conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
            conn.commit()
        
        # Dim_Student
        students_dim = silver_data['students'][['student_id', 'first_name', 'last_name', 
                                                'email', 'gender', 'nationality', 'admission_date']].copy()
        students_dim.columns = ['student_id', 'first_name', 'last_name', 'email', 
                               'gender', 'nationality', 'admission_date']
        students_dim.to_sql('dim_student', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        
        # Dim_Course
        courses_dim = silver_data['courses'][['course_code', 'course_name', 'credits', 'department']].copy()
        courses_dim.columns = ['course_code', 'course_name', 'credits', 'department']
        courses_dim.to_sql('dim_course', engine, if_exists='append', index=False)
        
        # Dim_Semester
        semesters = pd.DataFrame({
            'semester_id': [1, 2, 3, 4],
            'semester_name': ['Fall 2023', 'Spring 2024', 'Fall 2024', 'Spring 2025'],
            'academic_year': ['2023-2024', '2023-2024', '2024-2025', '2024-2025']
        })
        semesters.to_sql('dim_semester', engine, if_exists='append', index=False)
        
    def _populate_time_dimension(self, engine):
        """Populate time dimension table"""
        print("Populating time dimension...")
        
        # Generate dates from 2023-01-01 to 2025-12-31
        dates = pd.date_range(start='2023-01-01', end='2025-12-31', freq='D')
        time_dim = pd.DataFrame({
            'date_key': [d.strftime('%Y%m%d') for d in dates],
            'date': dates,
            'year': dates.year,
            'quarter': dates.quarter,
            'month': dates.month,
            'month_name': dates.strftime('%B'),
            'day': dates.day,
            'day_of_week': dates.dayofweek,
            'day_name': dates.strftime('%A'),
            'is_weekend': dates.dayofweek >= 5
        })
        
        time_dim.to_sql('dim_time', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        print("Time dimension populated!")
        
    def _create_time_dimension(self):
        """Create time dimension table (helper method)"""
        dates = pd.date_range(start='2023-01-01', end='2025-12-31', freq='D')
        time_dim = pd.DataFrame({
            'date_key': [d.strftime('%Y%m%d') for d in dates],
            'date': dates,
            'year': dates.year,
            'quarter': dates.quarter,
            'month': dates.month,
            'month_name': dates.strftime('%B'),
            'day': dates.day,
            'day_of_week': dates.dayofweek,
            'day_name': dates.strftime('%A'),
            'is_weekend': dates.dayofweek >= 5
        })
        return time_dim
    
    def _create_facts(self, engine, silver_data):
        """Create fact tables for star schema"""
        
        with engine.connect() as conn:
            # Fact_Enrollment
            conn.execute(text("DROP TABLE IF EXISTS fact_enrollment"))
            conn.execute(text("""
                CREATE TABLE fact_enrollment (
                    enrollment_id VARCHAR(20) PRIMARY KEY,
                    student_id VARCHAR(20),
                    course_code VARCHAR(20),
                    date_key VARCHAR(8),
                    semester_id INT,
                    status VARCHAR(20),
                    FOREIGN KEY (student_id) REFERENCES dim_student(student_id) ON DELETE CASCADE,
                    FOREIGN KEY (course_code) REFERENCES dim_course(course_code) ON DELETE CASCADE,
                    FOREIGN KEY (date_key) REFERENCES dim_time(date_key) ON DELETE CASCADE,
                    FOREIGN KEY (semester_id) REFERENCES dim_semester(semester_id) ON DELETE CASCADE,
                    INDEX idx_student (student_id),
                    INDEX idx_course (course_code),
                    INDEX idx_date (date_key),
                    INDEX idx_semester (semester_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
            
            # Fact_Attendance
            conn.execute(text("DROP TABLE IF EXISTS fact_attendance"))
            conn.execute(text("""
                CREATE TABLE fact_attendance (
                    attendance_id INT AUTO_INCREMENT PRIMARY KEY,
                    student_id VARCHAR(20),
                    course_code VARCHAR(20),
                    date_key VARCHAR(8),
                    total_hours DECIMAL(10,2),
                    days_present INT,
                    FOREIGN KEY (student_id) REFERENCES dim_student(student_id) ON DELETE CASCADE,
                    FOREIGN KEY (course_code) REFERENCES dim_course(course_code) ON DELETE CASCADE,
                    FOREIGN KEY (date_key) REFERENCES dim_time(date_key) ON DELETE CASCADE,
                    INDEX idx_student (student_id),
                    INDEX idx_course (course_code),
                    INDEX idx_date (date_key)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
            
            # Fact_Payment
            conn.execute(text("DROP TABLE IF EXISTS fact_payment"))
            conn.execute(text("""
                CREATE TABLE fact_payment (
                    payment_id VARCHAR(20) PRIMARY KEY,
                    student_id VARCHAR(20),
                    date_key VARCHAR(8),
                    semester_id INT,
                    amount DECIMAL(15,2),
                    payment_method VARCHAR(50),
                    status VARCHAR(20),
                    FOREIGN KEY (student_id) REFERENCES dim_student(student_id) ON DELETE CASCADE,
                    FOREIGN KEY (date_key) REFERENCES dim_time(date_key) ON DELETE CASCADE,
                    FOREIGN KEY (semester_id) REFERENCES dim_semester(semester_id) ON DELETE CASCADE,
                    INDEX idx_student (student_id),
                    INDEX idx_date (date_key),
                    INDEX idx_semester (semester_id),
                    INDEX idx_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
            
            # Fact_Grade
            conn.execute(text("DROP TABLE IF EXISTS fact_grade"))
            conn.execute(text("""
                CREATE TABLE fact_grade (
                    grade_id VARCHAR(20) PRIMARY KEY,
                    student_id VARCHAR(20),
                    course_code VARCHAR(20),
                    date_key VARCHAR(8),
                    semester_id INT,
                    grade DECIMAL(5,2),
                    letter_grade CHAR(1),
                    FOREIGN KEY (student_id) REFERENCES dim_student(student_id) ON DELETE CASCADE,
                    FOREIGN KEY (course_code) REFERENCES dim_course(course_code) ON DELETE CASCADE,
                    FOREIGN KEY (date_key) REFERENCES dim_time(date_key) ON DELETE CASCADE,
                    FOREIGN KEY (semester_id) REFERENCES dim_semester(semester_id) ON DELETE CASCADE,
                    INDEX idx_student (student_id),
                    INDEX idx_course (course_code),
                    INDEX idx_date (date_key),
                    INDEX idx_semester (semester_id),
                    INDEX idx_grade (grade)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
            conn.commit()
        
        # Fact_Enrollment
        enrollments = silver_data['enrollments'].copy()
        enrollments['date_key'] = pd.to_datetime(enrollments['enrollment_date'], errors='coerce').dt.strftime('%Y%m%d').fillna('')
        enrollments['semester_id'] = enrollments['semester'].map({
            'Fall 2023': 1, 'Spring 2024': 2, 'Fall 2024': 3, 'Spring 2025': 4
        }).fillna(1)  # Default to 1 if unmapped
        
        # Filter out rows with invalid date_key (must exist in dim_time)
        fact_enrollment = enrollments[['enrollment_id', 'student_id', 'course_code', 
                                      'date_key', 'semester_id', 'status']].copy()
        fact_enrollment = fact_enrollment[fact_enrollment['date_key'] != '']  # Remove invalid dates
        if not fact_enrollment.empty:
            fact_enrollment.to_sql('fact_enrollment', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        
        # Fact_Attendance
        attendance = silver_data['attendance'].copy()
        attendance['date_key'] = pd.to_datetime(attendance['attendance_date'], errors='coerce').dt.strftime('%Y%m%d').fillna('')
        
        # Filter out rows with invalid dates
        attendance = attendance[attendance['date_key'] != '']
        
        if not attendance.empty:
            # Aggregate attendance by student, course, and date
            attendance_agg = attendance.groupby(['student_id', 'course_code', 'date_key']).agg({
                'hours_attended': 'sum',
                'status': lambda x: (x == 'Present').sum() if 'Present' in x.values else 0
            }).reset_index()
            attendance_agg.columns = ['student_id', 'course_code', 'date_key', 
                                      'total_hours', 'days_present']
            
            fact_attendance = attendance_agg.copy()
            fact_attendance.to_sql('fact_attendance', engine, if_exists='append', index=False, method='multi', chunksize=5000)
        
        # Fact_Payment
        payments = silver_data['payments'].copy()
        payments['date_key'] = pd.to_datetime(payments['payment_date'], errors='coerce').dt.strftime('%Y%m%d').fillna('')
        payments['semester_id'] = payments['semester'].map({
            'Fall 2023': 1, 'Spring 2024': 2, 'Fall 2024': 3, 'Spring 2025': 4
        }).fillna(1)  # Default to 1 if unmapped
        
        # Filter out rows with invalid dates
        fact_payment = payments[['payment_id', 'student_id', 'date_key', 'semester_id',
                                'amount', 'payment_method', 'status']].copy()
        fact_payment = fact_payment[fact_payment['date_key'] != '']  # Remove invalid dates
        if not fact_payment.empty:
            fact_payment.to_sql('fact_payment', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        
        # Fact_Grade
        grades = silver_data['grades'].copy()
        grades['date_key'] = pd.to_datetime(grades['exam_date'], errors='coerce').dt.strftime('%Y%m%d').fillna('')
        grades['semester_id'] = grades['semester'].map({
            'Fall 2023': 1, 'Spring 2024': 2, 'Fall 2024': 3, 'Spring 2025': 4
        }).fillna(1)  # Default to 1 if unmapped
        
        # Filter out rows with invalid dates
        fact_grade = grades[['grade_id', 'student_id', 'course_code', 'date_key', 
                            'semester_id', 'grade', 'letter_grade']].copy()
        fact_grade = fact_grade[fact_grade['date_key'] != '']  # Remove invalid dates
        if not fact_grade.empty:
            fact_grade.to_sql('fact_grade', engine, if_exists='append', index=False, method='multi', chunksize=1000)
    
    def run(self):
        """Run the complete ETL pipeline"""
        print("Starting ETL Pipeline...")
        bronze_data = self.extract()
        silver_data = self.transform(bronze_data)
        self.load_to_warehouse(silver_data)
        print("ETL Pipeline completed successfully!")

if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run()
