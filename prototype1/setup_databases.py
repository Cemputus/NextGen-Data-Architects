"""
Setup script to create source databases and CSV files with university data
Uses MySQL
"""
import pymysql
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path
from sqlalchemy import create_engine, text
from config import (
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD,
    DB1_NAME, DB2_NAME, CSV1_PATH, CSV2_PATH,
    get_pymysql_params, DB1_CONN_STRING, DB2_CONN_STRING
)

# Create data directory
Path(CSV1_PATH).parent.mkdir(parents=True, exist_ok=True)

def create_database_if_not_exists(database_name):
    """Create MySQL database if it doesn't exist"""
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
        cursor.execute(f"SHOW DATABASES LIKE '{database_name}'")
        if cursor.fetchone() is None:
            cursor.execute(f"CREATE DATABASE `{database_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            print(f"Database {database_name} created successfully")
        else:
            print(f"Database {database_name} already exists")
        
        conn.close()
    except Exception as e:
        print(f"Error creating database {database_name}: {e}")
        raise

def execute_sql_file(engine, sql_file_path):
    """Execute SQL file"""
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        # Split by semicolon and execute each statement
        statements = [s.strip() for s in sql_script.split(';') if s.strip() and not s.strip().startswith('--')]
        
        with engine.connect() as conn:
            for statement in statements:
                if statement:
                    try:
                        conn.execute(text(statement))
                    except Exception as e:
                        # Skip errors for IF NOT EXISTS statements
                        if 'already exists' not in str(e).lower():
                            print(f"Warning executing statement: {e}")
            conn.commit()
    except FileNotFoundError:
        print(f"SQL file not found: {sql_file_path}, creating tables manually...")
        return False
    except Exception as e:
        print(f"Error executing SQL file: {e}")
        return False
    return True

def generate_student_data():
    """Generate realistic university student data"""
    np.random.seed(42)
    random.seed(42)
    
    # Generate student IDs
    student_ids = [f"STU{str(i).zfill(6)}" for i in range(1, 1001)]
    
    # Names (Uganda Christian University style)
    first_names = ["John", "Mary", "Peter", "Sarah", "David", "Grace", "James", "Ruth", 
                   "Michael", "Esther", "Paul", "Joy", "Daniel", "Faith", "Joseph", "Hope",
                   "Andrew", "Peace", "Mark", "Mercy", "Luke", "Patience", "Stephen", "Priscilla"]
    last_names = ["Mukasa", "Nakato", "Kato", "Nalubega", "Ssemwogerere", "Nabukeera", 
                  "Mugerwa", "Nakazibwe", "Kawuma", "Nalubwama", "Ssempijja", "Nabatanzi",
                  "Mugisha", "Nakayiza", "Kasozi", "Nalubega", "Ssebunya", "Nabukeera"]
    
    students = []
    for sid in student_ids:
        students.append({
            'student_id': sid,
            'first_name': random.choice(first_names),
            'last_name': random.choice(last_names),
            'email': f"{sid.lower()}@ucu.ac.ug",
            'phone': f"+2567{random.randint(10000000, 99999999)}",
            'date_of_birth': (datetime.now() - timedelta(days=random.randint(6570, 10950))).strftime('%Y-%m-%d'),
            'gender': random.choice(['M', 'F']),
            'nationality': random.choice(['Ugandan', 'Kenyan', 'Tanzanian', 'Rwandan', 'South Sudanese']),
            'admission_date': (datetime.now() - timedelta(days=random.randint(0, 1460))).strftime('%Y-%m-%d')
        })
    return pd.DataFrame(students)

def generate_course_data():
    """Generate course data"""
    courses = [
        {'course_code': 'CS101', 'course_name': 'Introduction to Computer Science', 'credits': 3, 'department': 'Computer Science'},
        {'course_code': 'CS201', 'course_name': 'Data Structures and Algorithms', 'credits': 4, 'department': 'Computer Science'},
        {'course_code': 'CS301', 'course_name': 'Database Systems', 'credits': 3, 'department': 'Computer Science'},
        {'course_code': 'CS401', 'course_name': 'Machine Learning', 'credits': 4, 'department': 'Computer Science'},
        {'course_code': 'BA101', 'course_name': 'Introduction to Business', 'credits': 3, 'department': 'Business'},
        {'course_code': 'BA201', 'course_name': 'Financial Accounting', 'credits': 3, 'department': 'Business'},
        {'course_code': 'BA301', 'course_name': 'Marketing Management', 'credits': 3, 'department': 'Business'},
        {'course_code': 'EN101', 'course_name': 'English Composition', 'credits': 2, 'department': 'English'},
        {'course_code': 'EN201', 'course_name': 'Literature Survey', 'credits': 3, 'department': 'English'},
        {'course_code': 'MA101', 'course_name': 'Calculus I', 'credits': 4, 'department': 'Mathematics'},
        {'course_code': 'MA201', 'course_name': 'Statistics', 'credits': 3, 'department': 'Mathematics'},
        {'course_code': 'PH101', 'course_name': 'Introduction to Physics', 'credits': 4, 'department': 'Physics'},
    ]
    return pd.DataFrame(courses)

def generate_enrollment_data(students_df, courses_df):
    """Generate enrollment data"""
    enrollments = []
    student_ids = students_df['student_id'].tolist()
    course_codes = courses_df['course_code'].tolist()
    
    for sid in student_ids:
        # Each student enrolls in 4-6 courses
        num_courses = random.randint(4, 6)
        enrolled_courses = random.sample(course_codes, num_courses)
        
        for course_code in enrolled_courses:
            enrollments.append({
                'enrollment_id': f"ENR{len(enrollments)+1:06d}",
                'student_id': sid,
                'course_code': course_code,
                'enrollment_date': (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'),
                'status': random.choice(['Active', 'Completed', 'Dropped']),
                'semester': random.choice(['Fall 2023', 'Spring 2024', 'Fall 2024', 'Spring 2025'])
            })
    return pd.DataFrame(enrollments)

def generate_attendance_data(students_df, courses_df):
    """Generate attendance data"""
    attendance = []
    student_ids = students_df['student_id'].tolist()
    course_codes = courses_df['course_code'].tolist()
    
    # Generate attendance for last 90 days
    for day_offset in range(90):
        date = (datetime.now() - timedelta(days=day_offset)).strftime('%Y-%m-%d')
        
        # Random selection of students attending classes
        for course_code in course_codes:
            num_students = random.randint(15, 30)
            attending_students = random.sample(student_ids, min(num_students, len(student_ids)))
            
            for sid in attending_students:
                attendance.append({
                    'attendance_id': f"ATT{len(attendance)+1:06d}",
                    'student_id': sid,
                    'course_code': course_code,
                    'attendance_date': date,
                    'status': random.choice(['Present', 'Absent', 'Late']),
                    'hours_attended': random.choice([1.0, 1.5, 2.0, 0.0])
                })
    return pd.DataFrame(attendance)

def generate_tuition_data(students_df):
    """Generate tuition payment data"""
    payments = []
    student_ids = students_df['student_id'].tolist()
    
    for sid in student_ids:
        # Generate payment history
        num_payments = random.randint(1, 8)
        base_amount = random.choice([500000, 750000, 1000000, 1250000])  # UGX
        
        for i in range(num_payments):
            payment_date = (datetime.now() - timedelta(days=random.randint(0, 730))).strftime('%Y-%m-%d')
            amount = base_amount + random.randint(-50000, 50000)
            payments.append({
                'payment_id': f"PAY{len(payments)+1:06d}",
                'student_id': sid,
                'payment_date': payment_date,
                'amount': amount,
                'payment_method': random.choice(['Bank Transfer', 'Mobile Money', 'Cash', 'Credit Card']),
                'status': random.choice(['Completed', 'Pending', 'Failed']),
                'semester': random.choice(['Fall 2023', 'Spring 2024', 'Fall 2024', 'Spring 2025'])
            })
    return pd.DataFrame(payments)

def generate_grades_data(students_df, courses_df):
    """Generate grades data"""
    grades = []
    student_ids = students_df['student_id'].tolist()
    course_codes = courses_df['course_code'].tolist()
    
    for sid in student_ids:
        enrolled_courses = random.sample(course_codes, random.randint(3, 6))
        for course_code in enrolled_courses:
            # Generate grade based on normal distribution
            grade = max(0, min(100, np.random.normal(70, 15)))
            grades.append({
                'grade_id': f"GRD{len(grades)+1:06d}",
                'student_id': sid,
                'course_code': course_code,
                'grade': round(grade, 2),
                'letter_grade': get_letter_grade(grade),
                'semester': random.choice(['Fall 2023', 'Spring 2024', 'Fall 2024', 'Spring 2025']),
                'exam_date': (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
            })
    return pd.DataFrame(grades)

def get_letter_grade(score):
    """Convert numeric grade to letter grade"""
    if score >= 90:
        return 'A'
    elif score >= 80:
        return 'B'
    elif score >= 70:
        return 'C'
    elif score >= 60:
        return 'D'
    else:
        return 'F'

def create_database1():
    """Create first source database"""
    print(f"Creating database {DB1_NAME}...")
    create_database_if_not_exists(DB1_NAME)
    
    # Create engine for database 1
    engine = create_engine(DB1_CONN_STRING)
    
    # Try to execute SQL file, otherwise create tables manually
    sql_file = Path(__file__).parent / 'sql' / 'create_source_db1.sql'
    if not execute_sql_file(engine, sql_file):
        # Create tables manually
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS students (
                    student_id VARCHAR(20) PRIMARY KEY,
                    first_name VARCHAR(50),
                    last_name VARCHAR(50),
                    email VARCHAR(100),
                    phone VARCHAR(20),
                    date_of_birth DATE,
                    gender CHAR(1),
                    nationality VARCHAR(50),
                    admission_date DATE,
                    INDEX idx_email (email),
                    INDEX idx_name (last_name, first_name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS courses (
                    course_code VARCHAR(20) PRIMARY KEY,
                    course_name VARCHAR(100),
                    credits INT,
                    department VARCHAR(50),
                    INDEX idx_department (department)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS enrollments (
                    enrollment_id VARCHAR(20) PRIMARY KEY,
                    student_id VARCHAR(20),
                    course_code VARCHAR(20),
                    enrollment_date DATE,
                    status VARCHAR(20),
                    semester VARCHAR(50),
                    FOREIGN KEY (student_id) REFERENCES students(student_id) ON DELETE CASCADE,
                    FOREIGN KEY (course_code) REFERENCES courses(course_code) ON DELETE CASCADE,
                    INDEX idx_student (student_id),
                    INDEX idx_course (course_code),
                    INDEX idx_semester (semester)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
            conn.commit()
    
    # Generate and insert data
    students_df = generate_student_data()
    courses_df = generate_course_data()
    enrollments_df = generate_enrollment_data(students_df, courses_df)
    
    students_df.to_sql('students', engine, if_exists='replace', index=False, method='multi', chunksize=1000)
    courses_df.to_sql('courses', engine, if_exists='replace', index=False)
    enrollments_df.to_sql('enrollments', engine, if_exists='replace', index=False, method='multi', chunksize=1000)
    
    engine.dispose()
    print(f"Database 1 ({DB1_NAME}) created and populated successfully!")

def create_database2():
    """Create second source database"""
    print(f"Creating database {DB2_NAME}...")
    create_database_if_not_exists(DB2_NAME)
    
    # Create engine for database 2
    engine = create_engine(DB2_CONN_STRING)
    
    # Try to execute SQL file, otherwise create tables manually
    sql_file = Path(__file__).parent / 'sql' / 'create_source_db2.sql'
    if not execute_sql_file(engine, sql_file):
        # Create tables manually
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS students (
                    student_id VARCHAR(20) PRIMARY KEY,
                    first_name VARCHAR(50),
                    last_name VARCHAR(50),
                    email VARCHAR(100),
                    phone VARCHAR(20),
                    date_of_birth DATE,
                    gender CHAR(1),
                    nationality VARCHAR(50),
                    admission_date DATE,
                    INDEX idx_email (email),
                    INDEX idx_name (last_name, first_name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS courses (
                    course_code VARCHAR(20) PRIMARY KEY,
                    course_name VARCHAR(100),
                    credits INT,
                    department VARCHAR(50),
                    INDEX idx_department (department)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS attendance (
                    attendance_id VARCHAR(20) PRIMARY KEY,
                    student_id VARCHAR(20),
                    course_code VARCHAR(20),
                    attendance_date DATE,
                    status VARCHAR(20),
                    hours_attended DECIMAL(5,2),
                    FOREIGN KEY (student_id) REFERENCES students(student_id) ON DELETE CASCADE,
                    FOREIGN KEY (course_code) REFERENCES courses(course_code) ON DELETE CASCADE,
                    INDEX idx_student (student_id),
                    INDEX idx_course (course_code),
                    INDEX idx_date (attendance_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
            conn.commit()
    
    # Generate and insert data
    students_df = generate_student_data()
    courses_df = generate_course_data()
    attendance_df = generate_attendance_data(students_df, courses_df)
    
    students_df.to_sql('students', engine, if_exists='replace', index=False, method='multi', chunksize=1000)
    courses_df.to_sql('courses', engine, if_exists='replace', index=False)
    attendance_df.to_sql('attendance', engine, if_exists='replace', index=False, method='multi', chunksize=5000)
    
    engine.dispose()
    print(f"Database 2 ({DB2_NAME}) created and populated successfully!")

def create_csv1():
    """Create first CSV file with tuition payment data"""
    students_df = generate_student_data()
    payments_df = generate_tuition_data(students_df)
    payments_df.to_csv(CSV1_PATH, index=False)
    print(f"CSV 1 created at {CSV1_PATH}")

def create_csv2():
    """Create second CSV file with grades data"""
    students_df = generate_student_data()
    courses_df = generate_course_data()
    grades_df = generate_grades_data(students_df, courses_df)
    grades_df.to_csv(CSV2_PATH, index=False)
    print(f"CSV 2 created at {CSV2_PATH}")

if __name__ == "__main__":
    print("Creating source databases and CSV files...")
    print("Note: Make sure MySQL is running and credentials are correct in config.py")
    print()
    try:
        create_database1()
        create_database2()
        create_csv1()
        create_csv2()
        print()
        print("All source data created successfully!")
    except Exception as e:
        print(f"Error: {e}")
        print("Please check:")
        print("1. MySQL is running")
        print("2. MySQL credentials in config.py are correct")
        print("3. User has CREATE DATABASE privileges")
