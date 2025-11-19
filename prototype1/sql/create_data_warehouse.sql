-- Data Warehouse: UCU_DataWarehouse
-- Star Schema with Dimension and Fact Tables

CREATE DATABASE IF NOT EXISTS UCU_DataWarehouse;
USE UCU_DataWarehouse;

-- Dimension: Student
CREATE TABLE IF NOT EXISTS dim_student (
    student_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    gender CHAR(1),
    nationality VARCHAR(50),
    admission_date DATE,
    INDEX idx_name (last_name, first_name),
    INDEX idx_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Dimension: Course
CREATE TABLE IF NOT EXISTS dim_course (
    course_code VARCHAR(20) PRIMARY KEY,
    course_name VARCHAR(100),
    credits INT,
    department VARCHAR(50),
    INDEX idx_department (department)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Dimension: Time
CREATE TABLE IF NOT EXISTS dim_time (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Dimension: Semester
CREATE TABLE IF NOT EXISTS dim_semester (
    semester_id INT PRIMARY KEY,
    semester_name VARCHAR(50),
    academic_year VARCHAR(20),
    INDEX idx_academic_year (academic_year)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Fact: Enrollment
CREATE TABLE IF NOT EXISTS fact_enrollment (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Fact: Attendance
CREATE TABLE IF NOT EXISTS fact_attendance (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Fact: Payment
CREATE TABLE IF NOT EXISTS fact_payment (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Fact: Grade
CREATE TABLE IF NOT EXISTS fact_grade (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Insert default semester data
INSERT INTO dim_semester (semester_id, semester_name, academic_year) VALUES
(1, 'Fall 2023', '2023-2024'),
(2, 'Spring 2024', '2023-2024'),
(3, 'Fall 2024', '2024-2025'),
(4, 'Spring 2025', '2024-2025')
ON DUPLICATE KEY UPDATE 
    semester_name = VALUES(semester_name),
    academic_year = VALUES(academic_year);

-- Populate time dimension (2023-01-01 to 2025-12-31)
-- This is a simplified version - you may want to use a stored procedure
-- For now, we'll populate it via Python script




