-- Source Database 2: UCU_SourceDB2
-- Contains: Students, Courses, Attendance

CREATE DATABASE IF NOT EXISTS UCU_SourceDB2;
USE UCU_SourceDB2;

-- Students Table
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Courses Table
CREATE TABLE IF NOT EXISTS courses (
    course_code VARCHAR(20) PRIMARY KEY,
    course_name VARCHAR(100),
    credits INT,
    department VARCHAR(50),
    INDEX idx_department (department)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Attendance Table
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;




