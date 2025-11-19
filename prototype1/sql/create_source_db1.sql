-- Source Database 1: UCU_SourceDB1
-- Contains: Students, Courses, Enrollments

CREATE DATABASE IF NOT EXISTS UCU_SourceDB1;
USE UCU_SourceDB1;

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

-- Enrollments Table
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;




