"""
Analytics API with RBAC and advanced filtering
Includes FEX analytics, high school analytics, and role-based data scoping
"""
from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt
from sqlalchemy import create_engine, text
import pandas as pd
from rbac import Role, Resource, Permission, has_permission
from datetime import datetime, timedelta
from config import DATA_WAREHOUSE_CONN_STRING

analytics_bp = Blueprint('analytics', __name__, url_prefix='/api/analytics')

def get_user_scope(claims):
    """Get user's data scope based on role"""
    role_str = claims.get('role', 'student')
    # Convert string role to Role enum, handling both string and enum inputs
    try:
        if isinstance(role_str, str):
            role = Role(role_str.lower())
        else:
            role = role_str
    except (ValueError, AttributeError):
        # Fallback to student if role is invalid
        role = Role.STUDENT
    
    scope = {
        'role': role,
        'student_id': claims.get('student_id'),
        'staff_id': claims.get('staff_id'),
        'department_id': claims.get('department_id'),
        'faculty_id': claims.get('faculty_id'),
        'access_number': claims.get('access_number')
    }
    return scope

def build_filter_query(filters, base_query, user_scope):
    """Build SQL query with filters and role-based scoping"""
    where_clauses = []
    params = {}
    
    # Role-based scoping
    if user_scope['role'] == Role.STUDENT:
        if user_scope['student_id']:
            where_clauses.append("ds.student_id = :student_id")
            params['student_id'] = user_scope['student_id']
        elif user_scope['access_number']:
            where_clauses.append("ds.access_number = :access_number")
            params['access_number'] = user_scope['access_number']
    
    elif user_scope['role'] == Role.STAFF:
        # Staff can see their classes - handled separately
        pass
    
    elif user_scope['role'] == Role.HOD:
        if user_scope['department_id']:
            where_clauses.append("dc.department_id = :department_id")
            params['department_id'] = user_scope['department_id']
    
    elif user_scope['role'] == Role.DEAN:
        if user_scope['faculty_id']:
            where_clauses.append("df.faculty_id = :faculty_id")
            params['faculty_id'] = user_scope['faculty_id']
    
    # Apply filters
    if filters:
        if filters.get('faculty_id'):
            where_clauses.append("df.faculty_id = :filter_faculty_id")
            params['filter_faculty_id'] = filters['faculty_id']
        
        if filters.get('department_id'):
            where_clauses.append("dc.department_id = :filter_department_id")
            params['filter_department_id'] = filters['department_id']
        
        if filters.get('program_id'):
            where_clauses.append("dp.program_id = :filter_program_id")
            params['filter_program_id'] = filters['program_id']
        
        if filters.get('course_code'):
            where_clauses.append("dc.course_code = :filter_course_code")
            params['filter_course_code'] = filters['course_code']
        
        if filters.get('access_number'):
            where_clauses.append("ds.access_number = :filter_access_number")
            params['filter_access_number'] = filters['access_number']
        
        if filters.get('reg_number'):
            where_clauses.append("ds.student_id = :filter_reg_number")
            params['filter_reg_number'] = filters['reg_number']
        
        if filters.get('intake_year'):
            where_clauses.append("YEAR(ds.admission_date) = :filter_intake_year")
            params['filter_intake_year'] = filters['intake_year']
        
        if filters.get('semester_id'):
            where_clauses.append("fg.semester_id = :filter_semester_id")
            params['filter_semester_id'] = filters['semester_id']
        
        if filters.get('gender'):
            where_clauses.append("ds.gender = :filter_gender")
            params['filter_gender'] = filters['gender']
        
        if filters.get('high_school'):
            where_clauses.append("ds.high_school LIKE :filter_high_school")
            params['filter_high_school'] = f"%{filters['high_school']}%"
    
    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses)
    
    return base_query, params

@analytics_bp.route('/fex', methods=['GET'])
@jwt_required()
def get_fex_analytics():
    """Get FEX analytics with drilldown capabilities"""
    try:
        claims = get_jwt()
        user_scope = get_user_scope(claims)
        
        # Check permission
        if not has_permission(user_scope['role'], Resource.FEX_ANALYTICS, Permission.READ, user_scope):
            return jsonify({'error': 'Permission denied'}), 403
        
        filters = request.args.to_dict()
        engine = create_engine(DATA_WAREHOUSE_CONN_STRING)
        
        # Base query for FEX analytics
        base_query = """
        SELECT 
            COUNT(CASE WHEN fg.exam_status = 'FEX' THEN 1 END) as total_fex,
            COUNT(CASE WHEN fg.exam_status = 'MEX' THEN 1 END) as total_mex,
            COUNT(CASE WHEN fg.exam_status = 'FCW' THEN 1 END) as total_fcw,
            COUNT(CASE WHEN fg.exam_status = 'Completed' THEN 1 END) as total_completed,
            COUNT(*) as total_exams,
            AVG(CASE WHEN fg.exam_status = 'FEX' THEN fg.grade ELSE NULL END) as avg_fex_score,
            dc.department,
            df.faculty_name,
            dp.program_name,
            dc.course_code,
            dc.course_name
        FROM fact_grade fg
        JOIN dim_student ds ON fg.student_id = ds.student_id
        JOIN dim_course dc ON fg.course_code = dc.course_code
        LEFT JOIN dim_program dp ON ds.program_id = dp.program_id
        LEFT JOIN dim_department ddept ON dp.department_id = ddept.department_id
        LEFT JOIN dim_faculty df ON ddept.faculty_id = df.faculty_id
        """
        
        query, params = build_filter_query(filters, base_query, user_scope)
        
        # Add grouping based on drilldown level
        drilldown = filters.get('drilldown', 'overall')
        if drilldown == 'faculty':
            query += " GROUP BY df.faculty_id, df.faculty_name"
        elif drilldown == 'department':
            query += " GROUP BY dc.department_id, dc.department"
        elif drilldown == 'program':
            query += " GROUP BY dp.program_id, dp.program_name"
        elif drilldown == 'course':
            query += " GROUP BY dc.course_code, dc.course_name"
        else:
            query += " GROUP BY df.faculty_id, df.faculty_name, dc.department, dp.program_name, dc.course_code, dc.course_name"
        
        df = pd.read_sql_query(text(query), engine, params=params)
        engine.dispose()
        
        return jsonify({
            'data': df.to_dict('records'),
            'summary': {
                'total_fex': int(df['total_fex'].sum()) if not df.empty else 0,
                'total_mex': int(df['total_mex'].sum()) if not df.empty else 0,
                'total_fcw': int(df['total_fcw'].sum()) if not df.empty else 0,
                'total_completed': int(df['total_completed'].sum()) if not df.empty else 0,
                'fex_rate': round((df['total_fex'].sum() / df['total_exams'].sum() * 100) if df['total_exams'].sum() > 0 else 0, 2)
            }
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@analytics_bp.route('/high-school', methods=['GET'])
@jwt_required()
def get_high_school_analytics():
    """Get high school analytics - enrollment, retention, graduation rates, tuition completion, performance"""
    try:
        claims = get_jwt()
        user_scope = get_user_scope(claims)
        
        if not has_permission(user_scope['role'], Resource.HIGH_SCHOOL_ANALYTICS, Permission.READ, user_scope):
            return jsonify({'error': 'Permission denied'}), 403
        
        filters = request.args.to_dict()
        engine = create_engine(DATA_WAREHOUSE_CONN_STRING)
        
        query = """
        SELECT 
            ds.high_school,
            ds.high_school_district,
            COUNT(DISTINCT ds.student_id) as total_students,
            COUNT(DISTINCT CASE WHEN ds.status = 'Active' THEN ds.student_id END) as active_students,
            COUNT(DISTINCT CASE WHEN ds.status = 'Graduated' THEN ds.student_id END) as graduated_students,
            COUNT(DISTINCT CASE WHEN ds.status = 'Withdrawn' THEN ds.student_id END) as withdrawn_students,
            COUNT(DISTINCT fe.student_id) as enrolled_students,
            COUNT(DISTINCT dp.program_id) as programs_enrolled,
            -- Performance metrics
            AVG(CASE WHEN fg.exam_status = 'Completed' THEN fg.grade ELSE NULL END) as avg_grade,
            AVG(CASE WHEN fg.exam_status = 'Completed' THEN fg.coursework_score ELSE NULL END) as avg_coursework_score,
            AVG(CASE WHEN fg.exam_status = 'Completed' THEN fg.exam_score ELSE NULL END) as avg_exam_score,
            STDDEV(CASE WHEN fg.exam_status = 'Completed' THEN fg.grade ELSE NULL END) as grade_stddev,
            COUNT(CASE WHEN fg.exam_status = 'Completed' AND fg.grade >= 80 THEN 1 END) as grade_a_count,
            COUNT(CASE WHEN fg.exam_status = 'Completed' AND fg.grade >= 75 AND fg.grade < 80 THEN 1 END) as grade_bplus_count,
            COUNT(CASE WHEN fg.exam_status = 'Completed' AND fg.grade < 50 THEN 1 END) as grade_f_count,
            -- Exam status metrics
            COUNT(CASE WHEN fg.exam_status = 'FEX' THEN 1 END) as total_fex,
            COUNT(CASE WHEN fg.exam_status = 'MEX' THEN 1 END) as total_mex,
            COUNT(CASE WHEN fg.exam_status = 'FCW' THEN 1 END) as total_fcw,
            -- Tuition completion metrics
            SUM(CASE WHEN fp.status = 'Completed' THEN fp.amount ELSE 0 END) as total_paid,
            SUM(CASE WHEN fp.status = 'Pending' THEN fp.amount ELSE 0 END) as total_pending,
            SUM(fp.amount) as total_required,
            COUNT(DISTINCT CASE WHEN fp.status = 'Pending' AND fp.amount > 500000 THEN fp.student_id END) as students_with_significant_balance,
            CASE 
                WHEN SUM(fp.amount) > 0 
                THEN SUM(CASE WHEN fp.status = 'Completed' THEN fp.amount ELSE 0 END) / SUM(fp.amount) * 100
                ELSE 0 
            END as tuition_completion_rate,
            -- Attendance metrics
            AVG(fa.total_hours) as avg_attendance_hours,
            AVG(fa.days_present) as avg_days_present,
            -- Relationship metrics
            COUNT(CASE WHEN fg.absence_reason LIKE '%Tuition%' OR fg.absence_reason LIKE '%Financial%' THEN 1 END) as tuition_related_missed_exams,
            COUNT(CASE WHEN fp.status = 'Pending' AND fg.exam_status = 'MEX' THEN 1 END) as missed_exams_with_pending_fees
        FROM dim_student ds
        LEFT JOIN fact_enrollment fe ON ds.student_id = fe.student_id
        LEFT JOIN fact_grade fg ON ds.student_id = fg.student_id
        LEFT JOIN fact_payment fp ON ds.student_id = fp.student_id
        LEFT JOIN fact_attendance fa ON ds.student_id = fa.student_id
        LEFT JOIN dim_program dp ON ds.program_id = dp.program_id
        """
        
        query, params = build_filter_query(filters, query, user_scope)
        query += " GROUP BY ds.high_school, ds.high_school_district"
        query += " ORDER BY total_students DESC"
        
        df = pd.read_sql_query(text(query), engine, params=params)
        engine.dispose()
        
        # Calculate rates and relationships
        if not df.empty:
            df['retention_rate'] = (df['active_students'] / df['total_students'] * 100).round(2)
            df['graduation_rate'] = (df['graduated_students'] / df['total_students'] * 100).round(2)
            df['dropout_rate'] = (df['withdrawn_students'] / df['total_students'] * 100).round(2)
            df['fex_rate'] = (df['total_fex'] / (df['total_fex'] + df['total_mex'] + df['total_fex'].fillna(0) + 1) * 100).round(2)
            df['tuition_completion_rate'] = df['tuition_completion_rate'].fillna(0).round(2)
            
            # Performance vs Tuition Completion correlation
            df['performance_tuition_correlation'] = df.apply(
                lambda row: 'High Performance, High Tuition Completion' if row['avg_grade'] >= 70 and row['tuition_completion_rate'] >= 80
                else 'High Performance, Low Tuition Completion' if row['avg_grade'] >= 70 and row['tuition_completion_rate'] < 80
                else 'Low Performance, High Tuition Completion' if row['avg_grade'] < 70 and row['tuition_completion_rate'] >= 80
                else 'Low Performance, Low Tuition Completion', axis=1
            )
        
        return jsonify({
            'data': df.to_dict('records'),
            'summary': {
                'total_high_schools': len(df),
                'total_students': int(df['total_students'].sum()) if not df.empty else 0,
                'avg_retention_rate': round(df['retention_rate'].mean(), 2) if not df.empty else 0,
                'avg_graduation_rate': round(df['graduation_rate'].mean(), 2) if not df.empty else 0,
                'avg_tuition_completion_rate': round(df['tuition_completion_rate'].mean(), 2) if not df.empty else 0,
                'avg_performance': round(df['avg_grade'].mean(), 2) if not df.empty else 0,
                'correlation_analysis': {
                    'high_perf_high_tuition': int(len(df[(df['avg_grade'] >= 70) & (df['tuition_completion_rate'] >= 80)])),
                    'high_perf_low_tuition': int(len(df[(df['avg_grade'] >= 70) & (df['tuition_completion_rate'] < 80)])),
                    'low_perf_high_tuition': int(len(df[(df['avg_grade'] < 70) & (df['tuition_completion_rate'] >= 80)])),
                    'low_perf_low_tuition': int(len(df[(df['avg_grade'] < 70) & (df['tuition_completion_rate'] < 80)]))
                }
            }
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@analytics_bp.route('/filter-options', methods=['GET'])
@jwt_required()
def get_filter_options():
    """Get available filter options based on user role"""
    try:
        claims = get_jwt()
        user_scope = get_user_scope(claims)
        engine = create_engine(DATA_WAREHOUSE_CONN_STRING)
        
        options = {}
        
        # Get faculties (if user has access)
        if has_permission(user_scope['role'], Resource.FACULTY_ANALYTICS, Permission.READ, user_scope):
            faculties = pd.read_sql_query(
                "SELECT DISTINCT faculty_id, faculty_name FROM dim_faculty ORDER BY faculty_name",
                engine
            )
            options['faculties'] = faculties.to_dict('records')
        
        # Get departments
        dept_query = "SELECT DISTINCT department_id, department_name FROM dim_department"
        if user_scope['role'] == Role.HOD and user_scope['department_id']:
            dept_query += f" WHERE department_id = {user_scope['department_id']}"
        dept_query += " ORDER BY department_name"
        
        departments = pd.read_sql_query(dept_query, engine)
        options['departments'] = departments.to_dict('records')
        
        # Get programs
        programs = pd.read_sql_query(
            "SELECT DISTINCT program_id, program_name FROM dim_program ORDER BY program_name",
            engine
        )
        options['programs'] = programs.to_dict('records')
        
        # Get courses
        courses = pd.read_sql_query(
            "SELECT DISTINCT course_code, course_name FROM dim_course ORDER BY course_code",
            engine
        )
        options['courses'] = courses.to_dict('records')
        
        # Get semesters
        semesters = pd.read_sql_query(
            "SELECT semester_id, semester_name FROM dim_semester ORDER BY semester_id",
            engine
        )
        options['semesters'] = semesters.to_dict('records')
        
        # Get high schools
        high_schools = pd.read_sql_query(
            "SELECT DISTINCT high_school, high_school_district FROM dim_student WHERE high_school IS NOT NULL ORDER BY high_school",
            engine
        )
        options['high_schools'] = high_schools.to_dict('records')
        
        # Get intake years
        intake_years = pd.read_sql_query(
            "SELECT DISTINCT YEAR(admission_date) as year FROM dim_student WHERE admission_date IS NOT NULL ORDER BY year DESC",
            engine
        )
        options['intake_years'] = intake_years['year'].tolist()
        
        engine.dispose()
        
        return jsonify(options), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
