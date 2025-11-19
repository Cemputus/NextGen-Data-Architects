"""
Flask Backend API for University Data Engineering System
"""
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
import pandas as pd
from datetime import datetime, timedelta
import bcrypt
from sqlalchemy import create_engine, text
from config import DATA_WAREHOUSE_CONN_STRING, SECRET_KEY, JWT_SECRET_KEY
from ml_model import StudentPerformancePredictor

app = Flask(__name__)
app.config['SECRET_KEY'] = SECRET_KEY
app.config['JWT_SECRET_KEY'] = JWT_SECRET_KEY
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(hours=24)

CORS(app)
jwt = JWTManager(app)

# Initialize ML model
predictor = StudentPerformancePredictor()

# User database (in production, use proper database)
users_db = {
    'admin': {
        'password': bcrypt.hashpw('admin123'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8'),
        'role': 'admin'
    },
    'analyst': {
        'password': bcrypt.hashpw('analyst123'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8'),
        'role': 'analyst'
    }
}

@app.route('/api/login', methods=['POST'])
def login():
    """User login endpoint"""
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    
    if not username or not password:
        return jsonify({'error': 'Username and password required'}), 400
    
    user = users_db.get(username)
    if user and bcrypt.checkpw(password.encode('utf-8'), user['password'].encode('utf-8')):
        access_token = create_access_token(identity=username)
        return jsonify({
            'access_token': access_token,
            'username': username,
            'role': user['role']
        }), 200
    
    return jsonify({'error': 'Invalid credentials'}), 401

@app.route('/api/dashboard/stats', methods=['GET'])
@jwt_required()
def get_dashboard_stats():
    """Get dashboard statistics"""
    try:
        engine = create_engine(DATA_WAREHOUSE_CONN_STRING)
        
        # Total students
        total_students = pd.read_sql_query("SELECT COUNT(DISTINCT student_id) as count FROM dim_student", engine)['count'][0] or 0
        
        # Total courses
        total_courses = pd.read_sql_query("SELECT COUNT(*) as count FROM dim_course", engine)['count'][0] or 0
        
        # Total enrollments
        total_enrollments = pd.read_sql_query("SELECT COUNT(*) as count FROM fact_enrollment", engine)['count'][0] or 0
        
        # Average grade
        avg_grade_result = pd.read_sql_query("SELECT AVG(grade) as avg FROM fact_grade", engine)
        avg_grade = avg_grade_result['avg'][0] if not avg_grade_result.empty and avg_grade_result['avg'][0] is not None else 0
        
        # Total payments
        total_payments_result = pd.read_sql_query(
            "SELECT SUM(amount) as total FROM fact_payment WHERE status = 'Completed'", engine
        )
        total_payments = total_payments_result['total'][0] if not total_payments_result.empty and total_payments_result['total'][0] is not None else 0
        
        # Average attendance
        avg_attendance_result = pd.read_sql_query(
            "SELECT AVG(total_hours) as avg FROM fact_attendance", engine
        )
        avg_attendance = avg_attendance_result['avg'][0] if not avg_attendance_result.empty and avg_attendance_result['avg'][0] is not None else 0
        
        engine.dispose()
        
        return jsonify({
            'total_students': int(total_students),
            'total_courses': int(total_courses),
            'total_enrollments': int(total_enrollments),
            'avg_grade': round(float(avg_grade), 2),
            'total_payments': round(float(total_payments), 2),
            'avg_attendance': round(float(avg_attendance), 2)
        })
    except Exception as e:
        print(f"Error in get_dashboard_stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/dashboard/students-by-department', methods=['GET'])
@jwt_required()
def get_students_by_department():
    """Get student count by department"""
    try:
        engine = create_engine(DATA_WAREHOUSE_CONN_STRING)
        
        query = """
        SELECT 
            dc.department,
            COUNT(DISTINCT fe.student_id) as student_count
        FROM fact_enrollment fe
        JOIN dim_course dc ON fe.course_code = dc.course_code
        GROUP BY dc.department
        ORDER BY student_count DESC
        """
        
        df = pd.read_sql_query(query, engine)
        engine.dispose()
        
        return jsonify({
            'departments': df['department'].tolist(),
            'counts': df['student_count'].tolist()
        })
    except Exception as e:
        print(f"Error in get_students_by_department: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/dashboard/grades-over-time', methods=['GET'])
@jwt_required()
def get_grades_over_time():
    """Get average grades over time"""
    try:
        engine = create_engine(DATA_WAREHOUSE_CONN_STRING)
        
        query = """
        SELECT 
            CONCAT(dt.month_name, ' ', CAST(dt.year AS CHAR)) as period,
            AVG(fg.grade) as avg_grade
        FROM fact_grade fg
        JOIN dim_time dt ON fg.date_key = dt.date_key
        GROUP BY dt.year, dt.month, dt.month_name
        ORDER BY dt.year, dt.month
        """
        
        df = pd.read_sql_query(query, engine)
        engine.dispose()
        
        return jsonify({
            'periods': df['period'].tolist(),
            'grades': df['avg_grade'].round(2).tolist()
        })
    except Exception as e:
        print(f"Error in get_grades_over_time: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/dashboard/payment-status', methods=['GET'])
@jwt_required()
def get_payment_status():
    """Get payment status distribution"""
    try:
        engine = create_engine(DATA_WAREHOUSE_CONN_STRING)
        
        query = """
        SELECT 
            status,
            COUNT(*) as count
        FROM fact_payment
        GROUP BY status
        """
        
        df = pd.read_sql_query(query, engine)
        engine.dispose()
        
        return jsonify({
            'statuses': df['status'].tolist(),
            'counts': df['count'].tolist()
        })
    except Exception as e:
        print(f"Error in get_payment_status: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/dashboard/attendance-by-course', methods=['GET'])
@jwt_required()
def get_attendance_by_course():
    """Get attendance statistics by course"""
    try:
        engine = create_engine(DATA_WAREHOUSE_CONN_STRING)
        
        query = """
        SELECT 
            dc.course_name,
            AVG(fa.total_hours) as avg_hours,
            SUM(fa.days_present) as total_days
        FROM fact_attendance fa
        JOIN dim_course dc ON fa.course_code = dc.course_code
        GROUP BY dc.course_name
        ORDER BY avg_hours DESC
        LIMIT 10
        """
        
        df = pd.read_sql_query(query, engine)
        engine.dispose()
        
        return jsonify({
            'courses': df['course_name'].tolist(),
            'avg_hours': df['avg_hours'].round(2).tolist(),
            'total_days': df['total_days'].tolist()
        })
    except Exception as e:
        print(f"Error in get_attendance_by_course: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/dashboard/grade-distribution', methods=['GET'])
@jwt_required()
def get_grade_distribution():
    """Get grade distribution"""
    try:
        engine = create_engine(DATA_WAREHOUSE_CONN_STRING)
        
        query = """
        SELECT 
            letter_grade,
            COUNT(*) as count
        FROM fact_grade
        GROUP BY letter_grade
        ORDER BY 
            CASE letter_grade
                WHEN 'A' THEN 1
                WHEN 'B' THEN 2
                WHEN 'C' THEN 3
                WHEN 'D' THEN 4
                WHEN 'F' THEN 5
            END
        """
        
        df = pd.read_sql_query(query, engine)
        engine.dispose()
        
        return jsonify({
            'grades': df['letter_grade'].tolist(),
            'counts': df['count'].tolist()
        })
    except Exception as e:
        print(f"Error in get_grade_distribution: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/dashboard/predict-performance', methods=['POST'])
@jwt_required()
def predict_performance():
    """Predict student performance"""
    data = request.get_json()
    student_id = data.get('student_id')
    
    if not student_id:
        return jsonify({'error': 'Student ID required'}), 400
    
    try:
        prediction = predictor.predict(student_id)
        return jsonify({
            'student_id': student_id,
            'predicted_grade': round(float(prediction), 2)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/dashboard/top-students', methods=['GET'])
@jwt_required()
def get_top_students():
    """Get top performing students"""
    try:
        engine = create_engine(DATA_WAREHOUSE_CONN_STRING)
        
        query = """
        SELECT 
            ds.student_id,
            CONCAT(ds.first_name, ' ', ds.last_name) as student_name,
            AVG(fg.grade) as avg_grade
        FROM fact_grade fg
        JOIN dim_student ds ON fg.student_id = ds.student_id
        GROUP BY ds.student_id, ds.first_name, ds.last_name
        ORDER BY avg_grade DESC
        LIMIT 10
        """
        
        df = pd.read_sql_query(query, engine)
        engine.dispose()
        
        return jsonify({
            'students': df['student_name'].tolist(),
            'grades': df['avg_grade'].round(2).tolist()
        })
    except Exception as e:
        print(f"Error in get_top_students: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/report/generate', methods=['POST', 'GET'])
@jwt_required()
def generate_report():
    """Generate PDF report"""
    from pdf_generator import PDFReportGenerator
    from flask import send_file
    import os
    
    try:
        # Generate PDF
        generator = PDFReportGenerator(
            api_base_url=f"http://localhost:5000",
            token=request.headers.get('Authorization', '').replace('Bearer ', '')
        )
        
        output_path = generator.generate_report()
        
        # Return PDF file
        if os.path.exists(output_path):
            return send_file(
                output_path, 
                as_attachment=True, 
                download_name=f'university_report_{datetime.now().strftime("%Y%m%d")}.pdf',
                mimetype='application/pdf'
            )
        else:
            return jsonify({'error': 'PDF generation failed'}), 500
    except Exception as e:
        import traceback
        print(f"Error generating PDF: {e}")
        print(traceback.format_exc())
        # Fallback: return JSON data
        engine = create_engine(DATA_WAREHOUSE_CONN_STRING)
        
        stats_query = """
        SELECT 
            (SELECT COUNT(DISTINCT student_id) FROM dim_student) as total_students,
            (SELECT COUNT(*) FROM dim_course) as total_courses,
            (SELECT COUNT(*) FROM fact_enrollment) as total_enrollments,
            (SELECT AVG(grade) FROM fact_grade) as avg_grade,
            (SELECT SUM(amount) FROM fact_payment WHERE status = 'Completed') as total_payments
        """
        stats = pd.read_sql_query(stats_query, engine).to_dict('records')[0]
        
        dept_query = """
        SELECT 
            dc.department,
            COUNT(DISTINCT fe.student_id) as student_count
        FROM fact_enrollment fe
        JOIN dim_course dc ON fe.course_code = dc.course_code
        GROUP BY dc.department
        """
        departments = pd.read_sql_query(dept_query, engine).to_dict('records')
        
        grade_query = """
        SELECT 
            letter_grade,
            COUNT(*) as count
        FROM fact_grade
        GROUP BY letter_grade
        """
        grades = pd.read_sql_query(grade_query, engine).to_dict('records')
        
        engine.dispose()
        
        return jsonify({
            'stats': stats,
            'departments': departments,
            'grades': grades,
            'generated_at': datetime.now().isoformat()
        })

if __name__ == '__main__':
    # Load ML model
    try:
        predictor.load_model()
        print("ML model loaded successfully")
    except Exception as e:
        print(f"Could not load ML model: {e}")
        print("Training new ML model...")
        try:
            predictor.train()
        except Exception as train_error:
            print(f"Error training model: {train_error}")
    
    app.run(debug=True, port=5000)

