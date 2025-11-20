"""
Prediction API with multiple ML models and scenario analysis
"""
from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt
from sqlalchemy import create_engine, text
import pandas as pd
# Import from parent directory (backend/)
import sys
from pathlib import Path
backend_dir = Path(__file__).parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from rbac import Role, Resource, Permission, has_permission
from ml_models import MultiModelPredictor
from config import DATA_WAREHOUSE_CONN_STRING

predictions_bp = Blueprint('predictions', __name__, url_prefix='/api/predictions')

# Initialize predictor
predictor = MultiModelPredictor()
try:
    predictor.load_models()
except:
    print("Models not loaded. Train models first.")

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

@predictions_bp.route('/predict', methods=['POST'])
@jwt_required()
def predict_student_performance():
    """Predict student performance using selected model"""
    try:
        claims = get_jwt()
        user_scope = get_user_scope(claims)
        data = request.get_json()
        
        student_id = data.get('student_id') or data.get('access_number') or data.get('reg_number')
        model_type = data.get('model_type', 'ensemble')  # 'random_forest', 'gradient_boosting', 'neural_network', 'ensemble'
        
        # Check permissions
        if user_scope['role'] == Role.STUDENT:
            # Students can only predict their own performance
            if user_scope['access_number'] and student_id != user_scope['access_number']:
                return jsonify({'error': 'Permission denied: Can only predict own performance'}), 403
            if user_scope['student_id'] and student_id != user_scope['student_id']:
                return jsonify({'error': 'Permission denied: Can only predict own performance'}), 403
        
        if not student_id:
            return jsonify({'error': 'Student ID, Access Number, or Reg Number required'}), 400
        
        # Resolve student_id if access_number or reg_number provided
        if student_id.startswith('A') or student_id.startswith('B'):
            engine = create_engine(DATA_WAREHOUSE_CONN_STRING)
            result = pd.read_sql_query(
                text("SELECT student_id FROM dim_student WHERE access_number = :access_number"),
                engine,
                params={'access_number': student_id}
            )
            if not result.empty:
                student_id = result['student_id'].iloc[0]
            engine.dispose()
        
        prediction = predictor.predict(student_id, model_type)
        
        return jsonify({
            'student_id': student_id,
            'model_type': model_type,
            'predicted_grade': round(float(prediction), 2),
            'predicted_letter_grade': get_letter_grade(prediction)
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@predictions_bp.route('/scenario', methods=['POST'])
@jwt_required()
def predict_scenario():
    """Predict performance for a hypothetical scenario (what-if analysis)"""
    try:
        claims = get_jwt()
        user_scope = get_user_scope(claims)
        data = request.get_json()
        
        # Check permissions - only analysts, sysadmin, and senate can do scenario analysis
        if user_scope['role'] not in [Role.ANALYST, Role.SYSADMIN, Role.SENATE]:
            return jsonify({'error': 'Permission denied: Scenario analysis not allowed'}), 403
        
        scenario = data.get('scenario', {})
        base_student_id = scenario.get('base_student_id')
        
        # Scenario parameters
        attendance_rate = scenario.get('attendance_rate')
        payment_completion_rate = scenario.get('payment_completion_rate')
        courses_enrolled = scenario.get('courses_enrolled')
        high_school = scenario.get('high_school')
        
        # Get base student data
        engine = create_engine(DATA_WAREHOUSE_CONN_STRING)
        query = text("""
        SELECT * FROM dim_student WHERE student_id = :student_id
        """)
        student_data = pd.read_sql_query(query, engine, params={'student_id': base_student_id})
        engine.dispose()
        
        if student_data.empty:
            return jsonify({'error': 'Base student not found'}), 404
        
        # Modify features based on scenario
        # This is a simplified version - in production, you'd modify the feature vector
        predictions = {}
        for model_type in ['random_forest', 'gradient_boosting', 'neural_network', 'ensemble']:
            try:
                pred = predictor.predict(base_student_id, model_type)
                predictions[model_type] = {
                    'predicted_grade': round(float(pred), 2),
                    'predicted_letter_grade': get_letter_grade(pred)
                }
            except:
                pass
        
        return jsonify({
            'scenario': scenario,
            'predictions': predictions,
            'analysis': analyze_scenario(scenario, predictions)
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@predictions_bp.route('/batch-predict', methods=['POST'])
@jwt_required()
def batch_predict():
    """Batch prediction for multiple students"""
    try:
        claims = get_jwt()
        user_scope = get_user_scope(claims)
        data = request.get_json()
        
        # Check permissions
        if user_scope['role'] == Role.STUDENT:
            return jsonify({'error': 'Permission denied'}), 403
        
        student_ids = data.get('student_ids', [])
        model_type = data.get('model_type', 'ensemble')
        filters = data.get('filters', {})
        
        # Apply role-based filtering
        engine = create_engine(DATA_WAREHOUSE_CONN_STRING)
        
        if user_scope['role'] == Role.STAFF:
            # Staff can only predict for their classes
            query = text("""
            SELECT DISTINCT fe.student_id 
            FROM fact_enrollment fe
            JOIN fact_attendance fa ON fe.student_id = fa.student_id
            WHERE fa.staff_id = :staff_id
            """)
            allowed_students = pd.read_sql_query(query, engine, params={'staff_id': user_scope['staff_id']})
            student_ids = [s for s in student_ids if s in allowed_students['student_id'].tolist()]
        
        elif user_scope['role'] == Role.HOD:
            # HOD can predict for their department
            query = text("""
            SELECT DISTINCT ds.student_id
            FROM dim_student ds
            JOIN dim_program dp ON ds.program_id = dp.program_id
            WHERE dp.department_id = :department_id
            """)
            allowed_students = pd.read_sql_query(query, engine, params={'department_id': user_scope['department_id']})
            student_ids = [s for s in student_ids if s in allowed_students['student_id'].tolist()]
        
        elif user_scope['role'] == Role.DEAN:
            # Dean can predict for their faculty
            query = text("""
            SELECT DISTINCT ds.student_id
            FROM dim_student ds
            JOIN dim_program dp ON ds.program_id = dp.program_id
            JOIN dim_department ddept ON dp.department_id = ddept.department_id
            WHERE ddept.faculty_id = :faculty_id
            """)
            allowed_students = pd.read_sql_query(query, engine, params={'faculty_id': user_scope['faculty_id']})
            student_ids = [s for s in student_ids if s in allowed_students['student_id'].tolist()]
        
        engine.dispose()
        
        results = []
        for student_id in student_ids:
            try:
                prediction = predictor.predict(student_id, model_type)
                results.append({
                    'student_id': student_id,
                    'predicted_grade': round(float(prediction), 2),
                    'predicted_letter_grade': get_letter_grade(prediction)
                })
            except Exception as e:
                results.append({
                    'student_id': student_id,
                    'error': str(e)
                })
        
        return jsonify({
            'model_type': model_type,
            'total_students': len(student_ids),
            'successful_predictions': len([r for r in results if 'error' not in r]),
            'results': results
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@predictions_bp.route('/scenarios', methods=['GET'])
@jwt_required()
def get_scenario_templates():
    """Get predefined scenario templates"""
    try:
        claims = get_jwt()
        user_scope = get_user_scope(claims)
        
        scenarios = [
            {
                'id': 'high_attendance',
                'name': 'High Attendance Scenario',
                'description': 'What if student maintains 90%+ attendance?',
                'parameters': {'attendance_rate': 90}
            },
            {
                'id': 'low_attendance',
                'name': 'Low Attendance Scenario',
                'description': 'What if student attendance drops to 50%?',
                'parameters': {'attendance_rate': 50}
            },
            {
                'id': 'full_tuition',
                'name': 'Full Tuition Payment',
                'description': 'What if all tuition is paid on time?',
                'parameters': {'payment_completion_rate': 100}
            },
            {
                'id': 'tuition_arrears',
                'name': 'Tuition Arrears Scenario',
                'description': 'What if student has significant tuition arrears?',
                'parameters': {'payment_completion_rate': 30, 'has_significant_balance': True}
            },
            {
                'id': 'increased_courses',
                'name': 'Increased Course Load',
                'description': 'What if student enrolls in more courses?',
                'parameters': {'courses_enrolled': '+3'}
            },
            {
                'id': 'reduced_courses',
                'name': 'Reduced Course Load',
                'description': 'What if student reduces course load?',
                'parameters': {'courses_enrolled': '-2'}
            },
            {
                'id': 'top_performer',
                'name': 'Top Performer Scenario',
                'description': 'Optimal conditions for best performance',
                'parameters': {
                    'attendance_rate': 95,
                    'payment_completion_rate': 100,
                    'courses_enrolled': 'optimal'
                }
            },
            {
                'id': 'at_risk',
                'name': 'At-Risk Student Scenario',
                'description': 'Multiple risk factors present',
                'parameters': {
                    'attendance_rate': 40,
                    'payment_completion_rate': 20,
                    'has_significant_balance': True
                }
            }
        ]
        
        return jsonify({'scenarios': scenarios}), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def get_letter_grade(score):
    """Convert numeric score to letter grade"""
    if score >= 80:
        return 'A'
    elif score >= 75:
        return 'B+'
    elif score >= 70:
        return 'B'
    elif score >= 60:
        return 'C'
    elif score >= 50:
        return 'D'
    else:
        return 'F'

def analyze_scenario(scenario, predictions):
    """Analyze scenario predictions and provide insights"""
    analysis = {
        'risk_level': 'medium',
        'recommendations': [],
        'key_factors': []
    }
    
    avg_prediction = sum([p['predicted_grade'] for p in predictions.values()]) / len(predictions)
    
    if avg_prediction < 50:
        analysis['risk_level'] = 'high'
        analysis['recommendations'].append('Student is at high risk of failure. Immediate intervention needed.')
    elif avg_prediction < 60:
        analysis['risk_level'] = 'medium-high'
        analysis['recommendations'].append('Student needs support to improve performance.')
    elif avg_prediction >= 70:
        analysis['risk_level'] = 'low'
        analysis['recommendations'].append('Student is performing well. Maintain current strategies.')
    
    if scenario.get('attendance_rate', 100) < 70:
        analysis['key_factors'].append('Low attendance is a major concern')
        analysis['recommendations'].append('Implement attendance monitoring and support')
    
    if scenario.get('payment_completion_rate', 100) < 50:
        analysis['key_factors'].append('Tuition arrears may impact performance')
        analysis['recommendations'].append('Financial aid or payment plan may be needed')
    
    return analysis

