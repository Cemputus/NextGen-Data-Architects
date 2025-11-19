"""
Machine Learning Model for Predicting Student Performance
Uses MySQL
"""
import pandas as pd
import numpy as np
import pickle
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, r2_score
from sqlalchemy import create_engine, text
from config import DATA_WAREHOUSE_CONN_STRING

class StudentPerformancePredictor:
    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()
        self.model_path = Path("models/student_performance_model.pkl")
        self.model_path.parent.mkdir(exist_ok=True)
    
    def prepare_features(self):
        """Prepare features from data warehouse"""
        engine = create_engine(DATA_WAREHOUSE_CONN_STRING)
        
        # Get attendance data
        attendance_query = """
        SELECT 
            fa.student_id,
            SUM(fa.total_hours) as total_attendance_hours,
            SUM(fa.days_present) as total_days_present,
            COUNT(DISTINCT fa.course_code) as courses_attended
        FROM fact_attendance fa
        GROUP BY fa.student_id
        """
        attendance_df = pd.read_sql_query(attendance_query, engine)
        
        # Get payment data
        payment_query = """
        SELECT 
            fp.student_id,
            SUM(CASE WHEN fp.status = 'Completed' THEN fp.amount ELSE 0 END) as total_paid,
            COUNT(CASE WHEN fp.status = 'Completed' THEN 1 END) as payment_count,
            AVG(CASE WHEN fp.status = 'Completed' THEN fp.amount ELSE 0 END) as avg_payment
        FROM fact_payment fp
        GROUP BY fp.student_id
        """
        payment_df = pd.read_sql_query(payment_query, engine)
        
        # Get grade data (target variable)
        grade_query = """
        SELECT 
            fg.student_id,
            AVG(fg.grade) as avg_grade,
            COUNT(fg.grade_id) as num_grades
        FROM fact_grade fg
        GROUP BY fg.student_id
        """
        grade_df = pd.read_sql_query(grade_query, engine)
        
        # Merge all features
        features_df = attendance_df.merge(payment_df, on='student_id', how='outer')
        features_df = features_df.merge(grade_df, on='student_id', how='outer')
        features_df = features_df.fillna(0)
        
        engine.dispose()
        return features_df
    
    def train(self):
        """Train the predictive model"""
        print("Preparing features...")
        data = self.prepare_features()
        
        # Features
        feature_cols = ['total_attendance_hours', 'total_days_present', 'courses_attended',
                       'total_paid', 'payment_count', 'avg_payment']
        X = data[feature_cols]
        y = data['avg_grade']
        
        # Remove rows with missing target
        mask = ~y.isna()
        X = X[mask]
        y = y[mask]
        
        if len(X) == 0:
            print("No data available for training")
            return
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Train model
        print("Training model...")
        self.model = RandomForestRegressor(n_estimators=100, random_state=42, max_depth=10)
        self.model.fit(X_train_scaled, y_train)
        
        # Evaluate
        y_pred = self.model.predict(X_test_scaled)
        mse = mean_squared_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        
        print(f"Model trained successfully!")
        print(f"MSE: {mse:.2f}, R2 Score: {r2:.2f}")
        
        # Save model
        with open(self.model_path, 'wb') as f:
            pickle.dump({'model': self.model, 'scaler': self.scaler, 'features': feature_cols}, f)
        print(f"Model saved to {self.model_path}")
    
    def predict(self, student_id):
        """Predict performance for a specific student"""
        if self.model is None:
            self.load_model()
        
        engine = create_engine(DATA_WAREHOUSE_CONN_STRING)
        
        # Get student features
        attendance_query = text("""
        SELECT 
            COALESCE(SUM(fa.total_hours), 0) as total_attendance_hours,
            COALESCE(SUM(fa.days_present), 0) as total_days_present,
            COALESCE(COUNT(DISTINCT fa.course_code), 0) as courses_attended
        FROM fact_attendance fa
        WHERE fa.student_id = :student_id
        """)
        attendance = pd.read_sql_query(attendance_query, engine, params={'student_id': student_id})
        
        payment_query = text("""
        SELECT 
            COALESCE(SUM(CASE WHEN fp.status = 'Completed' THEN fp.amount ELSE 0 END), 0) as total_paid,
            COALESCE(COUNT(CASE WHEN fp.status = 'Completed' THEN 1 END), 0) as payment_count,
            COALESCE(AVG(CASE WHEN fp.status = 'Completed' THEN fp.amount ELSE 0 END), 0) as avg_payment
        FROM fact_payment fp
        WHERE fp.student_id = :student_id
        """)
        payment = pd.read_sql_query(payment_query, engine, params={'student_id': student_id})
        
        engine.dispose()
        
        # Combine features - handle empty dataframes
        if attendance.empty:
            attendance = pd.DataFrame([[0, 0, 0]], columns=['total_attendance_hours', 'total_days_present', 'courses_attended'])
        if payment.empty:
            payment = pd.DataFrame([[0, 0, 0]], columns=['total_paid', 'payment_count', 'avg_payment'])
        
        features = pd.concat([attendance, payment], axis=1)
        features = features.fillna(0)
        
        feature_cols = ['total_attendance_hours', 'total_days_present', 'courses_attended',
                       'total_paid', 'payment_count', 'avg_payment']
        
        # Ensure all columns exist
        for col in feature_cols:
            if col not in features.columns:
                features[col] = 0
        
        X = features[feature_cols].values
        
        # Check if scaler is fitted
        if not hasattr(self.scaler, 'mean_') or self.scaler.mean_ is None:
            raise ValueError("Model scaler not fitted. Please train the model first.")
        
        X_scaled = self.scaler.transform(X)
        prediction = self.model.predict(X_scaled)[0]
        return max(0, min(100, prediction))  # Clamp between 0 and 100
    
    def load_model(self):
        """Load saved model"""
        if self.model_path.exists():
            with open(self.model_path, 'rb') as f:
                model_data = pickle.load(f)
                self.model = model_data['model']
                self.scaler = model_data['scaler']
        else:
            print("Model not found. Training new model...")
            self.train()

if __name__ == "__main__":
    predictor = StudentPerformancePredictor()
    predictor.train()

