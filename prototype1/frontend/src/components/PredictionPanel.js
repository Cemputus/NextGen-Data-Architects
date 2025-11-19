import React, { useState } from 'react';
import axios from 'axios';
import './PredictionPanel.css';

const PredictionPanel = () => {
  const [studentId, setStudentId] = useState('');
  const [prediction, setPrediction] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handlePredict = async () => {
    if (!studentId.trim()) {
      setError('Please enter a student ID');
      return;
    }

    setLoading(true);
    setError('');
    setPrediction(null);

    try {
      const response = await axios.post('/api/dashboard/predict-performance', {
        student_id: studentId
      });
      setPrediction(response.data);
    } catch (err) {
      setError(err.response?.data?.error || 'Failed to predict performance');
    } finally {
      setLoading(false);
    }
  };

  const getGradeColor = (grade) => {
    if (grade >= 80) return '#43e97b';
    if (grade >= 70) return '#4facfe';
    if (grade >= 60) return '#fee140';
    return '#fa709a';
  };

  return (
    <div className="prediction-panel">
      <div className="prediction-card">
        <h3>Student Performance Predictor</h3>
        <p className="prediction-description">
          Predict student performance based on attendance and tuition payment history
        </p>
        
        <div className="prediction-input">
          <input
            type="text"
            placeholder="Enter Student ID (e.g., STU000001)"
            value={studentId}
            onChange={(e) => setStudentId(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && handlePredict()}
          />
          <button onClick={handlePredict} disabled={loading}>
            {loading ? 'Predicting...' : 'Predict Performance'}
          </button>
        </div>

        {error && <div className="prediction-error">{error}</div>}

        {prediction && (
          <div className="prediction-result">
            <div className="result-header">
              <span>Student ID: {prediction.student_id}</span>
            </div>
            <div 
              className="result-grade"
              style={{ color: getGradeColor(prediction.predicted_grade) }}
            >
              <div className="grade-value">{prediction.predicted_grade}%</div>
              <div className="grade-label">Predicted Average Grade</div>
            </div>
            <div className="result-info">
              <p>This prediction is based on:</p>
              <ul>
                <li>Total attendance hours</li>
                <li>Number of days present</li>
                <li>Courses attended</li>
                <li>Total tuition payments</li>
                <li>Payment frequency</li>
                <li>Average payment amount</li>
              </ul>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default PredictionPanel;




